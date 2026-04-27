use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpStream};
use std::process::Child;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use eframe::egui;

use super::broker_client::BrokerClient;
use super::broker_subprocess;
use super::fleet_stats::compute_fleet_stats;
use super::simulator_subprocess;
use super::model::{VehicleSnapshot, apply_payload};
use super::process_log::{
    DRAIN_PER_FRAME_CAP, LogLine, LogRing, LogSource, LogStream, drain_into_ring,
};

const DEFAULT_BROKER_ADDR: &str = "127.0.0.1:7000";
const DEFAULT_TOPIC: &str = "events";
const POLL_INTERVAL: Duration = Duration::from_millis(250);
const MAX_FETCH_PER_POLL: usize = 64;
const RECONNECT_INITIAL_BACKOFF: Duration = Duration::from_millis(250);
const RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(2);
const BROKER_REACHABILITY_TIMEOUT: Duration = Duration::from_millis(35);
const MAP_MIN_SPAN: f64 = 0.0002;
const MAP_PADDING_RATIO: f64 = 0.08;
const MAP_MARKER_RADIUS: f32 = 4.0;
const MAP_SELECTED_MARKER_RADIUS: f32 = 7.0;
const MAP_HIT_RADIUS: f32 = 10.0;

#[derive(Debug, Clone, Copy)]
struct MapViewport {
    min_lat: f64,
    max_lat: f64,
    min_lon: f64,
    max_lon: f64,
}

impl MapViewport {
    fn lat_span(self) -> f64 {
        (self.max_lat - self.min_lat).max(MAP_MIN_SPAN)
    }

    fn lon_span(self) -> f64 {
        (self.max_lon - self.min_lon).max(MAP_MIN_SPAN)
    }
}

pub fn run() -> eframe::Result<()> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1280.0, 800.0])
            .with_min_inner_size([960.0, 640.0]),
        ..Default::default()
    };

    eframe::run_native(
        "Herbatka Fleet Console (UI Shell)",
        options,
        Box::new(|cc| {
            cc.egui_ctx.set_visuals(egui::Visuals::dark());
            Ok(Box::new(UiShellApp::new()))
        }),
    )
}

struct UiShellApp {
    broker_client: BrokerClient,
    fleet: BTreeMap<String, VehicleSnapshot>,
    /// Selected `vehicle_id` (map key), if any and still present in `fleet`.
    selected_id: Option<String>,
    next_offset: u64,
    parse_errors: u64,
    connection: ConnectionState,
    last_poll_at: Instant,
    next_reconnect_at: Instant,
    reconnect_backoff: Duration,
    pause_reconnect: bool,
    map_viewport: Option<MapViewport>,
    follow_selected: bool,

    log_tx: mpsc::Sender<LogLine>,
    log_rx: mpsc::Receiver<LogLine>,
    log_ring: LogRing,
    /// Broker child when started from **Broker Controls** (stdout/stderr pumped to `log_tx`).
    broker_child: Option<Child>,
    /// Simulator child when started from **Simulation Controls** (stdout/stderr pumped to `log_tx`).
    sim_child: Option<Child>,
}

enum ConnectionState {
    NeverConnected,
    Connected { last_ok_at: Instant },
    Disconnected { reason: String },
}

/// Construction and broker polling.
impl UiShellApp {
    fn new() -> Self {
        let (log_tx, log_rx) = mpsc::channel();
        Self {
            broker_client: BrokerClient::new(DEFAULT_BROKER_ADDR, DEFAULT_TOPIC),
            fleet: BTreeMap::new(),
            selected_id: None,
            next_offset: 0,
            parse_errors: 0,
            connection: ConnectionState::NeverConnected,
            last_poll_at: Instant::now() - POLL_INTERVAL,
            next_reconnect_at: Instant::now(),
            reconnect_backoff: RECONNECT_INITIAL_BACKOFF,
            pause_reconnect: false,
            map_viewport: None,
            follow_selected: false,
            log_tx,
            log_rx,
            log_ring: LogRing::with_default_cap(),
            broker_child: None,
            sim_child: None,
        }
    }

    fn reconcile_selection(&mut self) {
        if let Some(id) = &self.selected_id {
            if !self.fleet.contains_key(id) {
                self.selected_id = None;
                self.follow_selected = false;
            }
        }
    }

    fn poll_broker(&mut self) {
        if self.last_poll_at.elapsed() < POLL_INTERVAL {
            return;
        }
        self.last_poll_at = Instant::now();
        if self.pause_reconnect
            && matches!(
                self.connection,
                ConnectionState::Disconnected { .. } | ConnectionState::NeverConnected
            )
        {
            return;
        }
        if matches!(
            self.connection,
            ConnectionState::Disconnected { .. } | ConnectionState::NeverConnected
        )
            && Instant::now() < self.next_reconnect_at
        {
            return;
        }

        match self
            .broker_client
            .poll_from_offset(self.next_offset, MAX_FETCH_PER_POLL)
        {
            Ok(messages) => {
                self.connection = ConnectionState::Connected {
                    last_ok_at: Instant::now(),
                };
                self.reconnect_backoff = RECONNECT_INITIAL_BACKOFF;
                self.next_reconnect_at = Instant::now();
                for (offset, payload) in messages {
                    if apply_payload(&mut self.fleet, offset, &payload).is_err() {
                        self.parse_errors = self.parse_errors.saturating_add(1);
                    }
                    self.next_offset = offset.saturating_add(1);
                }
            }
            Err(reason) => {
                self.connection = ConnectionState::Disconnected {
                    reason: self.user_facing_reason(&reason),
                };
                self.next_reconnect_at = Instant::now() + self.reconnect_backoff;
                self.reconnect_backoff =
                    (self.reconnect_backoff.saturating_mul(2)).min(RECONNECT_MAX_BACKOFF);
            }
        }
    }

    fn connection_text(&self) -> String {
        match &self.connection {
            ConnectionState::NeverConnected => {
                if self.pause_reconnect {
                    "Connection: idle (auto reconnect paused)".to_string()
                } else {
                    format!(
                        "Connection: waiting for broker (next retry in {}ms)",
                        self.ms_until_next_reconnect()
                    )
                }
            }
            ConnectionState::Connected { last_ok_at } => format!(
                "Connection: connected (offset={}, last_ok={}ms)",
                self.next_offset,
                last_ok_at.elapsed().as_millis()
            ),
            ConnectionState::Disconnected { reason } => {
                if self.pause_reconnect {
                    format!("Connection: disconnected ({reason}; auto reconnect paused)")
                } else {
                    format!(
                        "Connection: disconnected ({reason}; retry in {}ms)",
                        self.ms_until_next_reconnect()
                    )
                }
            }
        }
    }

    fn ms_until_next_reconnect(&self) -> u128 {
        self.next_reconnect_at
            .checked_duration_since(Instant::now())
            .map(|d| d.as_millis())
            .unwrap_or(0)
    }

    fn force_reconnect_now(&mut self) {
        self.reconnect_backoff = RECONNECT_INITIAL_BACKOFF;
        self.next_reconnect_at = Instant::now();
        if !matches!(self.connection, ConnectionState::Connected { .. }) {
            self.connection = ConnectionState::Disconnected {
                reason: "manual reconnect requested".to_string(),
            };
        }
    }

    fn user_facing_reason(&self, raw_reason: &str) -> String {
        if raw_reason.contains("broker unavailable") {
            "broker is not running".to_string()
        } else if raw_reason.contains("timeout") {
            "broker did not respond in time".to_string()
        } else if raw_reason.contains("connection lost")
            || raw_reason.contains("closed connection")
            || raw_reason.contains("connection reset")
        {
            "connection lost".to_string()
        } else if raw_reason.contains("unknown topic") {
            format!("topic `{DEFAULT_TOPIC}` is not available yet")
        } else {
            raw_reason.to_string()
        }
    }

    fn fleet_bounds(&self) -> Option<MapViewport> {
        let mut iter = self.fleet.values();
        let first = iter.next()?;
        let (mut min_lat, mut max_lat) = (first.lat, first.lat);
        let (mut min_lon, mut max_lon) = (first.lon, first.lon);
        for v in iter {
            min_lat = min_lat.min(v.lat);
            max_lat = max_lat.max(v.lat);
            min_lon = min_lon.min(v.lon);
            max_lon = max_lon.max(v.lon);
        }
        let lat_center = (min_lat + max_lat) * 0.5;
        let lon_center = (min_lon + max_lon) * 0.5;
        let lat_span = (max_lat - min_lat).max(MAP_MIN_SPAN) * (1.0 + MAP_PADDING_RATIO * 2.0);
        let lon_span = (max_lon - min_lon).max(MAP_MIN_SPAN) * (1.0 + MAP_PADDING_RATIO * 2.0);
        Some(MapViewport {
            min_lat: lat_center - lat_span * 0.5,
            max_lat: lat_center + lat_span * 0.5,
            min_lon: lon_center - lon_span * 0.5,
            max_lon: lon_center + lon_span * 0.5,
        })
    }

    fn ensure_map_viewport(&mut self) {
        if self.map_viewport.is_none() {
            self.map_viewport = self.fleet_bounds();
        }
    }

    fn follow_selected_if_enabled(&mut self) {
        if !self.follow_selected {
            return;
        }
        let Some(id) = &self.selected_id else {
            return;
        };
        let Some(snap) = self.fleet.get(id) else {
            return;
        };
        let span = self.map_viewport.unwrap_or(MapViewport {
            min_lat: snap.lat - MAP_MIN_SPAN,
            max_lat: snap.lat + MAP_MIN_SPAN,
            min_lon: snap.lon - MAP_MIN_SPAN,
            max_lon: snap.lon + MAP_MIN_SPAN,
        });
        let lat_half = span.lat_span() * 0.5;
        let lon_half = span.lon_span() * 0.5;
        self.map_viewport = Some(MapViewport {
            min_lat: snap.lat - lat_half,
            max_lat: snap.lat + lat_half,
            min_lon: snap.lon - lon_half,
            max_lon: snap.lon + lon_half,
        });
    }

    fn project_to_screen(&self, view: MapViewport, rect: egui::Rect, lat: f64, lon: f64) -> egui::Pos2 {
        let x_t = ((lon - view.min_lon) / view.lon_span()).clamp(0.0, 1.0) as f32;
        let y_t = ((lat - view.min_lat) / view.lat_span()).clamp(0.0, 1.0) as f32;
        let x = egui::lerp(rect.left()..=rect.right(), x_t);
        let y = egui::lerp(rect.bottom()..=rect.top(), y_t);
        egui::pos2(x, y)
    }

    fn zoom_viewport(&mut self, factor: f64) {
        let Some(view) = self.map_viewport else {
            return;
        };
        let center_lat = (view.min_lat + view.max_lat) * 0.5;
        let center_lon = (view.min_lon + view.max_lon) * 0.5;
        let lat_half = (view.lat_span() * factor * 0.5).max(MAP_MIN_SPAN * 0.5);
        let lon_half = (view.lon_span() * factor * 0.5).max(MAP_MIN_SPAN * 0.5);
        self.map_viewport = Some(MapViewport {
            min_lat: center_lat - lat_half,
            max_lat: center_lat + lat_half,
            min_lon: center_lon - lon_half,
            max_lon: center_lon + lon_half,
        });
    }

    fn step_frame(&mut self, ctx: &egui::Context) {
        self.poll_broker();
        self.reconcile_selection();
        self.ensure_map_viewport();
        self.follow_selected_if_enabled();
        if drain_into_ring(&self.log_rx, &mut self.log_ring, DRAIN_PER_FRAME_CAP) {
            ctx.request_repaint();
        }
        ctx.request_repaint_after(POLL_INTERVAL);
    }
}

/// Egui layout split out so `update` stays a short list of high-level steps.
impl UiShellApp {
    fn is_broker_addr_reachable(&self) -> bool {
        let Ok(addr) = DEFAULT_BROKER_ADDR.parse::<SocketAddr>() else {
            return false;
        };
        TcpStream::connect_timeout(&addr, BROKER_REACHABILITY_TIMEOUT).is_ok()
    }

    fn stop_child(child: &mut Option<Child>) {
        if let Some(mut c) = child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    fn shutdown_children(&mut self) {
        Self::stop_child(&mut self.sim_child);
        Self::stop_child(&mut self.broker_child);
    }

    fn show_top_bar(&self, ctx: &egui::Context) {
        egui::TopBottomPanel::top("top_bar").show(ctx, |ui| {
            ui.horizontal_wrapped(|ui| {
                ui.strong("Herbatka Fleet Console");
                ui.separator();
                ui.label(self.connection_text());
                ui.separator();
                ui.label("Env: local");
                ui.separator();
                ui.label("Theme: dark(dummy)");
                ui.separator();
                ui.label("Status: shell mode");
            });
        });
    }

    fn show_bottom_panels(&mut self, ctx: &egui::Context) {
        egui::TopBottomPanel::bottom("bottom_panels")
            .resizable(true)
            .default_height(200.0)
            .show(ctx, |ui| {
                ui.columns(2, |columns| {
                    columns[0].group(|ui| {
                        ui.heading("Event / Command Log");
                        ui.separator();
                        ui.label("No events yet.");
                    });
                    columns[1].group(|ui| {
                        self.render_process_output_panel(ui);
                    });
                });
            });
    }

    fn show_right_sidebar(&mut self, ctx: &egui::Context) {
        egui::SidePanel::right("right_sidebar")
            .resizable(true)
            .default_width(330.0)
            .show(ctx, |ui| {
                egui::ScrollArea::vertical().show(ui, |ui| {
                    ui.heading("Telemetry and Controls");
                    ui.separator();
                    self.render_fleet_summary(ui);
                    ui.add_space(8.0);
                    self.render_selected_vehicle_panel(ui);
                    ui.add_space(8.0);
                    self.render_fleet_list(ui);
                    ui.add_space(8.0);
                    self.render_fleet_stats(ui);
                    ui.add_space(8.0);
                    self.render_broker_controls(ui);
                    ui.add_space(8.0);
                    self.render_sim_controls(ui);
                });
            });
    }

    fn render_fleet_summary(&self, ui: &mut egui::Ui) {
        ui.group(|ui| {
            ui.label(format!("Fleet (read-only): {} vehicles", self.fleet.len()));
            ui.small(format!(
                "topic={}, next_offset={}, parse_errors={}",
                DEFAULT_TOPIC, self.next_offset, self.parse_errors
            ));
        });
    }

    fn render_selected_vehicle_panel(&mut self, ui: &mut egui::Ui) {
        ui.group(|ui| {
            ui.horizontal(|ui| {
                ui.heading("Selected vehicle");
                if ui
                    .add_enabled(self.selected_id.is_some(), egui::Button::new("Clear"))
                    .clicked()
                {
                    self.selected_id = None;
                }
            });
            ui.separator();
            match &self.selected_id {
                None => {
                    ui.label("No vehicle selected.");
                    ui.small("Click a row in the list below.");
                }
                Some(id) => {
                    if let Some(snap) = self.fleet.get(id) {
                        ui.label(format!("vehicle_id: {id}"));
                        ui.label(format!("lat: {:.6}", snap.lat));
                        ui.label(format!("lon: {:.6}", snap.lon));
                        ui.label(format!("speed: {} km/h", snap.speed));
                        ui.label(format!("ts_ms: {}", snap.ts_ms));
                        ui.label(format!("last_offset: {}", snap.last_offset));
                    }
                }
            }
        });
    }

    fn render_fleet_list(&mut self, ui: &mut egui::Ui) {
        ui.group(|ui| {
            ui.label("Fleet list");
            ui.separator();
            egui::ScrollArea::vertical()
                .max_height(220.0)
                .show(ui, |ui| {
                    for (vehicle_id, snapshot) in &self.fleet {
                        let is_selected =
                            self.selected_id.as_ref().is_some_and(|s| s == vehicle_id);
                        let summary = format!(
                            "{}  lat={:.5} lon={:.5}  speed={}  off={}",
                            vehicle_id,
                            snapshot.lat,
                            snapshot.lon,
                            snapshot.speed,
                            snapshot.last_offset
                        );
                        if ui.selectable_label(is_selected, summary).clicked() {
                            self.selected_id = Some(vehicle_id.clone());
                        }
                    }
                    if self.fleet.is_empty() {
                        ui.small("No vehicles yet.");
                    }
                });
        });
    }

    fn render_fleet_stats(&self, ui: &mut egui::Ui) {
        let stats = compute_fleet_stats(&self.fleet, self.next_offset);
        ui.group(|ui| {
            ui.heading("Fleet Stats");
            ui.label(format!(
                "online: {}  /  stale: {}",
                stats.online, stats.stale
            ));
            if let Some(avg) = stats.avg_speed_kmh {
                ui.label(format!("avg speed: {:.1} km/h", avg));
            } else {
                ui.label("avg speed: n/a");
            }
            ui.label(format!("read next offset: {}", stats.read_next_offset));
            if let Some(o) = stats.newest_buffered_offset {
                ui.label(format!("newest buffered offset: {o}"));
            } else {
                ui.label("newest buffered offset: n/a");
            }
            if let Some(lag) = stats.lag_events {
                ui.label(format!("UI lag (events): {lag}"));
            } else {
                ui.label("UI lag (events): n/a");
            }
            ui.small("Lag: UI read position vs. latest buffered offset (not full broker depth).");
        });
    }

    fn render_process_output_panel(&mut self, ui: &mut egui::Ui) {
        ui.heading("Broker / Simulator Output");
        ui.label(format!("{} line(s) buffered", self.log_ring.len()));
        ui.separator();
        ui.horizontal(|ui| {
            if ui.button("Clear log").clicked() {
                self.log_ring.clear();
            }
        });
        ui.separator();
        let text = self.log_ring.as_single_text();
        if text.is_empty() {
            ui.label("No process output yet. Use Broker / Simulation Controls: Start, or an external process.");
        } else {
            egui::ScrollArea::vertical()
                .id_salt("process_output_log")
                .max_height(160.0)
                .show(ui, |ui| {
                    ui.add(egui::Label::new(egui::RichText::new(&text).monospace()));
                });
        }
    }

    fn render_broker_controls(&mut self, ui: &mut egui::Ui) {
        let running = self.broker_child.is_some();
        ui.group(|ui| {
            ui.label("Broker Controls");
            ui.horizontal(|ui| {
                if ui
                    .add_enabled(!running, egui::Button::new("Start"))
                    .clicked()
                {
                    if self.is_broker_addr_reachable() {
                        let _ = self.log_tx.send(LogLine {
                            source: LogSource::Broker,
                            stream: LogStream::Stderr,
                            text: format!(
                                "UI: broker start skipped: {DEFAULT_BROKER_ADDR} is already in use. Another broker (or app) is likely running.\n"
                            ),
                        });
                        return;
                    }
                    match broker_subprocess::spawn_broker(&self.log_tx) {
                        Ok(child) => {
                            self.broker_child = Some(child);
                            self.force_reconnect_now();
                            self.connection = ConnectionState::Disconnected {
                                reason: "broker started from UI; connecting...".to_string(),
                            };
                            let _ = self.log_tx.send(LogLine {
                                source: LogSource::Broker,
                                stream: LogStream::Stdout,
                                text: "UI: broker process started\n".to_string(),
                            });
                        }
                        Err(e) => {
                            let _ = self.log_tx.send(LogLine {
                                source: LogSource::Broker,
                                stream: LogStream::Stderr,
                                text: format!("UI: {e}\n"),
                            });
                        }
                    }
                }
                if ui.add_enabled(running, egui::Button::new("Stop")).clicked() {
                    Self::stop_child(&mut self.broker_child);
                    self.connection = ConnectionState::Disconnected {
                        reason: "broker stopped from UI".to_string(),
                    };
                    self.reconnect_backoff = RECONNECT_INITIAL_BACKOFF;
                    self.next_reconnect_at = Instant::now() + self.reconnect_backoff;
                    let _ = self.log_tx.send(LogLine {
                        source: LogSource::Broker,
                        stream: LogStream::Stdout,
                        text: "UI: broker process stopped\n".to_string(),
                    });
                }
                if ui.button("Reconnect now").clicked() {
                    self.force_reconnect_now();
                }
                let pause_label = if self.pause_reconnect {
                    "Resume auto-reconnect"
                } else {
                    "Pause auto-reconnect"
                };
                if ui.button(pause_label).clicked() {
                    self.pause_reconnect = !self.pause_reconnect;
                    if !self.pause_reconnect {
                        self.force_reconnect_now();
                    }
                }
            });
            if self.pause_reconnect {
                ui.small("Auto reconnect is paused.");
            } else {
                ui.small(format!(
                    "Auto reconnect: next attempt in {}ms",
                    self.ms_until_next_reconnect()
                ));
            }
        });
    }

    fn render_sim_controls(&mut self, ui: &mut egui::Ui) {
        let running = self.sim_child.is_some();
        ui.group(|ui| {
            ui.label("Simulation Controls");
            ui.horizontal(|ui| {
                if ui
                    .add_enabled(!running, egui::Button::new("Start"))
                    .clicked()
                {
                    match simulator_subprocess::spawn_simulator(
                        &self.log_tx,
                        DEFAULT_BROKER_ADDR,
                        DEFAULT_TOPIC,
                    ) {
                        Ok(child) => {
                            self.sim_child = Some(child);
                            let _ = self.log_tx.send(LogLine {
                                source: LogSource::Simulator,
                                stream: LogStream::Stdout,
                                text: "UI: simulator process started\n".to_string(),
                            });
                        }
                        Err(e) => {
                            let _ = self.log_tx.send(LogLine {
                                source: LogSource::Simulator,
                                stream: LogStream::Stderr,
                                text: format!("UI: {e}\n"),
                            });
                        }
                    }
                }
                let _ = ui
                    .add_enabled(false, egui::Button::new("Pause"))
                    .on_hover_text("Not supported: simulator has no pause/resume API yet.");
                if ui
                    .add_enabled(running, egui::Button::new("Stop"))
                    .clicked()
                {
                    Self::stop_child(&mut self.sim_child);
                    let _ = self.log_tx.send(LogLine {
                        source: LogSource::Simulator,
                        stream: LogStream::Stdout,
                        text: "UI: simulator process stopped\n".to_string(),
                    });
                }
            });
            ui.small("Pause: needs simulator or protocol support. Stop ends the run (sends SIGKILL to child).");
        });
    }

    fn show_map_pane(&mut self, ctx: &egui::Context) {
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("Map Pane");
            ui.separator();
            ui.horizontal(|ui| {
                if ui.button("Zoom +").clicked() {
                    self.zoom_viewport(0.8);
                    self.follow_selected = false;
                }
                if ui.button("Zoom -").clicked() {
                    self.zoom_viewport(1.25);
                    self.follow_selected = false;
                }
                if ui.button("Fit Fleet").clicked() {
                    self.map_viewport = self.fleet_bounds();
                    self.follow_selected = false;
                }
                let follow_label = if self.follow_selected {
                    "Unfollow Vehicle"
                } else {
                    "Follow Vehicle"
                };
                if ui.button(follow_label).clicked() {
                    if self.selected_id.is_some() {
                        self.follow_selected = !self.follow_selected;
                    }
                }
            });
            if self.selected_id.is_none() {
                ui.small("Follow Vehicle requires a selected vehicle.");
            }
            ui.add_space(8.0);

            let desired = egui::vec2(ui.available_width(), ui.available_height().max(240.0));
            let (rect, response) = ui.allocate_exact_size(desired, egui::Sense::click());
            let painter = ui.painter_at(rect);
            painter.rect_filled(rect, 4.0, egui::Color32::from_rgb(18, 22, 30));
            painter.rect_stroke(
                rect,
                4.0,
                egui::Stroke::new(1.0, egui::Color32::from_rgb(70, 78, 94)),
                egui::StrokeKind::Inside,
            );

            if self.fleet.is_empty() {
                painter.text(
                    rect.center(),
                    egui::Align2::CENTER_CENTER,
                    "No telemetry yet",
                    egui::TextStyle::Body.resolve(ui.style()),
                    ui.visuals().text_color(),
                );
                return;
            }

            let Some(view) = self.map_viewport else {
                return;
            };

            let mut nearest: Option<(String, f32)> = None;
            let pointer = response.interact_pointer_pos();
            for (vehicle_id, snap) in &self.fleet {
                let pos = self.project_to_screen(view, rect, snap.lat, snap.lon);
                let is_selected = self
                    .selected_id
                    .as_ref()
                    .is_some_and(|selected| selected == vehicle_id);
                let radius = if is_selected {
                    MAP_SELECTED_MARKER_RADIUS
                } else {
                    MAP_MARKER_RADIUS
                };
                let fill = if is_selected {
                    egui::Color32::from_rgb(80, 220, 120)
                } else {
                    egui::Color32::from_rgb(120, 170, 255)
                };
                painter.circle_filled(pos, radius, fill);
                if is_selected {
                    painter.circle_stroke(pos, radius + 2.0, egui::Stroke::new(1.5, egui::Color32::WHITE));
                    painter.text(
                        pos + egui::vec2(8.0, -8.0),
                        egui::Align2::LEFT_BOTTOM,
                        vehicle_id,
                        egui::TextStyle::Body.resolve(ui.style()),
                        egui::Color32::from_rgb(235, 242, 255),
                    );
                }
                if let Some(pointer_pos) = pointer {
                    let d = pointer_pos.distance(pos);
                    if d <= MAP_HIT_RADIUS {
                        let replace = nearest.as_ref().map(|(_, best)| d < *best).unwrap_or(true);
                        if replace {
                            nearest = Some((vehicle_id.clone(), d));
                        }
                    }
                }
            }

            if response.clicked() && let Some((nearest_id, _)) = nearest {
                self.selected_id = Some(nearest_id);
            }
        });
    }
}

impl eframe::App for UiShellApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.step_frame(ctx);
        self.show_top_bar(ctx);
        self.show_bottom_panels(ctx);
        self.show_right_sidebar(ctx);
        self.show_map_pane(ctx);
    }
}

impl Drop for UiShellApp {
    fn drop(&mut self) {
        self.shutdown_children();
    }
}
