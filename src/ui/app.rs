use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use eframe::egui;

use super::broker_client::BrokerClient;
use super::fleet_stats::compute_fleet_stats;
use super::model::{VehicleSnapshot, apply_payload};

const DEFAULT_BROKER_ADDR: &str = "127.0.0.1:7000";
const DEFAULT_TOPIC: &str = "events";
const POLL_INTERVAL: Duration = Duration::from_millis(250);
const MAX_FETCH_PER_POLL: usize = 64;

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
        Box::new(|_cc| Ok(Box::new(UiShellApp::new()))),
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
}

enum ConnectionState {
    Connected { last_ok_at: Instant },
    Disconnected { reason: String },
}

/// Construction and broker polling.
impl UiShellApp {
    fn new() -> Self {
        Self {
            broker_client: BrokerClient::new(DEFAULT_BROKER_ADDR, DEFAULT_TOPIC),
            fleet: BTreeMap::new(),
            selected_id: None,
            next_offset: 0,
            parse_errors: 0,
            connection: ConnectionState::Disconnected {
                reason: "not connected yet".to_string(),
            },
            last_poll_at: Instant::now() - POLL_INTERVAL,
        }
    }

    fn reconcile_selection(&mut self) {
        if let Some(id) = &self.selected_id {
            if !self.fleet.contains_key(id) {
                self.selected_id = None;
            }
        }
    }

    fn poll_broker(&mut self) {
        if self.last_poll_at.elapsed() < POLL_INTERVAL {
            return;
        }
        self.last_poll_at = Instant::now();

        match self
            .broker_client
            .poll_from_offset(self.next_offset, MAX_FETCH_PER_POLL)
        {
            Ok(messages) => {
                self.connection = ConnectionState::Connected {
                    last_ok_at: Instant::now(),
                };
                for (offset, payload) in messages {
                    if apply_payload(&mut self.fleet, offset, &payload).is_err() {
                        self.parse_errors = self.parse_errors.saturating_add(1);
                    }
                    self.next_offset = offset.saturating_add(1);
                }
            }
            Err(reason) => {
                self.connection = ConnectionState::Disconnected { reason };
            }
        }
    }

    fn connection_text(&self) -> String {
        match &self.connection {
            ConnectionState::Connected { last_ok_at } => format!(
                "Connection: connected (offset={}, last_ok={}ms)",
                self.next_offset,
                last_ok_at.elapsed().as_millis()
            ),
            ConnectionState::Disconnected { reason } => {
                format!("Connection: disconnected ({reason})")
            }
        }
    }

    fn step_frame(&mut self, ctx: &egui::Context) {
        self.poll_broker();
        self.reconcile_selection();
        ctx.request_repaint_after(POLL_INTERVAL);
    }
}

/// Egui layout split out so `update` stays a short list of high-level steps.
impl UiShellApp {
    fn show_top_bar(&self, ctx: &egui::Context) {
        egui::TopBottomPanel::top("top_bar").show(ctx, |ui| {
            ui.horizontal_wrapped(|ui| {
                ui.strong("Herbatka Fleet Console");
                ui.separator();
                ui.label(self.connection_text());
                ui.separator();
                ui.label("Env: local");
                ui.separator();
                ui.label("Theme: dark");
                ui.separator();
                ui.label("Status: shell mode");
            });
        });
    }

    fn show_bottom_panels(&self, ctx: &egui::Context) {
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
                        ui.heading("Broker / Simulator Output");
                        ui.separator();
                        ui.label("No process output yet.");
                    });
                });
            });
    }

    fn show_right_sidebar(&mut self, ctx: &egui::Context) {
        egui::SidePanel::right("right_sidebar")
            .resizable(true)
            .default_width(330.0)
            .show(ctx, |ui| {
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
                self.render_broker_controls_placeholder(ui);
                ui.add_space(8.0);
                self.render_sim_controls_placeholder(ui);
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
                    .add_enabled(
                        self.selected_id.is_some(),
                        egui::Button::new("Clear"),
                    )
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
                        let is_selected = self
                            .selected_id
                            .as_ref()
                            .is_some_and(|s| s == vehicle_id);
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
            ui.label(format!("online: {}  /  stale: {}", stats.online, stats.stale));
            if let Some(avg) = stats.avg_speed_kmh {
                ui.label(format!("avg speed: {:.1} km/h", avg));
            } else {
                ui.label("avg speed: n/a");
            }
            ui.label(format!(
                "read next offset: {}",
                stats.read_next_offset
            ));
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

    fn render_broker_controls_placeholder(&self, ui: &mut egui::Ui) {
        ui.group(|ui| {
            ui.label("Broker Controls");
            ui.horizontal(|ui| {
                let _ = ui.button("Start");
                let _ = ui.button("Stop");
            });
        });
    }

    fn render_sim_controls_placeholder(&self, ui: &mut egui::Ui) {
        ui.group(|ui| {
            ui.label("Simulation Controls");
            ui.horizontal(|ui| {
                let _ = ui.button("Start");
                let _ = ui.button("Pause");
                let _ = ui.button("Stop");
            });
            ui.small("Scenario: (placeholder)");
        });
    }

    fn show_map_pane(&self, ctx: &egui::Context) {
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("Map Pane");
            ui.separator();
            ui.label("Vehicle map placeholder (lat/lon markers will be rendered here).");
            ui.add_space(12.0);
            ui.horizontal(|ui| {
                let _ = ui.button("Zoom +");
                let _ = ui.button("Zoom -");
                let _ = ui.button("Fit Fleet");
                let _ = ui.button("Follow Vehicle");
            });
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
