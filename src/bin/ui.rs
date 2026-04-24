use eframe::egui;

fn main() -> eframe::Result<()> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1280.0, 800.0])
            .with_min_inner_size([960.0, 640.0]),
        ..Default::default()
    };

    eframe::run_native(
        "Herbatka Fleet Console (UI Shell)",
        options,
        Box::new(|_cc| Ok(Box::new(UiShellApp::default()))),
    )
}

#[derive(Default)]
struct UiShellApp;

impl eframe::App for UiShellApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        egui::TopBottomPanel::top("top_bar").show(ctx, |ui| {
            ui.horizontal_wrapped(|ui| {
                ui.strong("Herbatka Fleet Console");
                ui.separator();
                ui.label("Connection: disconnected");
                ui.separator();
                ui.label("Env: local");
                ui.separator();
                ui.label("Theme: dark");
                ui.separator();
                ui.label("Status: shell mode");
            });
        });

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

        egui::SidePanel::right("right_sidebar")
            .resizable(true)
            .default_width(330.0)
            .show(ctx, |ui| {
                ui.heading("Telemetry and Controls");
                ui.separator();

                ui.group(|ui| {
                    ui.label("Selected Vehicle");
                    ui.small("id, lat, lon, speed, heading, updated_at");
                });

                ui.add_space(8.0);
                ui.group(|ui| {
                    ui.label("Fleet Stats");
                    ui.small("online, stale, avg speed, topic lag");
                });

                ui.add_space(8.0);
                ui.group(|ui| {
                    ui.label("Broker Controls");
                    ui.horizontal(|ui| {
                        let _ = ui.button("Start");
                        let _ = ui.button("Stop");
                    });
                });

                ui.add_space(8.0);
                ui.group(|ui| {
                    ui.label("Simulation Controls");
                    ui.horizontal(|ui| {
                        let _ = ui.button("Start");
                        let _ = ui.button("Pause");
                        let _ = ui.button("Stop");
                    });
                    ui.small("Scenario: (placeholder)");
                });
            });

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
