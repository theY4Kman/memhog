mod lib;

extern crate bytesize;

use crate::lib::MemHogApp;

#[derive(Debug)]
pub enum MemHogError {
    EframeError(eframe::Error),
    PSUtilError(psutil::Error),
    ThreadError,
}

fn main() -> Result<(), MemHogError> {
    let options = eframe::NativeOptions {
        initial_window_size: Some(egui::Vec2::new(1024.0, 768.0)),
        ..Default::default()
    };

    eframe::run_native(
        "MemHog",
        options,
        Box::new(|_cc| Box::new(MemHogApp::new(_cc))),
    )
    .map_err(|e| MemHogError::EframeError(e))?;

    Ok(())
}
