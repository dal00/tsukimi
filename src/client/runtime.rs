use std::sync::OnceLock;

use gtk::prelude::*;
use tokio::runtime::{
    self,
    Runtime,
};

pub fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        runtime::Builder::new_multi_thread()
            .worker_threads(default_worker_threads())
            .enable_all()
            .build()
            .expect("Failed to create runtime")
    })
}

fn default_worker_threads() -> usize {
    gtk::gio::Settings::new(crate::APP_ID).int("threads").max(1) as usize
}
