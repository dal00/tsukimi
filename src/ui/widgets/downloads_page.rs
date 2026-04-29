use adw::prelude::*;
use gettextrs::gettext;
use gtk::glib;

use crate::{
    client::downloads::{
        DOWNLOAD_MANAGER,
        DownloadEntry,
        DownloadStatus,
    },
    ui::{
        SETTINGS,
        widgets::item::SelectedVideoSubInfo,
    },
    utils::{
        spawn,
        spawn_tokio,
    },
};

pub fn new() -> gtk::Widget {
    let list = gtk::ListBox::builder()
        .selection_mode(gtk::SelectionMode::None)
        .css_classes(["boxed-list"])
        .build();

    let clamp = adw::Clamp::builder()
        .margin_top(12)
        .margin_bottom(12)
        .margin_start(12)
        .margin_end(12)
        .child(&list)
        .build();

    let scrolled = gtk::ScrolledWindow::builder()
        .hscrollbar_policy(gtk::PolicyType::Never)
        .child(&clamp)
        .build();

    refresh(&list);
    glib::timeout_add_seconds_local(
        1,
        glib::clone!(
            #[weak]
            list,
            #[upgrade_or]
            glib::ControlFlow::Break,
            move || {
                refresh(&list);
                glib::ControlFlow::Continue
            }
        ),
    );

    scrolled.upcast()
}

fn refresh(list: &gtk::ListBox) {
    DOWNLOAD_MANAGER.set_base_dir(SETTINGS.download_dir());
    spawn(glib::clone!(
        #[weak]
        list,
        async move {
            let entries = spawn_tokio(async { DOWNLOAD_MANAGER.entries().await }).await;
            while let Some(child) = list.first_child() {
                list.remove(&child);
            }

            if entries.is_empty() {
                let row = adw::ActionRow::builder()
                    .title(gettext("No Downloads"))
                    .subtitle(gettext("Downloaded videos will appear here"))
                    .build();
                list.append(&row);
                return;
            }

            for entry in entries {
                list.append(&download_row(entry));
            }
        }
    ));
}

fn download_row(entry: DownloadEntry) -> gtk::Widget {
    let row = adw::ActionRow::builder()
        .title(entry.display_title())
        .subtitle(row_subtitle(&entry))
        .build();

    let progress = gtk::ProgressBar::builder()
        .width_request(120)
        .valign(gtk::Align::Center)
        .fraction(entry.progress_fraction())
        .build();
    progress.set_visible(matches!(entry.status, DownloadStatus::Downloading | DownloadStatus::Queued));
    row.add_suffix(&progress);

    match entry.status {
        DownloadStatus::Completed => {
            let play = gtk::Button::builder()
                .icon_name("media-playback-start-symbolic")
                .tooltip_text(gettext("Play"))
                .valign(gtk::Align::Center)
                .css_classes(["flat"])
                .build();
            play.connect_clicked(glib::clone!(
                #[strong]
                entry,
                move |button| {
                    let Some(window) = button.root().and_downcast::<crate::Window>() else {
                        return;
                    };
                    let item = crate::ui::provider::tu_item::TuItem::default();
                    item.set_id(entry.item_id.to_owned());
                    item.set_name(entry.name.to_owned());
                    item.set_series_name(entry.series_name.to_owned());
                    item.set_index_number(entry.index_number.unwrap_or_default());
                    item.set_parent_index_number(entry.parent_index_number.unwrap_or_default());
                    item.set_run_time_ticks(entry.run_time_ticks.unwrap_or_default());
                    item.set_item_type("Video".to_string());
                    let selected = SelectedVideoSubInfo {
                        sub_lang: String::new(),
                        sub_index: 0,
                        video_index: 0,
                        media_source_id: entry.media_source_id.to_owned(),
                    };
                    window.play_media(Some(selected), item, vec![], None, 0.0);
                }
            ));
            row.add_suffix(&play);
        }
        DownloadStatus::Failed | DownloadStatus::Cancelled => {
            let retry = gtk::Button::builder()
                .icon_name("view-refresh-symbolic")
                .tooltip_text(gettext("Retry"))
                .valign(gtk::Align::Center)
                .css_classes(["flat"])
                .build();
            retry.connect_clicked(glib::clone!(
                #[strong]
                entry,
                move |_| {
                    let item_id = entry.item_id.to_owned();
                    DOWNLOAD_MANAGER.set_base_dir(SETTINGS.download_dir());
                    spawn(async move {
                        let _ = spawn_tokio(async move {
                            DOWNLOAD_MANAGER.start_video_download(&item_id).await
                        })
                        .await;
                    });
                }
            ));
            row.add_suffix(&retry);
        }
        DownloadStatus::Queued | DownloadStatus::Downloading => {
            let cancel = gtk::Button::builder()
                .icon_name("process-stop-symbolic")
                .tooltip_text(gettext("Cancel"))
                .valign(gtk::Align::Center)
                .css_classes(["flat"])
                .build();
            cancel.connect_clicked(glib::clone!(
                #[strong]
                entry,
                move |_| {
                    let item_id = entry.item_id.to_owned();
                    let media_source_id = entry.media_source_id.to_owned();
                    DOWNLOAD_MANAGER.set_base_dir(SETTINGS.download_dir());
                    spawn(async move {
                        let _ = spawn_tokio(async move {
                            DOWNLOAD_MANAGER
                                .cancel(&item_id, &media_source_id)
                                .await
                        })
                        .await;
                    });
                }
            ));
            row.add_suffix(&cancel);
        }
    }

    let remove = gtk::Button::builder()
        .icon_name("user-trash-symbolic")
        .tooltip_text(gettext("Remove"))
        .valign(gtk::Align::Center)
        .css_classes(["flat"])
        .build();
    remove.connect_clicked(glib::clone!(
        #[strong]
        entry,
        move |_| {
            let item_id = entry.item_id.to_owned();
            let media_source_id = entry.media_source_id.to_owned();
            DOWNLOAD_MANAGER.set_base_dir(SETTINGS.download_dir());
            spawn(async move {
                let _ = spawn_tokio(async move {
                    DOWNLOAD_MANAGER
                        .remove(&item_id, &media_source_id)
                        .await
                })
                .await;
            });
        }
    ));
    row.add_suffix(&remove);

    row.upcast()
}

fn row_subtitle(entry: &DownloadEntry) -> String {
    let status = gettext(&entry.status.to_string());
    let progress = match entry.total_bytes {
        Some(total) if total > 0 => format!(
            "{} / {}",
            bytefmt::format(entry.downloaded_bytes),
            bytefmt::format(total)
        ),
        _ => bytefmt::format(entry.downloaded_bytes),
    };

    match &entry.error {
        Some(error) if !error.is_empty() => format!("{status} - {progress} - {error}"),
        _ => format!("{status} - {progress}"),
    }
}
