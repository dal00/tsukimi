use std::{
    collections::HashSet,
    path::PathBuf,
    sync::RwLock,
    time::Duration,
};

use anyhow::{
    Result,
    anyhow,
};
use once_cell::sync::Lazy;
use reqwest::StatusCode;
use serde::{
    Deserialize,
    Serialize,
};
use tokio::{
    fs,
    io::AsyncWriteExt,
    sync::{
        Mutex,
        Semaphore,
    },
};

use super::{
    jellyfin_client::JELLYFIN_CLIENT,
    structs::MediaSource,
};
use crate::utils::spawn_tokio_without_await;

pub static DOWNLOAD_MANAGER: Lazy<DownloadManager> = Lazy::new(DownloadManager::default);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DownloadStatus {
    Queued,
    Downloading,
    Completed,
    Failed,
    Cancelled,
}

impl std::fmt::Display for DownloadStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DownloadStatus::Queued => write!(f, "Queued"),
            DownloadStatus::Downloading => write!(f, "Downloading"),
            DownloadStatus::Completed => write!(f, "Completed"),
            DownloadStatus::Failed => write!(f, "Failed"),
            DownloadStatus::Cancelled => write!(f, "Cancelled"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadEntry {
    pub item_id: String,
    pub media_source_id: String,
    pub name: String,
    pub series_name: Option<String>,
    pub index_number: Option<u32>,
    pub parent_index_number: Option<u32>,
    pub run_time_ticks: Option<u64>,
    pub container: Option<String>,
    pub total_bytes: Option<u64>,
    pub downloaded_bytes: u64,
    pub media_path: PathBuf,
    pub part_path: PathBuf,
    pub status: DownloadStatus,
    pub error: Option<String>,
}

impl DownloadEntry {
    pub fn display_title(&self) -> String {
        match (&self.series_name, self.parent_index_number, self.index_number) {
            (Some(series), Some(season), Some(episode)) => {
                format!("{series} - S{season}E{episode}: {}", self.name)
            }
            _ => self.name.to_owned(),
        }
    }

    pub fn progress_fraction(&self) -> f64 {
        self.total_bytes
            .filter(|total| *total > 0)
            .map(|total| (self.downloaded_bytes as f64 / total as f64).clamp(0.0, 1.0))
            .unwrap_or(0.0)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct DownloadManifest {
    entries: Vec<DownloadEntry>,
}

pub struct DownloadManager {
    manifest: Mutex<Option<DownloadManifest>>,
    loaded_manifest_path: Mutex<Option<PathBuf>>,
    cancelled: Mutex<HashSet<(String, String)>>,
    semaphore: Semaphore,
    base_dir: RwLock<Option<PathBuf>>,
}

impl Default for DownloadManager {
    fn default() -> Self {
        Self {
            manifest: Mutex::new(None),
            loaded_manifest_path: Mutex::new(None),
            cancelled: Mutex::new(HashSet::new()),
            semaphore: Semaphore::new(1),
            base_dir: RwLock::new(None),
        }
    }
}

impl DownloadManager {
    pub fn set_base_dir(&self, path: String) {
        let path = if path.is_empty() {
            None
        } else {
            Some(PathBuf::from(path))
        };
        if let Ok(mut base_dir) = self.base_dir.write() {
            *base_dir = path;
        }
    }

    pub async fn entries(&self) -> Vec<DownloadEntry> {
        let _ = self.load_manifest().await;
        self.reconcile_files().await;
        self.manifest
            .lock()
            .await
            .as_ref()
            .map(|m| m.entries.to_owned())
            .unwrap_or_default()
    }

    pub async fn has_active_downloads(&self) -> bool {
        self.entries().await.into_iter().any(|entry| {
            matches!(
                entry.status,
                DownloadStatus::Queued | DownloadStatus::Downloading
            )
        })
    }

    pub async fn completed_for(
        &self, item_id: &str, media_source_id: Option<&str>,
    ) -> Option<DownloadEntry> {
        let entries = self.entries().await;
        entries.into_iter().find(|entry| {
            entry.item_id.as_str() == item_id
                && media_source_id
                    .map(|id| entry.media_source_id.as_str() == id)
                    .unwrap_or(true)
                && entry.status == DownloadStatus::Completed
                && entry.media_path.exists()
        })
    }

    pub async fn start_video_download(&self, item_id: &str) -> Result<()> {
        self.load_manifest().await?;
        let item = JELLYFIN_CLIENT.get_item_download_info(item_id).await?;
        if !matches!(item.item_type.as_str(), "Movie" | "Episode" | "Video" | "MusicVideo" | "AdultVideo") {
            return Err(anyhow!("Only video items can be downloaded"));
        }

        let playback = JELLYFIN_CLIENT
            .get_playbackinfo(item_id, None, None, false)
            .await?;
        let media_source = playback
            .media_sources
            .first()
            .ok_or_else(|| anyhow!("No media source found"))?
            .to_owned();
        let key = (item_id.to_string(), media_source.id.to_owned());

        {
            let mut manifest = self.manifest.lock().await;
            let manifest = manifest.get_or_insert_with(DownloadManifest::default);
            if let Some(entry) = manifest
                .entries
                .iter_mut()
                .find(|entry| {
                    entry.item_id.as_str() == key.0.as_str()
                        && entry.media_source_id.as_str() == key.1.as_str()
                })
            {
                match entry.status {
                    DownloadStatus::Completed if entry.media_path.exists() => return Ok(()),
                    DownloadStatus::Queued | DownloadStatus::Downloading => return Ok(()),
                    _ => {
                        entry.status = DownloadStatus::Queued;
                        entry.error = None;
                    }
                }
            } else {
                manifest.entries.push(self.new_entry(&item, &media_source).await?);
            }
        }

        self.save_manifest().await?;
        self.cancelled.lock().await.remove(&key);

        let item_id = item_id.to_string();
        spawn_tokio_without_await(async move {
            let _ = DOWNLOAD_MANAGER.run_download(&item_id, &media_source).await;
        });

        Ok(())
    }

    pub async fn cancel(&self, item_id: &str, media_source_id: &str) -> Result<()> {
        self.cancelled
            .lock()
            .await
            .insert((item_id.to_string(), media_source_id.to_string()));
        self.update_entry(item_id, media_source_id, |entry| {
            if entry.status != DownloadStatus::Completed {
                entry.status = DownloadStatus::Cancelled;
            }
        })
        .await
    }

    pub async fn remove(&self, item_id: &str, media_source_id: &str) -> Result<()> {
        self.load_manifest().await?;
        let (removed, should_cancel) = {
            let manifest = self.manifest.lock().await;
            let removed = manifest.as_ref().and_then(|manifest| {
                manifest
                    .entries
                    .iter()
                    .find(|entry| {
                        entry.item_id.as_str() == item_id
                            && entry.media_source_id.as_str() == media_source_id
                    })
                    .cloned()
            });
            let should_cancel = removed.as_ref().is_some_and(|entry| {
                matches!(entry.status, DownloadStatus::Queued | DownloadStatus::Downloading)
            });
            (removed, should_cancel)
        };

        if should_cancel {
            self.cancelled
                .lock()
                .await
                .insert((item_id.to_string(), media_source_id.to_string()));
        }

        {
            let mut manifest = self.manifest.lock().await;
            let manifest = manifest.get_or_insert_with(DownloadManifest::default);
            manifest.entries.retain(|entry| {
                !(entry.item_id.as_str() == item_id
                    && entry.media_source_id.as_str() == media_source_id)
            });
        }

        if let Some(entry) = removed {
            let _ = fs::remove_file(entry.media_path).await;
            let _ = fs::remove_file(entry.part_path).await;
        }

        self.save_manifest().await
    }

    pub async fn clear(&self) -> Result<()> {
        for entry in self.entries().await {
            self.cancelled
                .lock()
                .await
                .insert((entry.item_id, entry.media_source_id));
        }
        let root = self.download_root().await;
        if root.exists() {
            fs::remove_dir_all(&root).await?;
        }
        fs::create_dir_all(&root).await?;
        *self.manifest.lock().await = Some(DownloadManifest::default());
        self.save_manifest().await
    }

    async fn run_download(&self, item_id: &str, media_source: &MediaSource) -> Result<()> {
        let _permit = self.semaphore.acquire().await?;
        let media_source_id = media_source.id.to_owned();
        if self
            .cancelled
            .lock()
            .await
            .contains(&(item_id.to_string(), media_source_id.to_owned()))
        {
            return Ok(());
        }
        self.update_entry(item_id, &media_source_id, |entry| {
            entry.status = DownloadStatus::Downloading;
            entry.error = None;
        })
        .await?;

        let result = self.download_file(item_id, media_source).await;
        match result {
            Ok(()) => {
                self.update_entry(item_id, &media_source_id, |entry| {
                    entry.status = DownloadStatus::Completed;
                    entry.error = None;
                    entry.downloaded_bytes = entry.total_bytes.unwrap_or(entry.downloaded_bytes);
                })
                .await
            }
            Err(err) => {
                let message = err.to_string();
                self.update_entry(item_id, &media_source_id, |entry| {
                    if entry.status != DownloadStatus::Cancelled {
                        entry.status = DownloadStatus::Failed;
                    }
                    entry.error = Some(message);
                })
                .await
            }
        }
    }

    async fn download_file(&self, item_id: &str, media_source: &MediaSource) -> Result<()> {
        let media_source_id = media_source.id.to_owned();
        let entry = self
            .entry(item_id, &media_source_id)
            .await
            .ok_or_else(|| anyhow!("Download entry not found"))?;

        if let Some(parent) = entry.part_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let mut offset = fs::metadata(&entry.part_path)
            .await
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        let url = JELLYFIN_CLIENT
            .get_static_video_stream_url(
                media_source.container.as_deref(),
                item_id,
                &media_source_id,
                media_source.etag.as_deref(),
            )
            .await?;

        let mut response = JELLYFIN_CLIENT
            .request_download_url(&url, Some(offset))
            .await?
            .error_for_status()?;

        if offset > 0 && response.status() != StatusCode::PARTIAL_CONTENT {
            offset = 0;
            let _ = fs::remove_file(&entry.part_path).await;
        }

        let total = response
            .content_length()
            .map(|length| length + offset)
            .or(media_source.size);
        self.update_entry(item_id, &media_source_id, |entry| {
            entry.total_bytes = total;
            entry.downloaded_bytes = offset;
        })
        .await?;

        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(offset > 0)
            .write(true)
            .truncate(offset == 0)
            .open(&entry.part_path)
            .await?;

        let mut downloaded = offset;
        let mut last_save = std::time::Instant::now();
        while let Some(chunk) = response.chunk().await? {
            if self
                .cancelled
                .lock()
                .await
                .contains(&(item_id.to_string(), media_source_id.to_string()))
            {
                return Err(anyhow!("Download cancelled"));
            }

            file.write_all(&chunk).await?;
            downloaded += chunk.len() as u64;

            if last_save.elapsed() >= Duration::from_millis(500) {
                self.update_entry(item_id, &media_source_id, |entry| {
                    entry.downloaded_bytes = downloaded;
                })
                .await?;
                last_save = std::time::Instant::now();
            }
        }

        if self
            .cancelled
            .lock()
            .await
            .contains(&(item_id.to_string(), media_source_id.to_string()))
        {
            return Err(anyhow!("Download cancelled"));
        }

        file.flush().await?;
        fs::rename(&entry.part_path, &entry.media_path).await?;
        Ok(())
    }

    async fn new_entry(
        &self, item: &super::structs::SimpleListItem, media_source: &MediaSource,
    ) -> Result<DownloadEntry> {
        let root = self.download_root().await.join(&item.id);
        let container = media_source
            .container
            .to_owned()
            .unwrap_or_else(|| "mkv".to_string());
        let filename = format!("{}.{}", sanitize_filename(&item.name), container);
        let media_path = root.join(filename);
        let part_path = media_path.with_extension(format!("{container}.part"));

        Ok(DownloadEntry {
            item_id: item.id.to_owned(),
            media_source_id: media_source.id.to_owned(),
            name: item.name.to_owned(),
            series_name: item.series_name.to_owned(),
            index_number: item.index_number,
            parent_index_number: item.parent_index_number,
            run_time_ticks: item.run_time_ticks,
            container: media_source.container.to_owned(),
            total_bytes: media_source.size,
            downloaded_bytes: 0,
            media_path,
            part_path,
            status: DownloadStatus::Queued,
            error: None,
        })
    }

    async fn entry(&self, item_id: &str, media_source_id: &str) -> Option<DownloadEntry> {
        self.load_manifest().await.ok()?;
        self.manifest
            .lock()
            .await
            .as_ref()?
            .entries
            .iter()
            .find(|entry| {
                entry.item_id.as_str() == item_id && entry.media_source_id.as_str() == media_source_id
            })
            .cloned()
    }

    async fn update_entry<F>(&self, item_id: &str, media_source_id: &str, update: F) -> Result<()>
    where
        F: FnOnce(&mut DownloadEntry) + Send,
    {
        self.load_manifest().await?;
        {
            let mut manifest = self.manifest.lock().await;
            let manifest = manifest.get_or_insert_with(DownloadManifest::default);
            let entry = manifest
                .entries
                .iter_mut()
                .find(|entry| {
                    entry.item_id.as_str() == item_id
                        && entry.media_source_id.as_str() == media_source_id
                })
                .ok_or_else(|| anyhow!("Download entry not found"))?;
            update(entry);
        }
        self.save_manifest().await
    }

    async fn reconcile_files(&self) {
        let mut changed = false;
        {
            let mut manifest = self.manifest.lock().await;
            let Some(manifest) = manifest.as_mut() else {
                return;
            };
            for entry in &mut manifest.entries {
                if entry.status == DownloadStatus::Completed && !entry.media_path.exists() {
                    entry.status = DownloadStatus::Failed;
                    entry.error = Some("Downloaded file is missing".to_string());
                    changed = true;
                }
            }
        }
        if changed {
            let _ = self.save_manifest().await;
        }
    }

    async fn load_manifest(&self) -> Result<()> {
        let path = self.manifest_path().await;
        if self.manifest.lock().await.is_some()
            && self.loaded_manifest_path.lock().await.as_ref() == Some(&path)
        {
            return Ok(());
        }

        let mut manifest = match fs::read_to_string(&path).await {
            Ok(contents) => serde_json::from_str(&contents).unwrap_or_default(),
            Err(_) => DownloadManifest::default(),
        };
        for entry in &mut manifest.entries {
            if matches!(entry.status, DownloadStatus::Queued | DownloadStatus::Downloading) {
                entry.status = DownloadStatus::Failed;
                entry.error = Some("Download interrupted".to_string());
            }
        }
        *self.manifest.lock().await = Some(manifest);
        *self.loaded_manifest_path.lock().await = Some(path);
        Ok(())
    }

    async fn save_manifest(&self) -> Result<()> {
        let path = self.manifest_path().await;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let manifest = self.manifest.lock().await.to_owned().unwrap_or_default();
        fs::write(path, serde_json::to_string_pretty(&manifest)?).await?;
        Ok(())
    }

    async fn manifest_path(&self) -> PathBuf {
        self.download_root().await.join("manifest.json")
    }

    async fn download_root(&self) -> PathBuf {
        let server_hash = JELLYFIN_CLIENT.current_server_hash().await;
        let user_id = JELLYFIN_CLIENT.current_user_id().await;
        let base = self
            .base_dir
            .read()
            .ok()
            .and_then(|base_dir| base_dir.to_owned())
            .unwrap_or_else(|| {
                dirs::data_dir()
                    .unwrap_or_else(|| PathBuf::from("."))
                    .join("tsukimi")
                    .join("downloads")
            });
        let path = base.join(server_hash).join(user_id);
        if !path.exists() {
            let _ = std::fs::create_dir_all(&path);
        }
        path
    }
}

fn sanitize_filename(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .collect();
    let sanitized = sanitized.trim();
    if sanitized.is_empty() {
        "video".to_string()
    } else {
        sanitized.to_string()
    }
}
