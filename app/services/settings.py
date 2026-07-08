from __future__ import annotations

from dataclasses import dataclass, field

from PyQt6.QtCore import QSettings


@dataclass
class DescriptionSettings:
    tmdb_enabled: bool = False
    tmdb_api_key: str = ""
    tmdb_read_token: str = ""
    imdb_scrape_enabled: bool = True


@dataclass
class AppSettings:
    imdb_db_path: str = "imdb.db"
    collection_db_path: str = "media_collection.db"
    image_dir: str = "imdb_images"
    description: DescriptionSettings = field(default_factory=DescriptionSettings)


class SettingsStore:
    def __init__(self):
        self.settings = QSettings("IMDBDBTools", "NewVersion")

    def load(self) -> AppSettings:
        return AppSettings(
            imdb_db_path=self.settings.value("paths/imdb_db", "imdb.db", type=str),
            collection_db_path=self.settings.value("paths/collection_db", "media_collection.db", type=str),
            image_dir=self.settings.value("paths/image_dir", "imdb_images", type=str),
            description=DescriptionSettings(
                tmdb_enabled=self.settings.value("description/tmdb_enabled", False, type=bool),
                tmdb_api_key=self.settings.value("description/tmdb_api_key", "", type=str) or "",
                tmdb_read_token=self.settings.value("description/tmdb_read_token", "", type=str) or "",
                imdb_scrape_enabled=self.settings.value("description/imdb_scrape_enabled", True, type=bool),
            ),
        )

    def save(self, app_settings: AppSettings) -> None:
        self.settings.setValue("paths/imdb_db", app_settings.imdb_db_path)
        self.settings.setValue("paths/collection_db", app_settings.collection_db_path)
        self.settings.setValue("paths/image_dir", app_settings.image_dir)
        self.settings.setValue("description/tmdb_enabled", app_settings.description.tmdb_enabled)
        self.settings.setValue("description/tmdb_api_key", app_settings.description.tmdb_api_key)
        self.settings.setValue("description/tmdb_read_token", app_settings.description.tmdb_read_token)
        self.settings.setValue("description/imdb_scrape_enabled", app_settings.description.imdb_scrape_enabled)
        self.settings.sync()
