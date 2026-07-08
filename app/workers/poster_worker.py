from __future__ import annotations

import asyncio

from PyQt6.QtCore import QThread, pyqtSignal

from app.services.poster_scraper import PosterScraper


class PosterDownloadWorker(QThread):
    downloaded = pyqtSignal(str, str)
    failed = pyqtSignal(str, str)

    def __init__(self, image_dir: str, tconst: str):
        super().__init__()
        self.image_dir = image_dir
        self.tconst = tconst

    def run(self):
        try:
            path = asyncio.run(PosterScraper(self.image_dir).download_poster(self.tconst))
            if path:
                self.downloaded.emit(self.tconst, str(path))
            else:
                self.failed.emit(self.tconst, "No poster image was found on the IMDb page.")
        except Exception as exc:
            self.failed.emit(self.tconst, str(exc))
