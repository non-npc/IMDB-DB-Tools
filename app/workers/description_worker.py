from __future__ import annotations

import asyncio

from PyQt6.QtCore import QThread, pyqtSignal

from app.services.description_service import DescriptionService
from app.services.settings import DescriptionSettings


class DescriptionFetchWorker(QThread):
    fetched = pyqtSignal(str, str, str)
    failed = pyqtSignal(str, str)

    def __init__(
        self,
        settings: DescriptionSettings,
        tconst: str,
        title: str,
        year: int | None,
        title_type: str | None,
    ):
        super().__init__()
        self.settings = settings
        self.tconst = tconst
        self.title = title
        self.year = year
        self.title_type = title_type

    def run(self):
        try:
            result = asyncio.run(
                DescriptionService(self.settings).fetch(self.tconst, self.title, self.year, self.title_type)
            )
            if result:
                self.fetched.emit(self.tconst, result.text, result.source)
            else:
                self.failed.emit(self.tconst, "No description was found.")
        except Exception as exc:
            self.failed.emit(self.tconst, str(exc))
