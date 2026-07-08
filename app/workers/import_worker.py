from __future__ import annotations

import csv
import gzip
import os
import sqlite3
import time
from pathlib import Path

from PyQt6.QtCore import QThread, pyqtSignal

from app.db.schema import FULL_DATASET_TABLES, SUPPORTED_DATASETS, metadata_schema_sql


csv.field_size_limit(1024 * 1024 * 10)


PRIMARY_KEYS = {
    "title.basics.tsv.gz": ["tconst"],
    "title.akas.tsv.gz": ["titleId", "ordering"],
    "title.crew.tsv.gz": ["tconst"],
    "title.episode.tsv.gz": ["tconst"],
    "title.principals.tsv.gz": ["tconst", "ordering"],
    "title.ratings.tsv.gz": ["tconst"],
    "name.basics.tsv.gz": ["nconst"],
}


INTEGER_COLUMNS = {
    "isAdult",
    "startYear",
    "endYear",
    "runtimeMinutes",
    "seasonNumber",
    "episodeNumber",
    "ordering",
    "isOriginalTitle",
    "numVotes",
}


REAL_COLUMNS = {"averageRating"}


INDEX_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_title_basics_primaryTitle ON title_basics(primaryTitle)",
    "CREATE INDEX IF NOT EXISTS idx_title_basics_originalTitle ON title_basics(originalTitle)",
    "CREATE INDEX IF NOT EXISTS idx_title_basics_startYear ON title_basics(startYear)",
    "CREATE INDEX IF NOT EXISTS idx_title_basics_titleType ON title_basics(titleType)",
    "CREATE INDEX IF NOT EXISTS idx_title_ratings_averageRating ON title_ratings(averageRating)",
    "CREATE INDEX IF NOT EXISTS idx_name_basics_primaryName ON name_basics(primaryName)",
    "CREATE INDEX IF NOT EXISTS idx_title_principals_tconst ON title_principals(tconst)",
    "CREATE INDEX IF NOT EXISTS idx_title_principals_nconst ON title_principals(nconst)",
    "CREATE INDEX IF NOT EXISTS idx_title_crew_directors ON title_crew(directors)",
    "CREATE INDEX IF NOT EXISTS idx_title_episode_parentTconst ON title_episode(parentTconst)",
]


class ImportWorker(QThread):
    phase_changed = pyqtSignal(str)
    file_changed = pyqtSignal(str)
    progress_changed = pyqtSignal(int)
    log_message = pyqtSignal(str)
    finished = pyqtSignal(bool, str)

    def __init__(self, dataset_folder: str | Path, db_path: str | Path, replace_existing: bool):
        super().__init__()
        self.dataset_folder = Path(dataset_folder)
        self.db_path = Path(db_path)
        self.replace_existing = replace_existing
        self.cancel_requested = False

    def cancel(self):
        self.cancel_requested = True

    def run(self):
        conn = None
        try:
            self.phase_changed.emit("Preparing database")
            self.progress_changed.emit(0)
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

            if self.replace_existing and self.db_path.exists():
                self.log_message.emit(f"Replacing existing database: {self.db_path}")
                self.db_path.unlink()

            conn = sqlite3.connect(self.db_path)
            self.configure_connection(conn)
            self.create_metadata_tables(conn)

            total_files = len(SUPPORTED_DATASETS)
            for file_index, file_name in enumerate(SUPPORTED_DATASETS):
                if self.cancel_requested:
                    self.finished.emit(False, "Import cancelled")
                    return

                file_path = self.dataset_folder / file_name
                table_name = FULL_DATASET_TABLES[file_name]
                self.file_changed.emit(file_name)
                self.phase_changed.emit(f"Importing {file_name}")
                self.log_message.emit(f"Reading headers for {file_name}")

                headers = self.read_headers(file_path)
                self.create_table(conn, file_name, table_name, headers)
                row_count = self.import_file(conn, file_path, table_name, headers)
                self.record_dataset(conn, file_name, table_name, row_count)

                progress = int(((file_index + 1) / total_files) * 85)
                self.progress_changed.emit(progress)

            if self.cancel_requested:
                self.finished.emit(False, "Import cancelled")
                return

            self.phase_changed.emit("Creating indexes")
            self.file_changed.emit("")
            self.create_indexes(conn)
            self.progress_changed.emit(95)

            self.phase_changed.emit("Finalizing")
            self.write_metadata(conn)
            conn.commit()
            self.progress_changed.emit(100)
            self.finished.emit(True, f"Full IMDb dataset import completed: {self.db_path}")
        except Exception as exc:
            self.finished.emit(False, f"Import failed: {exc}")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def configure_connection(self, conn: sqlite3.Connection):
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("PRAGMA journal_mode = MEMORY")
        conn.execute("PRAGMA temp_store = MEMORY")
        conn.execute("PRAGMA cache_size = 100000")

    def create_metadata_tables(self, conn: sqlite3.Connection):
        for sql in metadata_schema_sql():
            conn.execute(sql)
        conn.commit()

    def read_headers(self, file_path: Path) -> list[str]:
        with gzip.open(file_path, "rt", encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle, delimiter="\t")
            return next(reader)

    def create_table(self, conn: sqlite3.Connection, file_name: str, table_name: str, headers: list[str]):
        columns_sql = [f"{header} {self.column_type(header)}" for header in headers]
        primary_key = PRIMARY_KEYS[file_name]
        if len(primary_key) == 1:
            key = primary_key[0]
            columns_sql = [
                f"{header} {self.column_type(header)} PRIMARY KEY" if header == key else column_sql
                for header, column_sql in zip(headers, columns_sql)
            ]
        else:
            columns_sql.append(f"PRIMARY KEY ({', '.join(primary_key)})")

        sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n  " + ",\n  ".join(columns_sql) + "\n)"
        conn.execute(sql)
        conn.commit()

    def import_file(self, conn: sqlite3.Connection, file_path: Path, table_name: str, headers: list[str]) -> int:
        placeholders = ", ".join(["?"] * len(headers))
        columns = ", ".join(headers)
        sql = f"INSERT OR REPLACE INTO {table_name} ({columns}) VALUES ({placeholders})"
        batch = []
        row_count = 0
        repaired_short_rows = 0
        repaired_long_rows = 0
        last_log = time.time()

        with gzip.open(file_path, "rt", encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle, delimiter="\t")
            next(reader)
            for row in reader:
                if self.cancel_requested:
                    break

                if len(row) < len(headers):
                    repaired_short_rows += 1
                    row = row + ["\\N"] * (len(headers) - len(row))
                elif len(row) > len(headers):
                    repaired_long_rows += 1
                    row = row[:len(headers)]

                batch.append([None if value == "\\N" else value for value in row])
                row_count += 1

                if len(batch) >= 10000:
                    conn.executemany(sql, batch)
                    conn.commit()
                    batch.clear()

                if time.time() - last_log >= 2:
                    db_size = self.db_path.stat().st_size / (1024 * 1024) if self.db_path.exists() else 0
                    repair_text = ""
                    if repaired_short_rows or repaired_long_rows:
                        repair_text = f", repaired rows: {repaired_short_rows + repaired_long_rows:,}"
                    self.log_message.emit(f"{file_path.name}: {row_count:,} rows imported{repair_text}, database {db_size:,.1f} MB")
                    last_log = time.time()

        if batch and not self.cancel_requested:
            conn.executemany(sql, batch)
            conn.commit()

        repair_summary = ""
        if repaired_short_rows or repaired_long_rows:
            repair_summary = f" ({repaired_short_rows:,} short rows padded, {repaired_long_rows:,} long rows trimmed)"
        self.log_message.emit(f"Completed {file_path.name}: {row_count:,} rows imported{repair_summary}")
        return row_count

    def record_dataset(self, conn: sqlite3.Connection, file_name: str, table_name: str, row_count: int):
        conn.execute(
            """
            INSERT OR REPLACE INTO imported_datasets (file_name, table_name, row_count, imported_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (file_name, table_name, row_count),
        )
        conn.commit()

    def create_indexes(self, conn: sqlite3.Connection):
        for index, sql in enumerate(INDEX_SQL, start=1):
            if self.cancel_requested:
                return
            self.log_message.emit(f"Creating index {index}/{len(INDEX_SQL)}")
            conn.execute(sql)
            self.progress_changed.emit(85 + int((index / len(INDEX_SQL)) * 10))
        conn.commit()

    def write_metadata(self, conn: sqlite3.Connection):
        conn.execute(
            "INSERT OR REPLACE INTO import_metadata (key, value) VALUES (?, ?)",
            ("schema_version", "1"),
        )
        conn.execute(
            "INSERT OR REPLACE INTO import_metadata (key, value) VALUES (?, CURRENT_TIMESTAMP)",
            ("last_full_import",),
        )

    def column_type(self, header: str) -> str:
        if header in INTEGER_COLUMNS:
            return "INTEGER"
        if header in REAL_COLUMNS:
            return "REAL"
        return "TEXT"
