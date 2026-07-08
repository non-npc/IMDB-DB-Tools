from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


SUPPORTED_DATASETS = [
    "title.basics.tsv.gz",
    "title.akas.tsv.gz",
    "title.crew.tsv.gz",
    "title.episode.tsv.gz",
    "title.principals.tsv.gz",
    "title.ratings.tsv.gz",
    "name.basics.tsv.gz",
]


FULL_DATASET_TABLES = {
    "title.basics.tsv.gz": "title_basics",
    "title.akas.tsv.gz": "title_akas",
    "title.crew.tsv.gz": "title_crew",
    "title.episode.tsv.gz": "title_episode",
    "title.principals.tsv.gz": "title_principals",
    "title.ratings.tsv.gz": "title_ratings",
    "name.basics.tsv.gz": "name_basics",
}


@dataclass(frozen=True)
class DatasetDefinition:
    file_name: str
    table_name: str
    required_for_full_import: bool = True


FULL_DATASET_DEFINITIONS = [
    DatasetDefinition(file_name=file_name, table_name=table_name)
    for file_name, table_name in FULL_DATASET_TABLES.items()
]


def missing_required_files(available_files: Iterable[str]) -> list[str]:
    available = set(available_files)
    return [file_name for file_name in SUPPORTED_DATASETS if file_name not in available]


def collection_schema_sql() -> list[str]:
    return [
        """
        CREATE TABLE IF NOT EXISTS collection_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tconst TEXT UNIQUE,
            title TEXT NOT NULL,
            title_type TEXT,
            year INTEGER,
            genres TEXT,
            rating REAL,
            votes INTEGER,
            description TEXT,
            description_source TEXT,
            poster_path TEXT,
            watched INTEGER NOT NULL DEFAULT 0,
            owned INTEGER NOT NULL DEFAULT 0,
            notes TEXT,
            date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            date_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS collection_tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS collection_item_tags (
            item_id INTEGER NOT NULL,
            tag_id INTEGER NOT NULL,
            PRIMARY KEY (item_id, tag_id),
            FOREIGN KEY (item_id) REFERENCES collection_items(id) ON DELETE CASCADE,
            FOREIGN KEY (tag_id) REFERENCES collection_tags(id) ON DELETE CASCADE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS title_metadata (
            tconst TEXT PRIMARY KEY,
            description TEXT,
            description_source TEXT,
            poster_path TEXT,
            date_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_collection_items_tconst ON collection_items(tconst)",
        "CREATE INDEX IF NOT EXISTS idx_collection_items_title ON collection_items(title)",
        "CREATE INDEX IF NOT EXISTS idx_collection_items_watched ON collection_items(watched)",
        "CREATE INDEX IF NOT EXISTS idx_collection_items_owned ON collection_items(owned)",
    ]


def metadata_schema_sql() -> list[str]:
    return [
        """
        CREATE TABLE IF NOT EXISTS import_metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS imported_datasets (
            file_name TEXT PRIMARY KEY,
            table_name TEXT NOT NULL,
            row_count INTEGER NOT NULL DEFAULT 0,
            imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
    ]
