from __future__ import annotations

import sys
from pathlib import Path

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QApplication,
    QCheckBox,
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
    QWidget,
    QWizard,
    QWizardPage,
)

from app.db.schema import SUPPORTED_DATASETS, missing_required_files
from app.workers.import_worker import ImportWorker


class DatasetFolderPage(QWizardPage):
    def __init__(self):
        super().__init__()
        self.setTitle("Dataset Folder")
        self.setSubTitle("Select the folder containing the complete IMDb TSV archive files.")

        layout = QVBoxLayout(self)
        folder_layout = QHBoxLayout()
        self.folder_label = QLabel("No folder selected")
        self.browse_button = QPushButton("Browse...")
        self.browse_button.clicked.connect(self.browse)
        folder_layout.addWidget(self.folder_label, 1)
        folder_layout.addWidget(self.browse_button)

        self.file_list = QListWidget()
        self.status_label = QLabel("The importer requires the full supported dataset.")

        layout.addLayout(folder_layout)
        layout.addWidget(self.file_list)
        layout.addWidget(self.status_label)

        self.folder_path: Path | None = None

    def browse(self):
        folder = QFileDialog.getExistingDirectory(self, "Select IMDb Dataset Folder")
        if not folder:
            return
        self.folder_path = Path(folder)
        self.folder_label.setText(folder)
        self.refresh_file_list()
        self.completeChanged.emit()

    def refresh_file_list(self):
        self.file_list.clear()
        available = {path.name for path in self.folder_path.glob("*.tsv.gz")} if self.folder_path else set()
        missing = missing_required_files(available)
        for file_name in SUPPORTED_DATASETS:
            item = QListWidgetItem(file_name)
            item.setCheckState(Qt.CheckState.Checked if file_name in available else Qt.CheckState.Unchecked)
            item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsUserCheckable)
            self.file_list.addItem(item)

        if missing:
            self.status_label.setText(f"Missing {len(missing)} required file(s). Full import cannot start yet.")
        else:
            self.status_label.setText("All supported IMDb dataset files found.")

    def isComplete(self):
        if not self.folder_path:
            return False
        available = {path.name for path in self.folder_path.glob("*.tsv.gz")}
        return not missing_required_files(available)


class ImportPlanPage(QWizardPage):
    def __init__(self):
        super().__init__()
        self.setTitle("Import Plan")
        self.setSubTitle("Review the full dataset import plan before starting.")

        layout = QVBoxLayout(self)
        self.replace_checkbox = QCheckBox("Replace existing imdb.db before import")
        self.replace_checkbox.setChecked(False)
        self.summary = QTextEdit()
        self.summary.setReadOnly(True)
        self.summary.setPlainText(
            "The new importer will import the full supported IMDb dataset, create primary keys, "
            "write import metadata, and create indexes only after data import completes."
        )
        layout.addWidget(self.replace_checkbox)
        layout.addWidget(self.summary)


class ImportProgressPage(QWizardPage):
    def __init__(self):
        super().__init__()
        self.setTitle("Import Progress")
        self.setSubTitle("Progress will show each phase, current file, rows processed, and warnings.")

        layout = QVBoxLayout(self)
        self.phase_label = QLabel("Ready to import")
        self.file_label = QLabel("")
        self.progress_bar = QProgressBar()
        self.log = QTextEdit()
        self.log.setReadOnly(True)
        self.cancel_button = QPushButton("Cancel Import")
        self.cancel_button.setEnabled(False)

        layout.addWidget(self.phase_label)
        layout.addWidget(self.file_label)
        layout.addWidget(self.progress_bar)
        layout.addWidget(self.log, 1)
        layout.addWidget(self.cancel_button)

    def initializePage(self):
        self.wizard().start_import()

    def isComplete(self):
        return self.wizard().import_finished


class ImportWizard(QWizard):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("IMDb Dataset Import Wizard")
        self.resize(820, 620)
        self.folder_page = DatasetFolderPage()
        self.plan_page = ImportPlanPage()
        self.progress_page = ImportProgressPage()
        self.addPage(self.folder_page)
        self.addPage(self.plan_page)
        self.addPage(self.progress_page)
        self.import_worker = None
        self.import_finished = False
        self.progress_page.cancel_button.clicked.connect(self.cancel_import)

    def accept(self):
        if self.import_finished:
            super().accept()
        elif self.currentPage() is self.progress_page:
            QMessageBox.information(self, "Import Running", "The import is still running. Cancel it or wait for it to finish.")
        else:
            super().accept()

    def start_import(self):
        if self.import_worker and self.import_worker.isRunning():
            return

        dataset_folder = self.folder_page.folder_path
        if not dataset_folder:
            QMessageBox.warning(self, "Dataset Folder", "Select a dataset folder before importing.")
            return

        db_path = Path.cwd() / "imdb.db"
        self.progress_page.phase_label.setText("Starting import")
        self.progress_page.file_label.setText("")
        self.progress_page.progress_bar.setValue(0)
        self.progress_page.log.clear()
        self.progress_page.cancel_button.setEnabled(True)
        self.button(QWizard.WizardButton.FinishButton).setEnabled(False)
        self.button(QWizard.WizardButton.BackButton).setEnabled(False)

        self.import_worker = ImportWorker(
            dataset_folder=dataset_folder,
            db_path=db_path,
            replace_existing=self.plan_page.replace_checkbox.isChecked(),
        )
        self.import_worker.phase_changed.connect(self.progress_page.phase_label.setText)
        self.import_worker.file_changed.connect(self.progress_page.file_label.setText)
        self.import_worker.progress_changed.connect(self.progress_page.progress_bar.setValue)
        self.import_worker.log_message.connect(self.append_log)
        self.import_worker.finished.connect(self.handle_import_finished)
        self.import_worker.start()

    def append_log(self, message: str):
        self.progress_page.log.append(message)

    def cancel_import(self):
        if self.import_worker and self.import_worker.isRunning():
            self.progress_page.cancel_button.setEnabled(False)
            self.progress_page.phase_label.setText("Cancelling after current batch...")
            self.import_worker.cancel()

    def handle_import_finished(self, success: bool, message: str):
        self.import_finished = success
        self.progress_page.cancel_button.setEnabled(False)
        self.button(QWizard.WizardButton.BackButton).setEnabled(not success)
        self.progress_page.log.append(message)
        if success:
            self.progress_page.progress_bar.setValue(100)
            self.progress_page.phase_label.setText("Import complete")
            QMessageBox.information(self, "Import Complete", message)
        else:
            self.progress_page.phase_label.setText("Import stopped")
            QMessageBox.warning(self, "Import Stopped", message)
        self.progress_page.completeChanged.emit()


def run_importer():
    app = QApplication(sys.argv)
    wizard = ImportWizard()
    wizard.show()
    sys.exit(app.exec())
