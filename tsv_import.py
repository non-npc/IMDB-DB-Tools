import os
import sys
import gzip
import sqlite3
import csv
import tempfile
import shutil
from pathlib import Path
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                            QHBoxLayout, QLabel, QProgressBar, QPushButton, 
                            QComboBox, QCheckBox, QGroupBox, QMessageBox,
                            QFileDialog, QListWidget, QListWidgetItem)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QSize

# Increase the CSV field size limit to handle large text fields
csv.field_size_limit(1024 * 1024 * 10)  # Set to 10MB, which should be more than enough

class ImportThread(QThread):
    """Thread for importing TSV files to avoid freezing the UI"""
    progress_update = pyqtSignal(int, str)
    import_complete = pyqtSignal(bool, str)
    
    def __init__(self, db_path, import_files, imdb_folder, append_mode=False):
        super().__init__()
        self.db_path = db_path
        self.import_files = import_files
        self.imdb_folder = imdb_folder
        self.is_cancelled = False
        self.temp_dir = None
        self.append_mode = append_mode
    
    def run(self):
        """Run the import process"""
        try:
            # Create or use an existing temporary directory in the project folder
            project_dir = os.path.dirname(os.path.abspath(__file__))
            temp_base_dir = os.path.join(project_dir, "temp")
            os.makedirs(temp_base_dir, exist_ok=True)
            self.temp_dir = temp_base_dir
            self.progress_update.emit(1, f"Using temporary directory: {self.temp_dir}")
            
            # Create database connection
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Disable foreign key constraints for faster import
            cursor.execute('PRAGMA foreign_keys = OFF')
            cursor.execute('PRAGMA synchronous = OFF')
            cursor.execute('PRAGMA journal_mode = MEMORY')
            cursor.execute('PRAGMA temp_store = MEMORY')
            cursor.execute('PRAGMA cache_size = 10000')
            
            # First, read all headers and create tables
            mode_str = "Appending to" if self.append_mode else "Creating"
            self.progress_update.emit(5, f"{mode_str} tables and reading file headers...")
            for file_name in self.import_files:
                if self.is_cancelled:
                    break
                
                file_path = os.path.join(self.imdb_folder, file_name)
                table_name = self.get_table_name(file_name)
                
                if not table_name:
                    self.progress_update.emit(-1, f"Skipping unknown file: {file_name}")
                    continue
                
                # Read headers and create table
                self.create_table_from_headers(cursor, file_name, file_path, table_name)
            
            conn.commit()
            
            # Now import the data
            total_files = len(self.import_files)
            self.progress_update.emit(10, "Starting data import...")
            
            # Calculate file sizes to weight progress by file size
            file_sizes = {}
            total_size = 0
            for file_name in self.import_files:
                file_path = os.path.join(self.imdb_folder, file_name)
                if os.path.exists(file_path):
                    size = os.path.getsize(file_path)
                    file_sizes[file_name] = size
                    total_size += size
            
            # Import each file with progress weighted by file size
            imported_size = 0
            for i, file_name in enumerate(self.import_files):
                if self.is_cancelled:
                    break
                
                file_path = os.path.join(self.imdb_folder, file_name)
                table_name = self.get_table_name(file_name)
                
                if not table_name:
                    continue
                
                # Calculate base progress percentage for this file (10-90%)
                file_size = file_sizes.get(file_name, 0)
                file_weight = file_size / total_size if total_size > 0 else 1/total_files
                base_progress = 10 + int(imported_size / total_size * 80) if total_size > 0 else int(10 + (i / total_files) * 80)
                
                self.progress_update.emit(base_progress, f"Importing {file_name}...")
                
                # Import the file - the import_file method will update progress within the file
                self.current_file = file_name
                self.current_file_base_progress = base_progress
                self.current_file_max_progress = base_progress + int(file_weight * 80)
                
                self.import_file(cursor, file_name, file_path, table_name, conn)
                conn.commit()
                
                imported_size += file_size
            
            # Create indexes after import for better performance
            if not self.is_cancelled:
                self.progress_update.emit(90, "Creating indexes...")
                self.create_indexes(cursor)
                
                # Re-enable foreign key constraints
                self.progress_update.emit(95, "Re-enabling foreign key constraints...")
                cursor.execute('PRAGMA foreign_keys = ON')
                
                conn.commit()
                self.progress_update.emit(100, "Import completed successfully")
            
            conn.close()
            
            # We no longer clean up the temporary directory - keep files for future use
            self.progress_update.emit(-1, f"Keeping extracted files in {self.temp_dir} for future imports")
            
            if self.is_cancelled:
                self.import_complete.emit(False, "Import cancelled")
            else:
                self.import_complete.emit(True, "Import completed successfully")
                
        except Exception as e:
            self.import_complete.emit(False, f"Error during import: {str(e)}")
            # We don't clean up the temp directory even on error
    
    def cancel(self):
        """Cancel the import process"""
        self.is_cancelled = True
    
    def extract_gzip_file(self, gz_path, output_path):
        """Extract a gzipped file to the specified output path if it doesn't already exist"""
        # Check if the file already exists in the temp directory
        if os.path.exists(output_path):
            file_size = os.path.getsize(output_path)
            if file_size > 0:  # Make sure it's not an empty file
                self.progress_update.emit(-1, f"Using existing extracted file: {os.path.basename(output_path)} ({file_size:,} bytes)")
                return True
        
        # File doesn't exist or is empty, extract it
        self.progress_update.emit(-1, f"Extracting {os.path.basename(gz_path)}...")
        
        try:
            with gzip.open(gz_path, 'rb') as f_in:
                with open(output_path, 'wb') as f_out:
                    # Use a buffer for efficient extraction of large files
                    buffer_size = 10 * 1024 * 1024  # 10MB buffer
                    while True:
                        if self.is_cancelled:
                            return False
                        
                        buffer = f_in.read(buffer_size)
                        if not buffer:
                            break
                        f_out.write(buffer)
            
            return True
        except Exception as e:
            self.progress_update.emit(-1, f"Error extracting {os.path.basename(gz_path)}: {str(e)}")
            return False
    
    def create_table_from_headers(self, cursor, file_name, file_path, table_name):
        """Create a table based on the headers in the TSV file"""
        try:
            # Check if the table already exists
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
            table_exists = cursor.fetchone() is not None
            
            if table_exists:
                self.progress_update.emit(-1, f"Table {table_name} already exists, will append data")
                
                # Get existing columns to verify compatibility
                cursor.execute(f"PRAGMA table_info({table_name})")
                existing_columns = [row[1] for row in cursor.fetchall()]
                
                # Open the gzipped TSV file just to read the header
                with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                    # Read the header to get column names
                    reader = csv.reader(f, delimiter='\t')
                    headers = next(reader)
                
                # Check if headers match existing columns
                missing_columns = [h for h in headers if h not in existing_columns]
                if missing_columns:
                    self.progress_update.emit(-1, f"Warning: The following columns in the file are not in the existing table: {missing_columns}")
                
                return headers
            
            # Table doesn't exist, create it
            # Open the gzipped TSV file just to read the header
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                # Read the header to get column names
                reader = csv.reader(f, delimiter='\t')
                headers = next(reader)
                
                self.progress_update.emit(-1, f"Creating table {table_name} with columns: {headers}")
                
                # Determine primary key based on file type
                primary_key = self.get_primary_key(file_name, headers)
                
                # Create SQL for table creation
                columns_sql = []
                for header in headers:
                    # Determine column type based on header name
                    col_type = self.get_column_type(header)
                    columns_sql.append(f"{header} {col_type}")
                
                # Add primary key constraint if applicable
                if primary_key:
                    if isinstance(primary_key, list):
                        columns_sql.append(f"PRIMARY KEY ({', '.join(primary_key)})")
                    else:
                        # Modify the column definition to include PRIMARY KEY
                        for i, col in enumerate(columns_sql):
                            if col.startswith(f"{primary_key} "):
                                columns_sql[i] = f"{primary_key} {self.get_column_type(primary_key)} PRIMARY KEY"
                                break
                
                # Create the table
                create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n  " + ",\n  ".join(columns_sql) + "\n)"
                self.progress_update.emit(-1, f"SQL: {create_sql}")
                cursor.execute(create_sql)
                
                return headers
                
        except Exception as e:
            error_msg = f"Error creating table for {file_name}: {str(e)}"
            self.progress_update.emit(-1, error_msg)
            raise Exception(error_msg)
    
    def get_column_type(self, header):
        """Determine SQLite column type based on header name"""
        # Headers that typically contain numeric values
        if header in ['isAdult', 'startYear', 'endYear', 'runtimeMinutes', 'seasonNumber', 
                      'episodeNumber', 'ordering', 'isOriginalTitle', 'numVotes']:
            return 'INTEGER'
        # Headers that typically contain floating point values
        elif header in ['averageRating']:
            return 'REAL'
        # All other headers are treated as text
        else:
            return 'TEXT'
    
    def get_primary_key(self, file_name, headers):
        """Determine primary key based on file type"""
        if file_name == 'title.basics.tsv.gz' and 'tconst' in headers:
            return 'tconst'
        elif file_name == 'name.basics.tsv.gz' and 'nconst' in headers:
            return 'nconst'
        elif file_name == 'title.ratings.tsv.gz' and 'tconst' in headers:
            return 'tconst'
        elif file_name == 'title.crew.tsv.gz' and 'tconst' in headers:
            return 'tconst'
        elif file_name == 'title.episode.tsv.gz' and 'tconst' in headers:
            return 'tconst'
        elif file_name == 'title.akas.tsv.gz' and 'titleId' in headers and 'ordering' in headers:
            return ['titleId', 'ordering']
        elif file_name == 'title.principals.tsv.gz' and 'tconst' in headers and 'ordering' in headers:
            return ['tconst', 'ordering']
        return None
    
    def create_indexes(self, cursor):
        """Create indexes for better query performance"""
        # Get all tables in the database
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        for table in tables:
            # Get columns for this table
            cursor.execute(f"PRAGMA table_info({table})")
            columns = [row[1] for row in cursor.fetchall()]
            
            # Create indexes based on column names
            for column in columns:
                # Create indexes for common join and search columns
                if column in ['tconst', 'nconst', 'titleId', 'parentTconst']:
                    index_name = f"idx_{table}_{column}"
                    cursor.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table} ({column})")
                    self.progress_update.emit(-1, f"Created index {index_name}")
                
                # Create indexes for text columns that are likely to be searched
                elif column in ['primaryTitle', 'originalTitle', 'primaryName', 'title']:
                    index_name = f"idx_{table}_{column}"
                    cursor.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table} ({column})")
                    self.progress_update.emit(-1, f"Created index {index_name}")
    
    def import_file(self, cursor, file_name, file_path, table_name, conn):
        """Import a specific TSV file into the database"""
        try:
            # Extract the gzipped file to a temporary location
            extracted_file = os.path.join(self.temp_dir, file_name.replace('.gz', ''))
            
            if not self.extract_gzip_file(file_path, extracted_file):
                raise Exception(f"Failed to extract {file_name}")
            
            # Get file size for progress calculation
            file_size = os.path.getsize(extracted_file)
            self.progress_update.emit(-1, f"Extracted file size: {file_size:,} bytes")
            
            # Count lines in the file for accurate progress reporting
            self.progress_update.emit(-1, f"Counting lines in {file_name}...")
            total_lines = 0
            with open(extracted_file, 'r', encoding='utf-8') as f:
                # Count lines efficiently for large files
                for _ in f:
                    total_lines += 1
                    # Check periodically if cancelled
                    if total_lines % 1000000 == 0 and self.is_cancelled:
                        return
            
            # Subtract 1 for the header
            total_rows = total_lines - 1
            self.progress_update.emit(-1, f"Total rows in {file_name}: {total_rows:,}")
            
            # Now process the extracted file
            with open(extracted_file, 'r', encoding='utf-8') as f:
                # Read the header to get column names
                reader = csv.reader(f, delimiter='\t')
                file_headers = next(reader)  # Skip the header row
                
                # Check if we're appending to an existing table
                if self.append_mode:
                    # Get existing table columns
                    cursor.execute(f"PRAGMA table_info({table_name})")
                    table_columns = [row[1] for row in cursor.fetchall()]
                    
                    # Check for missing columns
                    missing_columns = [h for h in file_headers if h not in table_columns]
                    if missing_columns:
                        self.progress_update.emit(-1, f"Warning: The following columns from {file_name} will be ignored: {missing_columns}")
                    
                    # Only use columns that exist in the table
                    headers = [h for h in file_headers if h in table_columns]
                    
                    if not headers:
                        self.progress_update.emit(-1, f"Error: No matching columns found between file and table for {file_name}")
                        return
                    
                    self.progress_update.emit(-1, f"Using {len(headers)} of {len(file_headers)} columns from {file_name}")
                else:
                    # Use all headers from the file
                    headers = file_headers
                
                # Prepare SQL for insertion
                placeholders = ', '.join(['?'] * len(headers))
                columns = ', '.join(headers)
                
                sql = f"INSERT OR IGNORE INTO {table_name} ({columns}) VALUES ({placeholders})"
                
                # Process the data in batches for better performance
                batch_size = 5000  # Reduced batch size for better handling of large fields
                batch = []
                row_count = 0
                error_count = 0
                last_progress = 0
                batch_count = 0
                
                # Get base progress from run method if available
                base_progress = getattr(self, 'current_file_base_progress', 0)
                max_progress = getattr(self, 'current_file_max_progress', 100)
                progress_range = max_progress - base_progress
                
                # Create a mapping from file column indices to table column indices
                if self.append_mode and len(headers) != len(file_headers):
                    # Create a mapping of file column indices to the indices in our headers list
                    column_mapping = []
                    for i, col in enumerate(file_headers):
                        if col in headers:
                            column_mapping.append(headers.index(col))
                        else:
                            column_mapping.append(-1)  # -1 means this column will be ignored
                else:
                    # If not appending or all columns match, use a direct mapping
                    column_mapping = list(range(len(headers)))
                
                # Process rows in batches with error handling
                while True:
                    if self.is_cancelled:
                        break
                    
                    # Read a batch of rows
                    current_batch_rows = []
                    for _ in range(batch_size):
                        try:
                            row = next(reader)
                            current_batch_rows.append(row)
                        except StopIteration:
                            break
                        except csv.Error as e:
                            error_count += 1
                            self.progress_update.emit(-1, f"CSV error in {file_name}: {str(e)}")
                            # Skip this row and continue
                            continue
                    
                    # If no rows were read, we're done
                    if not current_batch_rows:
                        break
                    
                    # Process the batch
                    for row in current_batch_rows:
                        # Skip rows that don't have enough columns
                        if len(row) < len(file_headers):
                            continue
                        
                        try:
                            # If we're appending and column mapping is needed
                            if self.append_mode and len(headers) != len(file_headers):
                                # Map values from file columns to table columns
                                mapped_row = []
                                for i, val in enumerate(row):
                                    if i < len(column_mapping) and column_mapping[i] != -1:
                                        mapped_row.append(val)
                            else:
                                # Use all values directly
                                mapped_row = row
                            
                            # Convert '\N' to None for SQL
                            values = [None if field == '\\N' else field for field in mapped_row]
                            
                            batch.append(values)
                            row_count += 1
                        except Exception as e:
                            error_count += 1
                            if error_count < 10:  # Only log the first few errors to avoid flooding
                                self.progress_update.emit(-1, f"Error processing row in {file_name}: {str(e)}")
                    
                    # Execute the batch if it's full
                    if batch:
                        try:
                            cursor.executemany(sql, batch)
                            batch_count += 1
                            
                            # Commit after each batch to ensure data is saved to disk
                            conn.commit()
                            
                            # Get current database size for reporting
                            db_size = os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
                            db_size_mb = db_size / (1024 * 1024)
                            
                            batch = []
                            
                            # Calculate progress within this file (0-100%)
                            file_progress = min(95, int((row_count / total_rows) * 100))
                            
                            # Map file progress to overall progress
                            overall_progress = base_progress + int((file_progress / 100) * progress_range)
                            
                            # Only emit progress update if it's changed significantly
                            if overall_progress > last_progress:
                                self.progress_update.emit(overall_progress, 
                                                         f"Importing {file_name}: {row_count:,} rows processed, {error_count} errors, DB size: {db_size_mb:.2f} MB")
                                last_progress = overall_progress
                        except sqlite3.Error as e:
                            # If the batch fails, try inserting rows one by one to identify problematic rows
                            self.progress_update.emit(-1, f"Batch insert error in {file_name}: {str(e)}")
                            self.progress_update.emit(-1, "Trying individual row inserts...")
                            
                            for values in batch:
                                try:
                                    cursor.execute(sql, values)
                                except sqlite3.Error as row_error:
                                    error_count += 1
                                    if error_count < 50:  # Limit error reporting
                                        self.progress_update.emit(-1, f"Row insert error: {str(row_error)}")
                            
                            # Commit after individual inserts
                            conn.commit()
                            batch = []
                
                # Insert any remaining rows
                if batch and not self.is_cancelled:
                    try:
                        cursor.executemany(sql, batch)
                        conn.commit()  # Commit the final batch
                    except sqlite3.Error as e:
                        # If the batch fails, try inserting rows one by one
                        self.progress_update.emit(-1, f"Final batch insert error: {str(e)}")
                        for values in batch:
                            try:
                                cursor.execute(sql, values)
                            except sqlite3.Error:
                                error_count += 1
                        conn.commit()  # Commit after individual inserts
                
                # Get final database size
                db_size = os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
                db_size_mb = db_size / (1024 * 1024)
                
                # Final progress update for this file
                if not self.is_cancelled:
                    self.progress_update.emit(max_progress, 
                                             f"Completed importing {file_name}: {row_count:,} rows imported, {error_count} errors, {batch_count} batches, DB size: {db_size_mb:.2f} MB")
            
            # We no longer delete the extracted file - keep it for future use
                
        except Exception as e:
            error_msg = f"Error importing {file_name}: {str(e)}"
            self.progress_update.emit(-1, error_msg)
            raise Exception(error_msg)
    
    def get_table_name(self, file_name):
        """Map file name to table name"""
        # Remove .tsv.gz extension and replace dots with underscores
        base_name = file_name.replace('.tsv.gz', '')
        table_name = base_name.replace('.', '_')
        return table_name

class IMDBImportApp(QMainWindow):
    def __init__(self):
        super().__init__()
        
        self.imdb_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'imdb')
        self.db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'imdb.db')
        self.import_thread = None
        self.available_files = []
        
        self.init_ui()
        self.scan_imdb_folder()
    
    def init_ui(self):
        """Initialize the user interface"""
        self.setWindowTitle("IMDB Dataset Importer")
        self.setGeometry(100, 100, 600, 550)  # Make window a bit taller
        
        # Main widget and layout
        main_widget = QWidget()
        main_layout = QVBoxLayout(main_widget)
        
        # Folder selection
        folder_group = QGroupBox("IMDB Dataset Folder")
        folder_layout = QHBoxLayout(folder_group)
        
        self.folder_label = QLabel(self.imdb_folder)
        self.folder_label.setWordWrap(True)
        folder_button = QPushButton("Browse...")
        folder_button.clicked.connect(self.browse_folder)
        
        folder_layout.addWidget(self.folder_label, 1)
        folder_layout.addWidget(folder_button)
        
        # Temp folder display
        temp_group = QGroupBox("Temporary Files Location")
        temp_layout = QHBoxLayout(temp_group)
        
        temp_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "temp")
        self.temp_label = QLabel(temp_dir)
        self.temp_label.setWordWrap(True)
        temp_button = QPushButton("Open Folder")
        temp_button.clicked.connect(self.open_temp_folder)
        
        temp_layout.addWidget(self.temp_label, 1)
        temp_layout.addWidget(temp_button)
        
        # Available files list
        files_group = QGroupBox("Available Datasets")
        files_layout = QVBoxLayout(files_group)
        
        self.files_list = QListWidget()
        self.files_list.setSelectionMode(QListWidget.SelectionMode.MultiSelection)
        
        select_all_button = QPushButton("Select All")
        select_all_button.clicked.connect(self.select_all_files)
        
        files_layout.addWidget(self.files_list)
        files_layout.addWidget(select_all_button)
        
        # Progress area
        progress_group = QGroupBox("Import Progress")
        progress_layout = QVBoxLayout(progress_group)
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        
        self.status_label = QLabel("Ready to import")
        
        progress_layout.addWidget(self.progress_bar)
        progress_layout.addWidget(self.status_label)
        
        # Action buttons
        button_layout = QHBoxLayout()
        
        self.import_button = QPushButton("Start Import")
        self.import_button.clicked.connect(self.start_import)
        
        self.cancel_button = QPushButton("Cancel")
        self.cancel_button.clicked.connect(self.cancel_import)
        self.cancel_button.setEnabled(False)
        
        button_layout.addWidget(self.import_button)
        button_layout.addWidget(self.cancel_button)
        
        # Add all components to main layout
        main_layout.addWidget(folder_group)
        main_layout.addWidget(temp_group)
        main_layout.addWidget(files_group, 1)  # Give this more space
        main_layout.addWidget(progress_group)
        main_layout.addLayout(button_layout)
        
        self.setCentralWidget(main_widget)
    
    def scan_imdb_folder(self):
        """Scan the IMDB folder for available dataset files"""
        self.available_files = []
        self.files_list.clear()
        
        if not os.path.exists(self.imdb_folder):
            self.status_label.setText(f"Folder not found: {self.imdb_folder}")
            return
        
        # List of expected IMDB dataset files
        expected_files = [
            'title.akas.tsv.gz',
            'title.basics.tsv.gz',
            'title.crew.tsv.gz',
            'title.episode.tsv.gz',
            'title.principals.tsv.gz',
            'title.ratings.tsv.gz',
            'name.basics.tsv.gz'
        ]
        
        # Check which files exist
        for file_name in expected_files:
            file_path = os.path.join(self.imdb_folder, file_name)
            if os.path.exists(file_path):
                self.available_files.append(file_name)
                item = QListWidgetItem(file_name)
                self.files_list.addItem(item)
        
        if not self.available_files:
            self.status_label.setText("No IMDB dataset files found in the selected folder")
        else:
            self.status_label.setText(f"Found {len(self.available_files)} dataset files")
    
    def browse_folder(self):
        """Open a folder browser dialog to select the IMDB dataset folder"""
        folder = QFileDialog.getExistingDirectory(self, "Select IMDB Dataset Folder", self.imdb_folder)
        if folder:
            self.imdb_folder = folder
            self.folder_label.setText(folder)
            self.scan_imdb_folder()
    
    def select_all_files(self):
        """Select all files in the list"""
        for i in range(self.files_list.count()):
            self.files_list.item(i).setSelected(True)
    
    def start_import(self):
        """Start the import process"""
        # Get selected files
        selected_files = [item.text() for item in self.files_list.selectedItems()]
        
        if not selected_files:
            QMessageBox.warning(self, "No Files Selected", "Please select at least one dataset file to import.")
            return
        
        # Default to not appending (creating new database)
        append_mode = False
        
        # Check if database file already exists
        if os.path.exists(self.db_path):
            # Give options to replace, append, or cancel
            message_box = QMessageBox()
            message_box.setWindowTitle("Database Exists")
            message_box.setText(f"The database file '{self.db_path}' already exists.")
            message_box.setInformativeText("What would you like to do?")
            
            replace_button = message_box.addButton("Replace", QMessageBox.ButtonRole.DestructiveRole)
            append_button = message_box.addButton("Append", QMessageBox.ButtonRole.AcceptRole)
            cancel_button = message_box.addButton("Cancel", QMessageBox.ButtonRole.RejectRole)
            
            message_box.exec()
            
            clicked_button = message_box.clickedButton()
            
            if clicked_button == cancel_button:
                return
            elif clicked_button == replace_button:
                # Delete existing database
                try:
                    os.remove(self.db_path)
                    append_mode = False  # We're creating a new database
                except Exception as e:
                    QMessageBox.critical(self, "Error", f"Could not delete existing database: {str(e)}")
                    return
            elif clicked_button == append_button:
                append_mode = True  # We're appending to existing database
        
        # Update UI
        self.import_button.setEnabled(False)
        self.cancel_button.setEnabled(True)
        self.progress_bar.setValue(0)
        
        mode_str = "Appending to" if append_mode else "Creating new"
        self.status_label.setText(f"Starting import... ({mode_str} database)")
        
        # Start import thread
        self.import_thread = ImportThread(self.db_path, selected_files, self.imdb_folder, append_mode=append_mode)
        self.import_thread.progress_update.connect(self.update_progress)
        self.import_thread.import_complete.connect(self.import_finished)
        self.import_thread.start()
    
    def cancel_import(self):
        """Cancel the import process"""
        if self.import_thread and self.import_thread.isRunning():
            self.status_label.setText("Cancelling import...")
            self.import_thread.cancel()
    
    def update_progress(self, progress, message):
        """Update the progress bar and status message"""
        if progress >= 0:
            self.progress_bar.setValue(progress)
            # Make sure the progress bar is visible and updates immediately
            QApplication.processEvents()
        
        # Always update the status message
        self.status_label.setText(message)
        # Make sure the UI updates immediately
        QApplication.processEvents()
    
    def import_finished(self, success, message):
        """Handle import completion"""
        self.import_button.setEnabled(True)
        self.cancel_button.setEnabled(False)
        
        if success:
            self.progress_bar.setValue(100)
            QMessageBox.information(self, "Import Complete", message)
        else:
            QMessageBox.warning(self, "Import Failed", message)
        
        self.status_label.setText(message)
    
    def open_temp_folder(self):
        """Open the temporary folder in the file explorer"""
        temp_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "temp")
        os.makedirs(temp_dir, exist_ok=True)  # Ensure it exists
        
        # Open folder in file explorer - platform specific
        if sys.platform == 'win32':
            os.startfile(temp_dir)
        elif sys.platform == 'darwin':  # macOS
            import subprocess
            subprocess.Popen(['open', temp_dir])
        else:  # Linux
            import subprocess
            subprocess.Popen(['xdg-open', temp_dir])

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = IMDBImportApp()
    window.show()
    sys.exit(app.exec()) 