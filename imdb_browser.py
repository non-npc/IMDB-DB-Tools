import os
import sys
import sqlite3
import re
import json
import time
import traceback
import requests
from datetime import datetime
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                             QLabel, QLineEdit, QPushButton, QComboBox, QTableWidget, QTableWidgetItem, QHeaderView,
                             QTabWidget, QTextEdit, QMessageBox, QSplitter, QFileDialog,
                             QProgressBar, QGroupBox, QCheckBox, QProgressDialog)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QSize, QTimer
from PyQt6.QtGui import QPixmap, QIcon, QFont

class ImageDownloader(QThread):
    """Thread for downloading images to avoid freezing the UI"""
    progress_update = pyqtSignal(str)
    download_complete = pyqtSignal(bool, str, str)
    
    def __init__(self, tconst, save_path):
        super().__init__()
        self.tconst = tconst
        self.save_path = save_path
        self.is_cancelled = False
    
    def run(self):
        """Run the download process"""
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(self.save_path), exist_ok=True)
            
            # Check if file already exists
            if os.path.exists(self.save_path):
                self.download_complete.emit(True, self.tconst, self.save_path)
                return
            
            # Construct IMDB URL
            imdb_url = f"https://www.imdb.com/title/{self.tconst}/"
            
            # Download the image
            self.progress_update.emit(f"Finding image for {self.tconst}...")
            
            # First, get the IMDB page to find the image URL
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(imdb_url, headers=headers)
            
            if response.status_code != 200:
                self.download_complete.emit(False, self.tconst, f"Failed to access IMDB page: HTTP {response.status_code}")
                return
            
            # Try to find the image URL in the page content
            self.progress_update.emit(f"Extracting image URL for {self.tconst}...")
            
            # Look for the poster image URL in the HTML
            # This is a simplified approach and might need adjustments if IMDB changes their page structure
            html_content = response.text
            
            # Try different patterns to find the image URL
            image_url = None
            
            # Pattern 1: Look for poster image in JSON-LD data
            json_ld_match = re.search(r'"image"\s*:\s*"(https://[^"]+\.jpg)"', html_content)
            if json_ld_match:
                image_url = json_ld_match.group(1)
            
            # Pattern 2: Look for poster image in meta tags
            if not image_url:
                meta_match = re.search(r'<meta property="og:image" content="([^"]+)"', html_content)
                if meta_match:
                    image_url = meta_match.group(1)
            
            # Pattern 3: Look for poster image with ipc-lockup-overlay__screen class
            if not image_url:
                img_match = re.search(r'<div[^>]+class="[^"]*ipc-lockup-overlay__screen[^"]*"[^>]+style="[^"]*background-image:url\(([^)]+)\)', html_content)
                if img_match:
                    image_url = img_match.group(1)
                    # Remove quotes if present
                    image_url = image_url.strip('"\'')
            
            # Pattern 4: Look for poster image in the page content with ipc-image class
            if not image_url:
                img_match = re.search(r'<img[^>]+class="[^"]*ipc-image[^"]*"[^>]+src="([^"]+\.jpg)"', html_content)
                if img_match:
                    image_url = img_match.group(1)
            
            # If we still don't have an image URL, try the old direct URL pattern as a fallback
            if not image_url:
                image_url = f"https://m.media-amazon.com/images/M/{self.tconst}.jpg"
                self.progress_update.emit(f"Using fallback image URL for {self.tconst}...")
            
            # Download the image
            self.progress_update.emit(f"Downloading image for {self.tconst}...")
            img_response = requests.get(image_url, stream=True, headers=headers)
            
            if img_response.status_code == 200:
                with open(self.save_path, 'wb') as f:
                    for chunk in img_response.iter_content(1024):
                        if self.is_cancelled:
                            break
                        f.write(chunk)
                
                if not self.is_cancelled:
                    self.download_complete.emit(True, self.tconst, self.save_path)
                else:
                    # Clean up partial download
                    if os.path.exists(self.save_path):
                        os.remove(self.save_path)
                    self.download_complete.emit(False, self.tconst, "Download cancelled")
            else:
                self.download_complete.emit(False, self.tconst, f"Failed to download image: HTTP {img_response.status_code}")
        
        except Exception as e:
            self.download_complete.emit(False, self.tconst, f"Error downloading image: {str(e)}")
    
    def cancel(self):
        """Cancel the download process"""
        self.is_cancelled = True

class SearchThread(QThread):
    """Thread for performing searches in the background and updating results progressively"""
    result_batch_ready = pyqtSignal(list, int)
    search_complete = pyqtSignal(int)
    
    def __init__(self, db_manager, search_params, page_size=20):
        super().__init__()
        self.db_manager = db_manager
        self.search_params = search_params
        self.page_size = page_size
        self.is_cancelled = False
        self.batch_size = 20  # Increased batch size for better performance
    
    def run(self):
        """Run the search process"""
        try:
            conn = self.db_manager.connect()[0]
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Set a timeout for the query to prevent long-running queries
            cursor.execute("PRAGMA timeout = 30000")  # 30 seconds timeout
            
            # Determine which search method to use based on search type
            if self.search_params['search_type'] == "cast":
                # Search by cast member name
                query, params = self.build_cast_search_query()
            elif self.search_params['search_type'] == "director":
                # Search by director name
                query, params = self.build_director_search_query()
            else:
                # Regular title search
                query, params = self.build_title_search_query()
            
            # Add a limit to the query to prevent excessive results
            if "LIMIT" not in query:
                query += " LIMIT 1000"  # Limit to 1000 results maximum
            
            # Execute the query without pagination to get all results
            cursor.execute(query, params)
            
            # Process results in batches
            results = []
            total_count = 0
            
            while True:
                if self.is_cancelled:
                    break
                    
                # Fetch a batch of rows
                rows = cursor.fetchmany(self.batch_size)
                if not rows:
                    break
                    
                # Convert rows to dictionaries
                batch_results = [dict(row) for row in rows]
                results.extend(batch_results)
                total_count += len(batch_results)
                
                # Emit signal with the current batch of results
                self.result_batch_ready.emit(batch_results, total_count)
            
            conn.close()
            
            # Emit signal that search is complete
            self.search_complete.emit(total_count)
            
        except Exception as e:
            print(f"Error in search thread: {str(e)}")
            self.search_complete.emit(-1)  # Negative value indicates error
    
    def build_title_search_query(self):
        """Build the SQL query for title search"""
        query = """
        SELECT tb.tconst, tb.titleType, tb.primaryTitle, tb.originalTitle, 
               tb.startYear, tb.endYear, tb.runtimeMinutes, tb.genres,
               tr.averageRating, tr.numVotes
        FROM title_basics tb
        LEFT JOIN title_ratings tr ON tb.tconst = tr.tconst
        WHERE 1=1
        """
        params = []
        
        # Add search conditions
        if self.search_params['search_term']:
            query += " AND (tb.primaryTitle LIKE ? OR tb.originalTitle LIKE ?)"
            params.extend([f"%{self.search_params['search_term']}%", f"%{self.search_params['search_term']}%"])
        
        if self.search_params['title_type']:
            query += " AND tb.titleType = ?"
            params.append(self.search_params['title_type'])
        
        if self.search_params['genre']:
            query += " AND tb.genres LIKE ?"
            params.append(f"%{self.search_params['genre']}%")
        
        if self.search_params['year']:
            query += " AND tb.startYear = ?"
            params.append(self.search_params['year'])
        
        if self.search_params['min_rating']:
            query += " AND tr.averageRating >= ?"
            params.append(self.search_params['min_rating'])
        
        # Add order by - first by year (latest first), then by rating
        query += " ORDER BY CASE WHEN tb.startYear IS NULL THEN 0 ELSE tb.startYear END DESC, CASE WHEN tr.averageRating IS NULL THEN 0 ELSE tr.averageRating END DESC, tb.primaryTitle"
        
        return query, params
    
    def build_cast_search_query(self):
        """Build the SQL query for cast member search"""
        # Use a more efficient query with better join order and limiting
        query = """
        WITH matching_people AS (
            SELECT nconst, primaryName 
            FROM name_basics 
            WHERE primaryName LIKE ? 
            LIMIT 100
        )
        SELECT DISTINCT tb.tconst, tb.titleType, tb.primaryTitle, tb.originalTitle, 
               tb.startYear, tb.endYear, tb.runtimeMinutes, tb.genres,
               tr.averageRating, tr.numVotes
        FROM matching_people mp
        JOIN title_principals tp ON mp.nconst = tp.nconst
        JOIN title_basics tb ON tp.tconst = tb.tconst
        LEFT JOIN title_ratings tr ON tb.tconst = tr.tconst
        WHERE 1=1
        """
        params = [f"%{self.search_params['search_term']}%"]
        
        # Add additional search conditions
        if self.search_params['title_type']:
            query += " AND tb.titleType = ?"
            params.append(self.search_params['title_type'])
        
        if self.search_params['genre']:
            query += " AND tb.genres LIKE ?"
            params.append(f"%{self.search_params['genre']}%")
        
        if self.search_params['year']:
            query += " AND tb.startYear = ?"
            params.append(self.search_params['year'])
        
        if self.search_params['min_rating']:
            query += " AND tr.averageRating >= ?"
            params.append(self.search_params['min_rating'])
        
        # Add order by - first by year (latest first), then by rating
        query += " ORDER BY CASE WHEN tb.startYear IS NULL THEN 0 ELSE tb.startYear END DESC, CASE WHEN tr.averageRating IS NULL THEN 0 ELSE tr.averageRating END DESC, tb.primaryTitle"
        
        return query, params
    
    def build_director_search_query(self):
        """Build the SQL query for director search"""
        # Use a more efficient query with better join order and limiting
        query = """
        WITH matching_directors AS (
            SELECT nconst, primaryName 
            FROM name_basics 
            WHERE primaryName LIKE ? 
            LIMIT 100
        )
        SELECT DISTINCT tb.tconst, tb.titleType, tb.primaryTitle, tb.originalTitle, 
               tb.startYear, tb.endYear, tb.runtimeMinutes, tb.genres,
               tr.averageRating, tr.numVotes
        FROM matching_directors md
        JOIN title_crew tc ON tc.directors LIKE '%' || md.nconst || '%'
        JOIN title_basics tb ON tc.tconst = tb.tconst
        LEFT JOIN title_ratings tr ON tb.tconst = tr.tconst
        WHERE 1=1
        """
        params = [f"%{self.search_params['search_term']}%"]
        
        # Add additional search conditions
        if self.search_params['title_type']:
            query += " AND tb.titleType = ?"
            params.append(self.search_params['title_type'])
        
        if self.search_params['genre']:
            query += " AND tb.genres LIKE ?"
            params.append(f"%{self.search_params['genre']}%")
        
        if self.search_params['year']:
            query += " AND tb.startYear = ?"
            params.append(self.search_params['year'])
        
        if self.search_params['min_rating']:
            query += " AND tr.averageRating >= ?"
            params.append(self.search_params['min_rating'])
        
        # Add order by - first by year (latest first), then by rating
        query += " ORDER BY CASE WHEN tb.startYear IS NULL THEN 0 ELSE tb.startYear END DESC, CASE WHEN tr.averageRating IS NULL THEN 0 ELSE tr.averageRating END DESC, tb.primaryTitle"
        
        return query, params
    
    def cancel(self):
        """Cancel the search process"""
        self.is_cancelled = True

class IMDBDatabaseManager:
    """Class to handle database operations for the IMDB database"""
    def __init__(self, db_path):
        self.db_path = db_path
        # Create tables if they don't exist (won't affect existing data)
        self.create_tables()
        # Ensure the plots table exists when initializing
        self.create_plots_table()
        # Create indexes if they don't exist
        self.create_indexes()
        # TMDB API credentials
        self.tmdb_api_key = "5c5f1bfbf6d1e1059cd5604602654454"
        self.tmdb_read_token = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI1YzVmMWJmYmY2ZDFlMTA1OWNkNTYwNDYwMjY1NDQ1NCIsIm5iZiI6MTc0MTgwMTUwOC4zMywic3ViIjoiNjdkMWM4MjRkNGY3NDEzNzMyNjBhNjY1Iiwic2NvcGVzIjpbImFwaV9yZWFkIl0sInZlcnNpb24iOjF9.OU2EfCOJSShlG5v62hc5P4-vsxWyPx3oQD7ppi-iYmU"
    
    def create_tables(self):
        """Create the required tables if they don't exist"""
        try:
            conn, cursor = self.connect()
            
            # Create title_basics table if it doesn't exist
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS title_basics (
                tconst TEXT PRIMARY KEY,
                titleType TEXT,
                primaryTitle TEXT,
                originalTitle TEXT,
                isAdult INTEGER,
                startYear INTEGER,
                endYear INTEGER,
                runtimeMinutes INTEGER,
                genres TEXT
            )
            """)
            
            # Create title_ratings table if it doesn't exist
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS title_ratings (
                tconst TEXT PRIMARY KEY,
                averageRating REAL,
                numVotes INTEGER,
                FOREIGN KEY(tconst) REFERENCES title_basics(tconst)
            )
            """)
            
            # Create title_crew table if it doesn't exist
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS title_crew (
                tconst TEXT PRIMARY KEY,
                directors TEXT,
                writers TEXT,
                FOREIGN KEY(tconst) REFERENCES title_basics(tconst)
            )
            """)
            
            # Create title_episode table if it doesn't exist
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS title_episode (
                tconst TEXT PRIMARY KEY,
                parentTconst TEXT,
                seasonNumber INTEGER,
                episodeNumber INTEGER,
                FOREIGN KEY(parentTconst) REFERENCES title_basics(tconst)
            )
            """)
            
            # Create title_akas table if it doesn't exist
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS title_akas (
                titleId TEXT,
                ordering INTEGER,
                title TEXT,
                region TEXT,
                language TEXT,
                types TEXT,
                attributes TEXT,
                isOriginalTitle INTEGER,
                PRIMARY KEY(titleId, ordering),
                FOREIGN KEY(titleId) REFERENCES title_basics(tconst)
            )
            """)
            
            # Create name_basics table if it doesn't exist
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS name_basics (
                nconst TEXT PRIMARY KEY,
                primaryName TEXT,
                birthYear INTEGER,
                deathYear INTEGER,
                primaryProfession TEXT,
                knownForTitles TEXT
            )
            """)
            
            # Create title_principals table if it doesn't exist
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS title_principals (
                tconst TEXT,
                ordering INTEGER,
                nconst TEXT,
                category TEXT,
                job TEXT,
                characters TEXT,
                PRIMARY KEY(tconst, ordering),
                FOREIGN KEY(tconst) REFERENCES title_basics(tconst),
                FOREIGN KEY(nconst) REFERENCES name_basics(nconst)
            )
            """)
            
            conn.commit()
            print("Database tables verified successfully")
            
        except Exception as e:
            print(f"Error verifying tables: {str(e)}")
        finally:
            conn.close()
    
    def connect(self):
        """Connect to the database and return connection and cursor"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        return conn, cursor
    
    def create_plots_table(self):
        """Create the plots table if it doesn't exist"""
        try:
            conn, cursor = self.connect()
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS plots (
                tconst TEXT PRIMARY KEY,
                plot_summary TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error creating plots table: {str(e)}")
    
    def get_plot_from_db(self, tconst):
        """Get plot summary from the database if it exists"""
        try:
            conn, cursor = self.connect()
            cursor.execute("SELECT plot_summary FROM plots WHERE tconst = ?", (tconst,))
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return result[0]
            return None
        except Exception as e:
            print(f"Error retrieving plot from database: {str(e)}")
            return None
    
    def save_plot_to_db(self, tconst, plot_summary):
        """Save plot summary to the database"""
        try:
            conn, cursor = self.connect()
            cursor.execute("""
            INSERT OR REPLACE INTO plots (tconst, plot_summary, last_updated)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            """, (tconst, plot_summary))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error saving plot to database: {str(e)}")
            return False
    
    def get_title_types(self):
        """Get all unique title types from the database"""
        conn, cursor = self.connect()
        cursor.execute("SELECT DISTINCT titleType FROM title_basics ORDER BY titleType")
        types = [row[0] for row in cursor.fetchall()]
        conn.close()
        return types
    
    def get_genres(self):
        """Get all unique genres from the database"""
        conn, cursor = self.connect()
        cursor.execute("SELECT DISTINCT value FROM (SELECT DISTINCT genres FROM title_basics) t, json_each(json_array(t.genres)) WHERE value IS NOT NULL ORDER BY value")
        genres = [row[0] for row in cursor.fetchall()]
        conn.close()
        return genres
    
    def search_titles(self, search_term="", title_type=None, genre=None, year=None, min_rating=None, page=1, page_size=20):
        """Search for titles based on various criteria, with pagination"""
        conn, cursor = self.connect()
        
        # Base query
        query = """
        SELECT tb.tconst, tb.titleType, tb.primaryTitle, tb.originalTitle, 
               tb.startYear, tb.endYear, tb.runtimeMinutes, tb.genres,
               tr.averageRating, tr.numVotes
        FROM title_basics tb
        LEFT JOIN title_ratings tr ON tb.tconst = tr.tconst
        WHERE 1=1
        """
        params = []
        
        # Add search conditions
        if search_term:
            query += " AND (tb.primaryTitle LIKE ? OR tb.originalTitle LIKE ?)"
            params.extend([f"%{search_term}%", f"%{search_term}%"])
        
        if title_type:
            query += " AND tb.titleType = ?"
            params.append(title_type)
        
        if genre:
            query += " AND tb.genres LIKE ?"
            params.append(f"%{genre}%")
        
        if year:
            query += " AND tb.startYear = ?"
            params.append(year)
        
        if min_rating:
            query += " AND tr.averageRating >= ?"
            params.append(min_rating)
        
        # Count total results (for pagination)
        count_query = f"SELECT COUNT(*) FROM ({query}) as count_query"
        cursor.execute(count_query, params)
        total_count = cursor.fetchone()[0]
        
        # Add order by and pagination - first by year (latest first), then by rating
        query += " ORDER BY CASE WHEN tb.startYear IS NULL THEN 0 ELSE tb.startYear END DESC, CASE WHEN tr.averageRating IS NULL THEN 0 ELSE tr.averageRating END DESC, tb.primaryTitle"
        query += " LIMIT ? OFFSET ?"
        
        # Calculate offset
        offset = (page - 1) * page_size
        params.append(page_size)
        params.append(offset)
        
        cursor.execute(query, params)
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return results, total_count
    
    def get_title_details(self, tconst):
        """Get detailed information about a specific title"""
        conn, cursor = self.connect()
        
        # Get basic title information
        cursor.execute("""
        SELECT tb.tconst, tb.titleType, tb.primaryTitle, tb.originalTitle, 
               tb.isAdult, tb.startYear, tb.endYear, tb.runtimeMinutes, tb.genres,
               tr.averageRating, tr.numVotes
        FROM title_basics tb
        LEFT JOIN title_ratings tr ON tb.tconst = tr.tconst
        WHERE tb.tconst = ?
        """, (tconst,))
        
        title_info = dict(cursor.fetchone() or {})
        
        if not title_info:
            conn.close()
            return None
        
        # Get crew information
        cursor.execute("""
        SELECT tc.directors, tc.writers
        FROM title_crew tc
        WHERE tc.tconst = ?
        """, (tconst,))
        
        crew_info = dict(cursor.fetchone() or {})
        title_info.update(crew_info)
        
        # Get director and writer names
        if 'directors' in title_info and title_info['directors']:
            directors = title_info['directors'].split(',')
            director_names = []
            
            for director in directors:
                cursor.execute("""
                SELECT primaryName FROM name_basics
                WHERE nconst = ?
                """, (director,))
                result = cursor.fetchone()
                if result:
                    director_names.append(result[0])
            
            title_info['director_names'] = director_names
        
        if 'writers' in title_info and title_info['writers']:
            writers = title_info['writers'].split(',')
            writer_names = []
            
            for writer in writers:
                cursor.execute("""
                SELECT primaryName FROM name_basics
                WHERE nconst = ?
                """, (writer,))
                result = cursor.fetchone()
                if result:
                    writer_names.append(result[0])
            
            title_info['writer_names'] = writer_names
        
        # Get principal cast
        cursor.execute("""
        SELECT tp.ordering, tp.nconst, tp.category, tp.job, tp.characters,
               nb.primaryName
        FROM title_principals tp
        JOIN name_basics nb ON tp.nconst = nb.nconst
        WHERE tp.tconst = ?
        ORDER BY tp.ordering
        """, (tconst,))
        
        cast = [dict(row) for row in cursor.fetchall()]
        title_info['cast'] = cast
        
        # Get alternative titles
        cursor.execute("""
        SELECT ordering, title, region, language, types, attributes, isOriginalTitle
        FROM title_akas
        WHERE titleId = ?
        ORDER BY ordering
        """, (tconst,))
        
        akas = [dict(row) for row in cursor.fetchall()]
        title_info['akas'] = akas
        
        # If it's a TV series, get episodes
        if title_info['titleType'] == 'tvSeries':
            cursor.execute("""
            SELECT te.tconst, te.seasonNumber, te.episodeNumber,
                   tb.primaryTitle, tb.originalTitle, tb.startYear,
                   tr.averageRating
            FROM title_episode te
            JOIN title_basics tb ON te.tconst = tb.tconst
            LEFT JOIN title_ratings tr ON te.tconst = tr.tconst
            WHERE te.parentTconst = ?
            ORDER BY te.seasonNumber, te.episodeNumber
            """, (tconst,))
            
            episodes = [dict(row) for row in cursor.fetchall()]
            title_info['episodes'] = episodes
        
        conn.close()
        return title_info
    
    def get_person_details(self, nconst):
        """Get detailed information about a person"""
        conn, cursor = self.connect()
        
        # Get basic person information
        cursor.execute("""
        SELECT nconst, primaryName, birthYear, deathYear, primaryProfession, knownForTitles
        FROM name_basics
        WHERE nconst = ?
        """, (nconst,))
        
        person_info = dict(cursor.fetchone() or {})
        
        if not person_info:
            conn.close()
            return None
        
        # Get titles they're known for
        if 'knownForTitles' in person_info and person_info['knownForTitles']:
            known_for_tconsts = person_info['knownForTitles'].split(',')
            known_for_titles = []
            
            placeholders = ','.join(['?'] * len(known_for_tconsts))
            cursor.execute(f"""
            SELECT tb.tconst, tb.primaryTitle, tb.titleType, tb.startYear,
                   tr.averageRating
            FROM title_basics tb
            LEFT JOIN title_ratings tr ON tb.tconst = tr.tconst
            WHERE tb.tconst IN ({placeholders})
            ORDER BY CASE WHEN tb.startYear IS NULL THEN 0 ELSE tb.startYear END DESC
            """, known_for_tconsts)
            
            known_for_titles = [dict(row) for row in cursor.fetchall()]
            person_info['known_for_titles'] = known_for_titles
        
        # Get all titles they've worked on
        cursor.execute("""
        SELECT tp.tconst, tp.category, tp.job, tp.characters,
               tb.primaryTitle, tb.titleType, tb.startYear,
               tr.averageRating
        FROM title_principals tp
        JOIN title_basics tb ON tp.tconst = tb.tconst
        LEFT JOIN title_ratings tr ON tp.tconst = tr.tconst
        WHERE tp.nconst = ?
        ORDER BY CASE WHEN tb.startYear IS NULL THEN 0 ELSE tb.startYear END DESC, 
                 CASE WHEN tr.averageRating IS NULL THEN 0 ELSE tr.averageRating END DESC
        """, (nconst,))
        
        filmography = [dict(row) for row in cursor.fetchall()]
        person_info['filmography'] = filmography
        
        conn.close()
        return person_info
    
    def get_image_path(self, tconst):
        """Get the path to the image for a title, if it exists"""
        app_dir = os.path.dirname(os.path.abspath(__file__))
        image_dir = os.path.join(app_dir, "imdb_images")
        
        # Check for different image extensions
        for ext in ['.jpg', '.jpeg', '.png']:
            image_path = os.path.join(image_dir, f"{tconst}{ext}")
            if os.path.exists(image_path):
                return image_path
        
        return None
    
    def get_plot_from_tmdb(self, tconst, title, year, is_tv=False):
        """Get plot summary from TMDB API"""
        try:
            # First, search for the movie or TV show by title and year
            media_type = "tv" if is_tv else "movie"
            search_url = f"https://api.themoviedb.org/3/search/{media_type}"
            
            headers = {
                "Authorization": f"Bearer {self.tmdb_read_token}",
                "Content-Type": "application/json;charset=utf-8"
            }
            
            params = {
                "api_key": self.tmdb_api_key,
                "query": title,
                "year": year if year and year.isdigit() else None
            }
            
            # Remove None values from params
            params = {k: v for k, v in params.items() if v is not None}
            
            print(f"TMDB Search URL: {search_url} with params: {params}")
            
            # Make the search request
            search_response = requests.get(search_url, headers=headers, params=params)
            
            if search_response.status_code != 200:
                print(f"TMDB search request failed with status code: {search_response.status_code}")
                return None
            
            search_data = search_response.json()
            
            # Check if we found any results
            if not search_data.get('results') or len(search_data['results']) == 0:
                print(f"No results found on TMDB for {title} ({year})")
                return None
            
            # Get the ID of the first result
            tmdb_id = search_data['results'][0]['id']
            print(f"Found TMDB ID: {tmdb_id} for {title}")
            
            # Now get the details for this movie/TV show
            details_url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}"
            
            details_params = {
                "api_key": self.tmdb_api_key,
                "language": "en-US"
            }
            
            print(f"TMDB Details URL: {details_url}")
            
            details_response = requests.get(details_url, headers=headers, params=details_params)
            
            if details_response.status_code != 200:
                print(f"TMDB details request failed with status code: {details_response.status_code}")
                return None
            
            details_data = details_response.json()
            
            # Extract the overview (plot summary)
            overview = details_data.get('overview', '')
            
            if overview:
                print(f"Successfully retrieved overview from TMDB: {overview[:100]}...")
                return overview
            else:
                print("Overview field was empty in TMDB response")
            
            return None
            
        except Exception as e:
            print(f"Error retrieving plot from TMDB: {str(e)}")
            return None
    
    def get_plot_summary(self, tconst):
        """Get plot summary for a title, first checking the database, then TMDB"""
        # First check if we have it in the database
        db_plot = self.get_plot_from_db(tconst)
        if db_plot:
            print(f"Found plot in database for {tconst}")
            return db_plot
        
        # If not in database, get from TMDB
        try:
            # First, get basic title info from our database
            conn, cursor = self.connect()
            cursor.execute("""
            SELECT primaryTitle, titleType, startYear 
            FROM title_basics 
            WHERE tconst = ?
            """, (tconst,))
            
            title_info = cursor.fetchone()
            conn.close()
            
            if title_info:
                title = title_info[0]
                is_tv = title_info[1] in ['tvSeries', 'tvMiniSeries', 'tvSpecial']
                year = str(title_info[2]) if title_info[2] else None
                
                # Get plot from TMDB
                print(f"Requesting plot from TMDB for {tconst} ({title}, {year})")
                tmdb_plot = self.get_plot_from_tmdb(tconst, title, year, is_tv)
                if tmdb_plot:
                    print(f"Successfully retrieved plot from TMDB for {tconst}")
                    return tmdb_plot
                else:
                    print(f"No plot found on TMDB for {tconst}")
            
            # If we get here, we couldn't find a plot
            return "Plot summary not available from TMDB."
            
        except Exception as e:
            print(f"Error retrieving plot from TMDB: {str(e)}")
            return f"Error retrieving plot summary: {str(e)}"
    
    def search_titles_by_cast(self, cast_name, title_type=None, genre=None, year=None, min_rating=None, page=1, page_size=20):
        """Search for titles based on cast member name, with pagination"""
        conn, cursor = self.connect()
        
        # Base query - join with name_basics and title_principals to search by cast name
        query = """
        SELECT DISTINCT tb.tconst, tb.titleType, tb.primaryTitle, tb.originalTitle, 
               tb.startYear, tb.endYear, tb.runtimeMinutes, tb.genres,
               tr.averageRating, tr.numVotes
        FROM title_basics tb
        LEFT JOIN title_ratings tr ON tb.tconst = tr.tconst
        JOIN title_principals tp ON tb.tconst = tp.tconst
        JOIN name_basics nb ON tp.nconst = nb.nconst
        WHERE nb.primaryName LIKE ?
        """
        params = [f"%{cast_name}%"]
        
        # Add additional search conditions
        if title_type:
            query += " AND tb.titleType = ?"
            params.append(title_type)
        
        if genre:
            query += " AND tb.genres LIKE ?"
            params.append(f"%{genre}%")
        
        if year:
            query += " AND tb.startYear = ?"
            params.append(year)
        
        if min_rating:
            query += " AND tr.averageRating >= ?"
            params.append(min_rating)
        
        # Count total results (for pagination)
        count_query = f"SELECT COUNT(*) FROM ({query}) as count_query"
        cursor.execute(count_query, params)
        total_count = cursor.fetchone()[0]
        
        # Add order by and pagination - first by year (latest first), then by rating
        query += " ORDER BY CASE WHEN tb.startYear IS NULL THEN 0 ELSE tb.startYear END DESC, CASE WHEN tr.averageRating IS NULL THEN 0 ELSE tr.averageRating END DESC, tb.primaryTitle"
        query += " LIMIT ? OFFSET ?"
        
        # Calculate offset
        offset = (page - 1) * page_size
        params.append(page_size)
        params.append(offset)
        
        cursor.execute(query, params)
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return results, total_count
    
    def create_indexes(self):
        """Create indexes on the database tables to improve search performance"""
        try:
            conn, cursor = self.connect()
            
            # Check if indexes already exist
            cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_name_basics_primaryName'")
            if not cursor.fetchone():
                print("Creating indexes on database tables...")
                
                # Index on name_basics.primaryName for faster cast member searches
                cursor.execute("CREATE INDEX idx_name_basics_primaryName ON name_basics(primaryName)")
                
                # Index on title_principals.nconst for faster joins
                cursor.execute("CREATE INDEX idx_title_principals_nconst ON title_principals(nconst)")
                
                # Index on title_principals.tconst for faster joins
                cursor.execute("CREATE INDEX idx_title_principals_tconst ON title_principals(tconst)")
                
                # Index on title_basics.primaryTitle for faster title searches
                cursor.execute("CREATE INDEX idx_title_basics_primaryTitle ON title_basics(primaryTitle)")
                
                # Index on title_basics.originalTitle for faster title searches
                cursor.execute("CREATE INDEX idx_title_basics_originalTitle ON title_basics(originalTitle)")
                
                # Index on title_basics.startYear for faster year filtering
                cursor.execute("CREATE INDEX idx_title_basics_startYear ON title_basics(startYear)")
                
                # Index on title_ratings.averageRating for faster rating filtering
                cursor.execute("CREATE INDEX idx_title_ratings_averageRating ON title_ratings(averageRating)")
                
                # Index on title_crew.directors for faster director searches
                cursor.execute("CREATE INDEX idx_title_crew_directors ON title_crew(directors)")
                
                conn.commit()
                print("Database indexes created successfully.")
            else:
                print("Database indexes already exist.")
                
                # Check if we need to add the directors index separately
                cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_title_crew_directors'")
                if not cursor.fetchone():
                    print("Adding index on title_crew.directors...")
                    cursor.execute("CREATE INDEX idx_title_crew_directors ON title_crew(directors)")
                    conn.commit()
                    print("Directors index created successfully.")
                
            conn.close()
        except Exception as e:
            print(f"Error creating indexes: {str(e)}")
    
    def search_titles_by_director(self, director_name, title_type=None, genre=None, year=None, min_rating=None, page=1, page_size=20):
        """Search for titles based on director name, with pagination"""
        conn, cursor = self.connect()
        
        # Base query - join with name_basics and title_crew to search by director name
        query = """
        WITH matching_directors AS (
            SELECT nconst, primaryName 
            FROM name_basics 
            WHERE primaryName LIKE ? 
            LIMIT 100
        )
        SELECT DISTINCT tb.tconst, tb.titleType, tb.primaryTitle, tb.originalTitle, 
               tb.startYear, tb.endYear, tb.runtimeMinutes, tb.genres,
               tr.averageRating, tr.numVotes
        FROM matching_directors md
        JOIN title_crew tc ON tc.directors LIKE '%' || md.nconst || '%'
        JOIN title_basics tb ON tc.tconst = tb.tconst
        LEFT JOIN title_ratings tr ON tb.tconst = tr.tconst
        WHERE 1=1
        """
        params = [f"%{director_name}%"]
        
        # Add additional search conditions
        if title_type:
            query += " AND tb.titleType = ?"
            params.append(title_type)
        
        if genre:
            query += " AND tb.genres LIKE ?"
            params.append(f"%{genre}%")
        
        if year:
            query += " AND tb.startYear = ?"
            params.append(year)
        
        if min_rating:
            query += " AND tr.averageRating >= ?"
            params.append(min_rating)
        
        # Count total results (for pagination)
        count_query = f"SELECT COUNT(*) FROM ({query}) as count_query"
        cursor.execute(count_query, params)
        total_count = cursor.fetchone()[0]
        
        # Add order by and pagination - first by year (latest first), then by rating
        query += " ORDER BY CASE WHEN tb.startYear IS NULL THEN 0 ELSE tb.startYear END DESC, CASE WHEN tr.averageRating IS NULL THEN 0 ELSE tr.averageRating END DESC, tb.primaryTitle"
        query += " LIMIT ? OFFSET ?"
        
        # Calculate offset
        offset = (page - 1) * page_size
        params.append(page_size)
        params.append(offset)
        
        cursor.execute(query, params)
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return results, total_count

class StartupProgressDialog(QMessageBox):
    """Dialog to show startup progress with a progress bar"""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Initializing IMDB Browser")
        self.setIcon(QMessageBox.Icon.Information)
        self.setText("Initializing database connection...")
        
        # Add a progress bar
        self.progress_bar = QProgressBar(self)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setTextVisible(True)
        
        # Add the progress bar to the layout
        layout = self.layout()
        layout.addWidget(self.progress_bar, 1, 1)
        
        # Set a fixed size
        self.setMinimumWidth(400)
        
        # No buttons initially
        self.setStandardButtons(QMessageBox.StandardButton.NoButton)
    
    def update_progress(self, value, message):
        """Update the progress bar and message"""
        self.progress_bar.setValue(value)
        self.setText(message)
        QApplication.processEvents()  # Ensure UI updates
    
    def complete(self, success, message=None):
        """Mark the progress as complete"""
        if success:
            self.progress_bar.setValue(100)
            self.setText("Initialization complete!")
            self.setStandardButtons(QMessageBox.StandardButton.Ok)
        else:
            self.setText(f"Initialization failed: {message}")
            self.setIcon(QMessageBox.Icon.Critical)
            self.setStandardButtons(QMessageBox.StandardButton.Ok)

class IMDBBrowserApp(QMainWindow):
    """Main application window for browsing IMDB data"""
    def __init__(self):
        super().__init__()
        
        # Set up image directory
        app_dir = os.path.dirname(os.path.abspath(__file__))
        self.image_dir = os.path.join(app_dir, "imdb_images")
        os.makedirs(self.image_dir, exist_ok=True)
        
        # Database path - but don't connect yet
        self.db_path = os.path.join(app_dir, "imdb.db")
        self.db_manager = None
        
        # Pagination variables
        self.current_page = 1
        self.page_size = 20
        self.total_pages = 1
        self.total_results = 0
        
        # Search parameters
        self.current_search_params = {
            'search_term': '',
            'search_type': 'title',  # Default to title search
            'title_type': None,
            'genre': None,
            'year': None,
            'min_rating': None
        }
        
        # Set up the UI immediately
        self.init_ui()
        
        # Current selection
        self.current_tconst = None
        self.current_nconst = None
        self.download_threads = {}
        
        # Search thread
        self.search_thread = None
        self.all_results = []  # Store all search results
    
    def init_ui(self):
        """Initialize the user interface"""
        self.setWindowTitle("IMDB Database Browser")
        self.setGeometry(100, 100, 1200, 800)
        
        # Main layout
        main_widget = QWidget()
        main_layout = QVBoxLayout(main_widget)
        
        # Create search panel
        search_group = QGroupBox("Search")
        search_layout = QVBoxLayout(search_group)
        
        # Search controls
        search_controls_layout = QHBoxLayout()
        
        # Search type (Title, Cast, or Director)
        search_type_layout = QVBoxLayout()
        search_type_label = QLabel("Search Type:")
        self.search_type_combo = QComboBox()
        self.search_type_combo.addItem("Title", "title")
        self.search_type_combo.addItem("Cast Member", "cast")
        self.search_type_combo.addItem("Director", "director")
        self.search_type_combo.currentIndexChanged.connect(self.update_search_placeholder)
        search_type_layout.addWidget(search_type_label)
        search_type_layout.addWidget(self.search_type_combo)
        
        # Search term
        search_term_layout = QVBoxLayout()
        search_term_label = QLabel("Search Term:")
        self.search_term_input = QLineEdit()
        self.search_term_input.setPlaceholderText("Enter title or keywords")
        self.search_term_input.returnPressed.connect(self.search_titles)
        search_term_layout.addWidget(search_term_label)
        search_term_layout.addWidget(self.search_term_input)
        
        # Title type filter
        title_type_layout = QVBoxLayout()
        title_type_label = QLabel("Title Type:")
        self.title_type_combo = QComboBox()
        self.title_type_combo.addItem("All Types", None)
        title_type_layout.addWidget(title_type_label)
        title_type_layout.addWidget(self.title_type_combo)
        
        # Genre filter
        genre_layout = QVBoxLayout()
        genre_label = QLabel("Genre:")
        self.genre_combo = QComboBox()
        self.genre_combo.addItem("All Genres", None)
        genre_layout.addWidget(genre_label)
        genre_layout.addWidget(self.genre_combo)
        
        # Year filter
        year_layout = QVBoxLayout()
        year_label = QLabel("Year:")
        self.year_input = QLineEdit()
        self.year_input.setPlaceholderText("e.g. 2020")
        year_layout.addWidget(year_label)
        year_layout.addWidget(self.year_input)
        
        # Rating filter
        rating_layout = QVBoxLayout()
        rating_label = QLabel("Min Rating:")
        self.rating_combo = QComboBox()
        self.rating_combo.addItem("Any Rating", None)
        for rating in range(1, 10):
            self.rating_combo.addItem(f"{rating}+", rating)
        rating_layout.addWidget(rating_label)
        rating_layout.addWidget(self.rating_combo)
        
        # Search button
        search_button_layout = QVBoxLayout()
        search_button_label = QLabel("")  # Empty label for alignment
        self.search_button = QPushButton("Search")
        self.search_button.clicked.connect(self.search_titles)
        search_button_layout.addWidget(search_button_label)
        search_button_layout.addWidget(self.search_button)
        
        # Set relative sizes for search controls
        search_controls_layout.addLayout(search_type_layout, 2)   # Add search type
        search_controls_layout.addLayout(search_term_layout, 3)  # Default size
        search_controls_layout.addLayout(title_type_layout, 2)   # 30% larger
        search_controls_layout.addLayout(genre_layout, 2)        # 30% larger
        search_controls_layout.addLayout(year_layout, 1)         # 50% smaller
        search_controls_layout.addLayout(rating_layout, 2)       # Default size
        search_controls_layout.addLayout(search_button_layout, 1)  # Default size
        
        # Search results table
        self.results_table = QTableWidget()
        self.results_table.setColumnCount(6)
        self.results_table.setHorizontalHeaderLabels(["Image", "Title", "Type", "Year", "Rating", "Genres"])
        self.results_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self.results_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.results_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.results_table.cellDoubleClicked.connect(self.show_title_details)
        
        # Set default row height
        self.results_table.verticalHeader().setDefaultSectionSize(100)
        
        # Pagination and action buttons layout
        action_layout = QHBoxLayout()
        
        # Pagination controls
        pagination_layout = QHBoxLayout()
        
        self.prev_button = QPushButton("Previous")
        self.prev_button.clicked.connect(self.prev_page)
        self.prev_button.setEnabled(False)
        
        self.page_label = QLabel("Page 1 of 1")
        self.page_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        self.next_button = QPushButton("Next")
        self.next_button.clicked.connect(self.next_page)
        self.next_button.setEnabled(False)
        
        pagination_layout.addWidget(self.prev_button)
        pagination_layout.addWidget(self.page_label)
        pagination_layout.addWidget(self.next_button)
        
        # Export button
        self.export_button = QPushButton("Export to Collection")
        self.export_button.clicked.connect(self.export_to_collection)
        self.export_button.setToolTip("Export titles with descriptions and images to media collection database")
        
        # Add pagination and export button to action layout
        action_layout.addLayout(pagination_layout, 3)
        action_layout.addStretch(1)
        action_layout.addWidget(self.export_button, 1)
        
        # Status bar with progress indicator
        status_layout = QHBoxLayout()
        self.status_label = QLabel("Loading database...")
        self.search_progress = QProgressBar()
        self.search_progress.setRange(0, 0)  # Indeterminate progress
        self.search_progress.setVisible(False)
        status_layout.addWidget(self.status_label, 4)
        status_layout.addWidget(self.search_progress, 1)
        
        # Add components to search layout
        search_layout.addLayout(search_controls_layout)
        search_layout.addWidget(self.results_table)
        search_layout.addLayout(action_layout)
        search_layout.addLayout(status_layout)
        
        # Add search panel to main layout
        main_layout.addWidget(search_group)
        
        # Set the central widget
        self.setCentralWidget(main_widget)
    
    def initialize_database(self):
        """Initialize database connection and load filter data after UI is shown"""
        # Create and show progress dialog
        progress_dialog = StartupProgressDialog(self)
        progress_dialog.show()
        progress_dialog.update_progress(10, "Checking database file...")
        QApplication.processEvents()
        
        # Check if the database exists
        if not os.path.exists(self.db_path):
            progress_dialog.complete(False, f"IMDB database not found at:\n{self.db_path}\n\nPlease run the import tool first to create the database.")
            QMessageBox.critical(self, "Database Error", 
                               f"IMDB database not found at:\n{self.db_path}\n\nPlease run the import tool first to create the database.")
            return False
        
        try:
            # Initialize database manager
            progress_dialog.update_progress(20, "Connecting to database...")
            self.db_manager = IMDBDatabaseManager(self.db_path)
            
            # Load filter data with progress updates
            self.load_filter_data(progress_dialog)
            
            # Complete the progress dialog
            progress_dialog.complete(True)
            return True
        except Exception as e:
            progress_dialog.complete(False, str(e))
            QMessageBox.critical(self, "Database Error", 
                               f"Failed to initialize database: {str(e)}")
            return False
    
    def load_filter_data(self, progress_dialog=None):
        """Load data for filter dropdowns"""
        if not self.db_manager:
            return
            
        try:
            # Update status for title types
            self.status_label.setText("Loading title types...")
            if progress_dialog:
                progress_dialog.update_progress(40, "Loading title types...")
            QApplication.processEvents()
            
            # Load title types
            title_types = self.db_manager.get_title_types()
            for title_type in title_types:
                self.title_type_combo.addItem(title_type, title_type)
            
            # Update status for genres
            self.status_label.setText("Loading genres...")
            if progress_dialog:
                progress_dialog.update_progress(70, "Loading genres...")
            QApplication.processEvents()
            
            # Load genres
            genres = self.db_manager.get_genres()
            for genre in genres:
                self.genre_combo.addItem(genre, genre)
            
            # Update final status
            self.status_label.setText("Ready to search")
            if progress_dialog:
                progress_dialog.update_progress(95, "Finalizing...")
            self.search_button.setEnabled(True)
            
        except Exception as e:
            self.status_label.setText(f"Error loading filter data: {str(e)}")
            QMessageBox.warning(self, "Error", f"Failed to load filter data: {str(e)}")
            raise  # Re-raise to be caught by initialize_database
    
    def update_search_placeholder(self):
        """Update the search input placeholder based on the selected search type"""
        search_type = self.search_type_combo.currentData()
        if search_type == "cast":
            self.search_term_input.setPlaceholderText("Enter cast member name (e.g. Bud Spencer)")
        elif search_type == "director":
            self.search_term_input.setPlaceholderText("Enter director name (e.g. Steven Spielberg)")
        else:
            self.search_term_input.setPlaceholderText("Enter title or keywords")
    
    def search_titles(self):
        """Search for titles based on the current filter settings"""
        if not self.db_manager:
            QMessageBox.warning(self, "Database Not Ready", "Please wait for the database to initialize.")
            return
            
        try:
            # Reset pagination and results
            self.current_page = 1
            self.all_results = []
            
            # Clear existing results
            self.results_table.setRowCount(0)
            
            # Get search parameters
            self.current_search_params = {
                'search_term': self.search_term_input.text().strip(),
                'search_type': self.search_type_combo.currentData(),
                'title_type': self.title_type_combo.currentData(),
                'genre': self.genre_combo.currentData(),
                'year': int(self.year_input.text().strip()) if self.year_input.text().strip().isdigit() else None,
                'min_rating': self.rating_combo.currentData()
            }
            
            # Update status and show progress bar
            search_type_text = self.get_search_type_display_text()
            self.status_label.setText(f"Searching for {search_type_text}...")
            self.search_progress.setVisible(True)
            QApplication.processEvents()
            
            # Cancel any existing search thread
            if self.search_thread and self.search_thread.isRunning():
                self.search_thread.cancel()
                self.search_thread.wait()
            
            # Create and start a new search thread
            self.search_thread = SearchThread(self.db_manager, self.current_search_params, self.page_size)
            self.search_thread.result_batch_ready.connect(self.handle_search_batch)
            self.search_thread.search_complete.connect(self.handle_search_complete)
            self.search_thread.start()
            
            # Disable search button while searching
            self.search_button.setEnabled(False)
            
        except Exception as e:
            self.status_label.setText(f"Search error: {str(e)}")
            self.search_progress.setVisible(False)
            QMessageBox.warning(self, "Search Error", f"An error occurred during search: {str(e)}")
    
    def handle_search_batch(self, batch_results, total_count_so_far):
        """Handle a batch of search results"""
        # Add the batch to our full results list
        self.all_results.extend(batch_results)
        
        # Update the results table with the new batch
        self.display_search_batch(batch_results)
        
        # Update status
        search_type_text = self.get_search_type_display_text()
        if self.current_search_params['search_type'] in ["cast", "director"] and self.current_search_params['search_term']:
            self.status_label.setText(f"Found {total_count_so_far} titles with {search_type_text} '{self.current_search_params['search_term']}' (searching...)")
        else:
            self.status_label.setText(f"Found {total_count_so_far} titles (searching...)")
    
    def handle_search_complete(self, total_count):
        """Handle search completion"""
        # Re-enable search button and hide progress bar
        self.search_button.setEnabled(True)
        self.search_progress.setVisible(False)
        
        if total_count < 0:
            # Error occurred
            self.status_label.setText("Error during search. Please try again.")
            return
        
        # Update pagination info
        self.total_results = total_count
        self.total_pages = max(1, (total_count + self.page_size - 1) // self.page_size)
        
        # Update pagination controls
        self.page_label.setText(f"Page {self.current_page} of {self.total_pages}")
        self.prev_button.setEnabled(self.current_page > 1)
        self.next_button.setEnabled(self.current_page < self.total_pages)
        
        # Update status with final count
        search_type_text = self.get_search_type_display_text()
        if self.current_search_params['search_type'] in ["cast", "director"] and self.current_search_params['search_term']:
            self.status_label.setText(f"Found {total_count} titles with {search_type_text} '{self.current_search_params['search_term']}' (showing {min(self.page_size, total_count)} per page)")
        else:
            self.status_label.setText(f"Found {total_count} titles (showing {min(self.page_size, total_count)} per page)")
    
    def get_search_type_display_text(self):
        """Get the display text for the current search type"""
        search_type = self.current_search_params['search_type']
        if search_type == "cast":
            return "cast member"
        elif search_type == "director":
            return "director"
        else:
            return "title"
    
    def display_search_batch(self, batch_results):
        """Display a batch of search results in the table"""
        # Get the current row count
        current_row_count = self.results_table.rowCount()
        
        # Add new results
        for row_offset, title in enumerate(batch_results):
            row = current_row_count + row_offset
            self.results_table.insertRow(row)
            
            # Set a consistent row height for all rows
            self.results_table.setRowHeight(row, 100)
            
            # Image/ID column
            tconst = title.get('tconst', '')
            image_path = self.db_manager.get_image_path(tconst)
            
            if image_path:
                # Create a small thumbnail
                pixmap = QPixmap(image_path)
                if not pixmap.isNull():
                    pixmap = pixmap.scaled(60, 90, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
                    
                    # Create a label to hold the image
                    image_label = QLabel()
                    image_label.setPixmap(pixmap)
                    image_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
                    
                    # Set the label as the cell widget
                    self.results_table.setCellWidget(row, 0, image_label)
            else:
                # Create a download button for entries without images
                download_btn = QPushButton("Get Image")
                download_btn.setProperty("tconst", tconst)  # Store tconst as a property
                download_btn.clicked.connect(self.download_image_from_results)
                
                # Set the button as the cell widget
                self.results_table.setCellWidget(row, 0, download_btn)
            
            # Store tconst as hidden data in the first column
            id_item = QTableWidgetItem(tconst)
            id_item.setData(Qt.ItemDataRole.UserRole, tconst)
            self.results_table.setItem(row, 0, id_item)
            
            # Title
            title_text = title.get('primaryTitle', '')
            if title.get('originalTitle') and title.get('originalTitle') != title_text:
                title_text += f" ({title.get('originalTitle')})"
            title_item = QTableWidgetItem(title_text)
            self.results_table.setItem(row, 1, title_item)
            
            # Type
            type_item = QTableWidgetItem(title.get('titleType', ''))
            self.results_table.setItem(row, 2, type_item)
            
            # Year
            year_text = str(title.get('startYear', ''))
            if title.get('endYear'):
                year_text += f"-{title.get('endYear')}"
            year_item = QTableWidgetItem(year_text)
            self.results_table.setItem(row, 3, year_item)
            
            # Rating
            rating = title.get('averageRating')
            votes = title.get('numVotes')
            rating_text = f"{rating:.1f} ({votes:,})" if rating and votes else ""
            rating_item = QTableWidgetItem(rating_text)
            if rating:
                rating_item.setData(Qt.ItemDataRole.UserRole, float(rating))
            self.results_table.setItem(row, 4, rating_item)
            
            # Genres
            genres_item = QTableWidgetItem(title.get('genres', ''))
            self.results_table.setItem(row, 5, genres_item)
        
        # Resize columns to content
        self.results_table.resizeColumnsToContents()
        # But keep the title column wider
        self.results_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        # Set a fixed width for the image column
        self.results_table.setColumnWidth(0, 80)
        
        # Process events to update the UI
        QApplication.processEvents()
    
    def download_image_from_results(self):
        """Download image for a title from the results table"""
        # Get the button that was clicked
        button = self.sender()
        if not button:
            return
            
        # Get the tconst from the button's properties
        tconst = button.property("tconst")
        if not tconst:
            return
            
        # Get the row of the button
        for row in range(self.results_table.rowCount()):
            if self.results_table.cellWidget(row, 0) == button:
                current_row = row
                break
        else:
            return  # Button not found in table
            
        # Download the image
        self.download_image(tconst)
        
        # After download completes, the handle_download_complete method will be called
        # We need to update the results table there as well
    
    def load_current_page(self):
        """Load the current page of results from the stored all_results list"""
        try:
            # Calculate start and end indices for the current page
            start_idx = (self.current_page - 1) * self.page_size
            end_idx = min(start_idx + self.page_size, len(self.all_results))
            
            # Get the results for the current page
            page_results = self.all_results[start_idx:end_idx]
            
            # Clear the table and display the current page
            self.results_table.setRowCount(0)
            self.display_search_batch(page_results)
            
            # Update pagination controls
            self.page_label.setText(f"Page {self.current_page} of {self.total_pages}")
            self.prev_button.setEnabled(self.current_page > 1)
            self.next_button.setEnabled(self.current_page < self.total_pages)
            
            # Update status
            search_type_text = self.get_search_type_display_text()
            if self.current_search_params['search_type'] in ["cast", "director"] and self.current_search_params['search_term']:
                self.status_label.setText(f"Showing page {self.current_page} of {self.total_pages} - {len(page_results)} titles with {search_type_text} '{self.current_search_params['search_term']}'")
            else:
                self.status_label.setText(f"Showing page {self.current_page} of {self.total_pages} - {len(page_results)} titles")
            
        except Exception as e:
            self.status_label.setText(f"Error loading page: {str(e)}")
            QMessageBox.warning(self, "Error", f"Failed to load page: {str(e)}")
    
    def display_search_results(self, results):
        """Display search results in the table (legacy method, kept for compatibility)"""
        # Clear existing results
        self.results_table.setRowCount(0)
        
        # Use the batch display method
        self.display_search_batch(results)
    
    def prev_page(self):
        """Go to the previous page of results"""
        if self.current_page > 1:
            self.current_page -= 1
            self.load_current_page()
    
    def next_page(self):
        """Go to the next page of results"""
        if self.current_page < self.total_pages:
            self.current_page += 1
            self.load_current_page()
    
    def show_title_details(self, row, column):
        """Show detailed information about the selected title"""
        tconst = self.results_table.item(row, 0).text()
        self.current_tconst = tconst
        
        # Create a new dialog for the details
        details_dialog = QMainWindow(self)
        details_dialog.setWindowTitle("Title Details")
        details_dialog.setGeometry(150, 150, 1000, 700)
        
        # Main widget and layout
        main_widget = QWidget()
        main_layout = QVBoxLayout(main_widget)
        
        # Create a splitter for the left and right panels
        splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # Left panel for image and basic info
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        
        # Image display
        image_label = QLabel()
        image_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        image_label.setMinimumSize(300, 450)
        image_label.setMaximumSize(300, 450)
        image_label.setStyleSheet("border: 1px solid #ccc; background-color: #f0f0f0;")
        
        # Download image button
        download_button = QPushButton("Download Image from IMDB")
        download_button.clicked.connect(lambda: self.download_image(tconst))
        
        # Add image and button to left panel
        left_layout.addWidget(image_label)
        left_layout.addWidget(download_button)
        left_layout.addStretch()
        
        # Right panel for detailed information
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        
        # Create tabs for different types of information
        tabs = QTabWidget()
        
        # Overview tab
        overview_tab = QWidget()
        overview_layout = QVBoxLayout(overview_tab)
        overview_text = QTextEdit()
        overview_text.setReadOnly(True)
        overview_layout.addWidget(overview_text)
        tabs.addTab(overview_tab, "Overview")
        
        # Cast tab
        cast_tab = QWidget()
        cast_layout = QVBoxLayout(cast_tab)
        cast_table = QTableWidget()
        cast_table.setColumnCount(4)
        cast_table.setHorizontalHeaderLabels(["Name", "Category", "Job", "Characters"])
        cast_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        cast_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        cast_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        cast_table.cellDoubleClicked.connect(self.show_person_details)
        cast_layout.addWidget(cast_table)
        tabs.addTab(cast_tab, "Cast & Crew")
        
        # Alternative titles tab
        aka_tab = QWidget()
        aka_layout = QVBoxLayout(aka_tab)
        aka_table = QTableWidget()
        aka_table.setColumnCount(5)
        aka_table.setHorizontalHeaderLabels(["Title", "Region", "Language", "Type", "Original"])
        aka_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        aka_layout.addWidget(aka_table)
        tabs.addTab(aka_tab, "Alternative Titles")
        
        # Episodes tab (only for TV series)
        episodes_tab = QWidget()
        episodes_layout = QVBoxLayout(episodes_tab)
        episodes_table = QTableWidget()
        episodes_table.setColumnCount(5)
        episodes_table.setHorizontalHeaderLabels(["Season", "Episode", "Title", "Year", "Rating"])
        episodes_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        episodes_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        episodes_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        episodes_table.cellDoubleClicked.connect(self.show_episode_details)
        episodes_layout.addWidget(episodes_table)
        tabs.addTab(episodes_tab, "Episodes")
        
        # Add tabs to right panel
        right_layout.addWidget(tabs)
        
        # Add panels to splitter
        splitter.addWidget(left_panel)
        splitter.addWidget(right_panel)
        splitter.setSizes([300, 700])
        
        # Add splitter to main layout
        main_layout.addWidget(splitter)
        
        # Set the central widget
        details_dialog.setCentralWidget(main_widget)
        
        # Store references to UI elements in the dialog for later use
        details_dialog.image_label = image_label
        details_dialog.download_button = download_button
        details_dialog.overview_text = overview_text
        details_dialog.cast_table = cast_table
        details_dialog.aka_table = aka_table
        details_dialog.episodes_table = episodes_table
        details_dialog.tabs = tabs
        
        # Load the title details
        self.load_title_details(tconst, details_dialog)
        
        # Show the dialog
        details_dialog.show()
    
    def load_title_details(self, tconst, dialog):
        """Load and display detailed information about a title"""
        try:
            # Get title details from the database
            title_info = self.db_manager.get_title_details(tconst)
            
            if not title_info:
                QMessageBox.warning(dialog, "Error", f"Title with ID {tconst} not found")
                return
            
            # Set dialog title
            dialog.setWindowTitle(f"{title_info.get('primaryTitle', 'Title Details')}")
            
            # Load image if available
            image_path = self.db_manager.get_image_path(tconst)
            if image_path:
                pixmap = QPixmap(image_path)
                if not pixmap.isNull():
                    pixmap = pixmap.scaled(300, 450, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
                    dialog.image_label.setPixmap(pixmap)
                    dialog.download_button.setText("Image Downloaded")
                    dialog.download_button.setEnabled(False)
            
            # Update overview text
            overview_text = f"<h2>{title_info.get('primaryTitle', '')}</h2>"
            if title_info.get('originalTitle') and title_info.get('originalTitle') != title_info.get('primaryTitle'):
                overview_text += f"<h3>Original Title: {title_info.get('originalTitle', '')}</h3>"
            
            overview_text += f"<p><b>Type:</b> {title_info.get('titleType', '')}</p>"
            
            year_text = str(title_info.get('startYear', ''))
            if title_info.get('endYear'):
                year_text += f" - {title_info.get('endYear')}"
            overview_text += f"<p><b>Year:</b> {year_text}</p>"
            
            if title_info.get('runtimeMinutes'):
                overview_text += f"<p><b>Runtime:</b> {title_info.get('runtimeMinutes')} minutes</p>"
            
            if title_info.get('genres'):
                overview_text += f"<p><b>Genres:</b> {title_info.get('genres')}</p>"
            
            if title_info.get('averageRating') and title_info.get('numVotes'):
                overview_text += f"<p><b>Rating:</b> {title_info.get('averageRating'):.1f}/10 from {title_info.get('numVotes'):,} votes</p>"
            
            if title_info.get('director_names'):
                overview_text += f"<p><b>Director(s):</b> {', '.join(title_info.get('director_names'))}</p>"
            
            if title_info.get('writer_names'):
                overview_text += f"<p><b>Writer(s):</b> {', '.join(title_info.get('writer_names'))}</p>"
            
            # Add a status message while fetching the plot
            overview_text += f"<p><b>Plot Summary:</b></p><p>Loading plot summary...</p>"
            dialog.overview_text.setHtml(overview_text)
            
            # Fetch plot summary in the background
            QTimer.singleShot(100, lambda: self.fetch_and_update_plot(tconst, dialog))
            
            # Update cast table
            dialog.cast_table.setRowCount(0)
            
            if 'cast' in title_info:
                for row, person in enumerate(title_info['cast']):
                    dialog.cast_table.insertRow(row)
                    
                    # Name
                    name_item = QTableWidgetItem(person.get('primaryName', ''))
                    name_item.setData(Qt.ItemDataRole.UserRole, person.get('nconst', ''))
                    dialog.cast_table.setItem(row, 0, name_item)
                    
                    # Category
                    category_item = QTableWidgetItem(person.get('category', ''))
                    dialog.cast_table.setItem(row, 1, category_item)
                    
                    # Job
                    job_item = QTableWidgetItem(person.get('job', ''))
                    dialog.cast_table.setItem(row, 2, job_item)
                    
                    # Characters
                    characters = person.get('characters', '')
                    if characters:
                        # Clean up characters string (remove brackets, quotes, etc.)
                        try:
                            characters = characters.strip('[]"\'')
                        except:
                            pass
                    characters_item = QTableWidgetItem(characters)
                    dialog.cast_table.setItem(row, 3, characters_item)
            
            dialog.cast_table.resizeColumnsToContents()
            dialog.cast_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
            
            # Update alternative titles table
            dialog.aka_table.setRowCount(0)
            
            if 'akas' in title_info:
                for row, aka in enumerate(title_info['akas']):
                    dialog.aka_table.insertRow(row)
                    
                    # Title
                    title_item = QTableWidgetItem(aka.get('title', ''))
                    dialog.aka_table.setItem(row, 0, title_item)
                    
                    # Region
                    region_item = QTableWidgetItem(aka.get('region', ''))
                    dialog.aka_table.setItem(row, 1, region_item)
                    
                    # Language
                    language_item = QTableWidgetItem(aka.get('language', ''))
                    dialog.aka_table.setItem(row, 2, language_item)
                    
                    # Type
                    types_item = QTableWidgetItem(aka.get('types', ''))
                    dialog.aka_table.setItem(row, 3, types_item)
                    
                    # Is Original
                    is_original = "Yes" if aka.get('isOriginalTitle') == 1 else ""
                    original_item = QTableWidgetItem(is_original)
                    dialog.aka_table.setItem(row, 4, original_item)
            
            dialog.aka_table.resizeColumnsToContents()
            dialog.aka_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
            
            # Update episodes table (if applicable)
            dialog.episodes_table.setRowCount(0)
            
            if 'episodes' in title_info:
                for row, episode in enumerate(title_info['episodes']):
                    dialog.episodes_table.insertRow(row)
                    
                    # Season
                    season_item = QTableWidgetItem(str(episode.get('seasonNumber', '')))
                    dialog.episodes_table.setItem(row, 0, season_item)
                    
                    # Episode
                    episode_item = QTableWidgetItem(str(episode.get('episodeNumber', '')))
                    dialog.episodes_table.setItem(row, 1, episode_item)
                    
                    # Title
                    title_text = episode.get('primaryTitle', '')
                    if episode.get('originalTitle') and episode.get('originalTitle') != title_text:
                        title_text += f" ({episode.get('originalTitle')})"
                    title_item = QTableWidgetItem(title_text)
                    title_item.setData(Qt.ItemDataRole.UserRole, episode.get('tconst', ''))
                    dialog.episodes_table.setItem(row, 2, title_item)
                    
                    # Year
                    year_item = QTableWidgetItem(str(episode.get('startYear', '')))
                    dialog.episodes_table.setItem(row, 3, year_item)
                    
                    # Rating
                    rating = episode.get('averageRating')
                    rating_text = f"{rating:.1f}" if rating else ""
                    rating_item = QTableWidgetItem(rating_text)
                    dialog.episodes_table.setItem(row, 4, rating_item)
                
                dialog.episodes_table.resizeColumnsToContents()
                dialog.episodes_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
            
            # Show/hide episodes tab based on whether it's a TV series
            episodes_tab_index = 3  # Assuming it's the fourth tab (0-indexed)
            if title_info.get('titleType') == 'tvSeries':
                dialog.tabs.setTabEnabled(episodes_tab_index, True)
            else:
                dialog.tabs.setTabEnabled(episodes_tab_index, False)
        
        except Exception as e:
            QMessageBox.warning(dialog, "Error", f"Failed to load title details: {str(e)}")
    
    def fetch_and_update_plot(self, tconst, dialog):
        """Fetch plot summary and update the overview text"""
        try:
            # Replace the loading message with the actual plot
            plot_summary = self.db_manager.get_plot_summary(tconst)
            
            # Print debug info
            print(f"Plot summary retrieved: {plot_summary[:100]}..." if plot_summary else "No plot summary retrieved")
            
            # Check if the plot is from the database
            is_from_db = self.db_manager.get_plot_from_db(tconst) is not None
            
            # Make sure we have a valid plot summary
            if not plot_summary:
                plot_summary = "Plot summary not available."
            
            # Get the current overview text as plain text
            current_text = dialog.overview_text.toPlainText()
            
            # Create a completely new HTML content
            # Start with the basic info that's already there, but remove the loading message
            lines = current_text.split('\n')
            new_html = ""
            
            # Find where the Plot Summary section begins
            plot_summary_index = -1
            for i, line in enumerate(lines):
                if line.strip() == "Plot Summary:":
                    plot_summary_index = i
                    break
            
            # Rebuild the HTML content
            if plot_summary_index >= 0:
                # Add all lines before Plot Summary
                for i in range(plot_summary_index + 1):
                    if lines[i].strip() != "Loading plot summary...":
                        new_html += f"<p>{lines[i]}</p>"
                
                # Add the plot summary
                new_html += f"<p>{plot_summary}</p>"
                
                # Add source information
                if is_from_db:
                    new_html += f"<p><i>(Plot loaded from database)</i></p>"
                else:
                    new_html += f"<p><i>(Plot retrieved from TMDB)</i></p>"
                
                # Skip the loading line and add any remaining lines
                loading_found = False
                for i in range(plot_summary_index + 1, len(lines)):
                    if lines[i].strip() == "Loading plot summary...":
                        loading_found = True
                        continue
                    if not loading_found or (loading_found and lines[i].strip() and not lines[i].strip().startswith("Vincent is an")):
                        new_html += f"<p>{lines[i]}</p>"
            else:
                # If we can't find the Plot Summary section, just use the original text
                for line in lines:
                    if line.strip() != "Loading plot summary...":
                        new_html += f"<p>{line}</p>"
                new_html += f"<p><b>Plot Summary:</b></p><p>{plot_summary}</p>"
                if is_from_db:
                    new_html += f"<p><i>(Plot loaded from database)</i></p>"
                else:
                    new_html += f"<p><i>(Plot retrieved from TMDB)</i></p>"
            
            # Set the updated HTML
            dialog.overview_text.setHtml(new_html)
            
            # Add a real save button to the dialog instead of trying to use HTML button
            if not is_from_db and not hasattr(dialog, 'save_plot_button'):
                save_plot_button = QPushButton("Save Plot to Database")
                save_plot_button.clicked.connect(lambda: self.save_plot_to_database(tconst, plot_summary, dialog))
                
                # Find a good place to add the button
                if hasattr(dialog, 'download_button'):
                    parent_layout = dialog.download_button.parent().layout()
                    parent_layout.addWidget(save_plot_button)
                    
                    # Store the button reference
                    dialog.save_plot_button = save_plot_button
            
        except Exception as e:
            # If there's an error, just show an error message
            print(f"Error in fetch_and_update_plot: {str(e)}")
            
            # Get the current overview text as plain text
            current_text = dialog.overview_text.toPlainText()
            
            # Create a completely new HTML content
            lines = current_text.split('\n')
            new_html = ""
            
            # Find where the Plot Summary section begins
            plot_summary_index = -1
            for i, line in enumerate(lines):
                if line.strip() == "Plot Summary:":
                    plot_summary_index = i
                    break
            
            # Rebuild the HTML content
            if plot_summary_index >= 0:
                # Add all lines before Plot Summary
                for i in range(plot_summary_index + 1):
                    if lines[i].strip() != "Loading plot summary...":
                        new_html += f"<p>{lines[i]}</p>"
                
                # Add the error message
                new_html += f"<p>Error loading plot summary: {str(e)}</p>"
                
                # Skip the loading line and add any remaining lines
                loading_found = False
                for i in range(plot_summary_index + 1, len(lines)):
                    if lines[i].strip() == "Loading plot summary...":
                        loading_found = True
                        continue
                    if not loading_found or (loading_found and lines[i].strip()):
                        new_html += f"<p>{lines[i]}</p>"
            else:
                # If we can't find the Plot Summary section, just use the original text
                for line in lines:
                    if line.strip() != "Loading plot summary...":
                        new_html += f"<p>{line}</p>"
                new_html += f"<p><b>Plot Summary:</b></p><p>Error loading plot summary: {str(e)}</p>"
            
            dialog.overview_text.setHtml(new_html)
    
    def save_plot_to_database(self, tconst, plot_summary, dialog):
        """Save the plot summary to the database"""
        if not plot_summary or plot_summary.startswith("Error retrieving plot summary"):
            QMessageBox.warning(dialog, "Save Error", "No valid plot summary to save.")
            return
            
        success = self.db_manager.save_plot_to_db(tconst, plot_summary)
        
        if success:
            QMessageBox.information(dialog, "Plot Saved", 
                                   f"Plot summary for {tconst} has been saved to the database.")
            
            # Get the current overview text as plain text
            current_text = dialog.overview_text.toPlainText()
            
            # Create a completely new HTML content
            lines = current_text.split('\n')
            new_html = ""
            
            # Find where the Plot Summary section begins and where the source info is
            plot_summary_index = -1
            source_info_index = -1
            
            for i, line in enumerate(lines):
                if line.strip() == "Plot Summary:":
                    plot_summary_index = i
                if line.strip() == "(Plot retrieved from TMDB)":
                    source_info_index = i
                    break
            
            # Rebuild the HTML content
            if plot_summary_index >= 0 and source_info_index >= 0:
                # Add all lines before Plot Summary
                for i in range(plot_summary_index + 1):
                    new_html += f"<p>{lines[i]}</p>"
                
                # Add the plot summary
                new_html += f"<p>{plot_summary}</p>"
                
                # Add updated source information
                new_html += f"<p><i>(Plot saved to database)</i></p>"
                
                # Add any remaining lines after the source info
                for i in range(source_info_index + 1, len(lines)):
                    new_html += f"<p>{lines[i]}</p>"
                
                # Set the updated HTML
                dialog.overview_text.setHtml(new_html)
            
            # Hide the save button if it exists
            if hasattr(dialog, 'save_plot_button'):
                dialog.save_plot_button.setVisible(False)
                
        else:
            QMessageBox.warning(dialog, "Save Error", 
                               f"Failed to save plot summary for {tconst} to the database.")
    
    def show_episode_details(self, row, column):
        """Show details for a selected episode"""
        episode_tconst = self.episodes_table.item(row, 2).data(Qt.ItemDataRole.UserRole)
        if episode_tconst:
            self.current_tconst = episode_tconst
            self.show_title_details(0, 0)  # The row and column don't matter here
    
    def show_person_details(self, row, column):
        """Show details for a selected person"""
        nconst = self.cast_table.item(row, 0).data(Qt.ItemDataRole.UserRole)
        self.current_nconst = nconst
        
        # Create a new dialog for the person details
        person_dialog = QMainWindow(self)
        person_dialog.setWindowTitle("Person Details")
        person_dialog.setGeometry(200, 200, 800, 600)
        
        # Main widget and layout
        main_widget = QWidget()
        main_layout = QVBoxLayout(main_widget)
        
        # Create tabs for different types of information
        tabs = QTabWidget()
        
        # Overview tab
        overview_tab = QWidget()
        overview_layout = QVBoxLayout(overview_tab)
        overview_text = QTextEdit()
        overview_text.setReadOnly(True)
        overview_layout.addWidget(overview_text)
        tabs.addTab(overview_tab, "Overview")
        
        # Filmography tab
        filmography_tab = QWidget()
        filmography_layout = QVBoxLayout(filmography_tab)
        filmography_table = QTableWidget()
        filmography_table.setColumnCount(5)
        filmography_table.setHorizontalHeaderLabels(["Title", "Year", "Category", "Job", "Rating"])
        filmography_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        filmography_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        filmography_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        filmography_table.cellDoubleClicked.connect(lambda row, col: self.show_title_from_filmography(row, col, filmography_table))
        filmography_layout.addWidget(filmography_table)
        tabs.addTab(filmography_tab, "Filmography")
        
        # Add tabs to main layout
        main_layout.addWidget(tabs)
        
        # Set the central widget
        person_dialog.setCentralWidget(main_widget)
        
        # Load the person details
        try:
            # Get person details from the database
            person_info = self.db_manager.get_person_details(nconst)
            
            if not person_info:
                QMessageBox.warning(person_dialog, "Error", f"Person with ID {nconst} not found")
                return
            
            # Set dialog title
            person_dialog.setWindowTitle(f"{person_info.get('primaryName', 'Person Details')}")
            
            # Update overview text
            overview_html = f"<h2>{person_info.get('primaryName', '')}</h2>"
            
            birth_year = person_info.get('birthYear', '')
            death_year = person_info.get('deathYear', '')
            
            if birth_year or death_year:
                life_span = f"{birth_year or '?'}"
                if death_year:
                    life_span += f" - {death_year}"
                overview_html += f"<p><b>Years:</b> {life_span}</p>"
            
            if person_info.get('primaryProfession'):
                professions = person_info.get('primaryProfession').replace(',', ', ')
                overview_html += f"<p><b>Professions:</b> {professions}</p>"
            
            # Known for titles
            if 'known_for_titles' in person_info and person_info['known_for_titles']:
                overview_html += "<h3>Known For:</h3><ul>"
                for title in person_info['known_for_titles']:
                    title_text = title.get('primaryTitle', '')
                    year = title.get('startYear', '')
                    rating = title.get('averageRating')
                    
                    known_for_text = f"<li><b>{title_text}</b>"
                    if year:
                        known_for_text += f" ({year})"
                    if rating:
                        known_for_text += f" - Rating: {rating:.1f}"
                    known_for_text += "</li>"
                    
                    overview_html += known_for_text
                overview_html += "</ul>"
            
            overview_text.setHtml(overview_html)
            
            # Update filmography table
            if 'filmography' in person_info:
                for row, title in enumerate(person_info['filmography']):
                    filmography_table.insertRow(row)
                    
                    # Title
                    title_text = title.get('primaryTitle', '')
                    title_item = QTableWidgetItem(title_text)
                    title_item.setData(Qt.ItemDataRole.UserRole, title.get('tconst', ''))
                    filmography_table.setItem(row, 0, title_item)
                    
                    # Year
                    year_item = QTableWidgetItem(str(title.get('startYear', '')))
                    filmography_table.setItem(row, 1, year_item)
                    
                    # Category
                    category_item = QTableWidgetItem(title.get('category', ''))
                    filmography_table.setItem(row, 2, category_item)
                    
                    # Job
                    job_text = title.get('job', '')
                    if not job_text and title.get('characters'):
                        # Clean up characters string
                        characters = title.get('characters', '')
                        try:
                            characters = characters.strip('[]"\'')
                            job_text = f"as {characters}"
                        except:
                            pass
                    job_item = QTableWidgetItem(job_text)
                    filmography_table.setItem(row, 3, job_item)
                    
                    # Rating
                    rating = title.get('averageRating')
                    rating_text = f"{rating:.1f}" if rating else ""
                    rating_item = QTableWidgetItem(rating_text)
                    filmography_table.setItem(row, 4, rating_item)
            
            filmography_table.resizeColumnsToContents()
            filmography_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
            
            # Show the dialog
            person_dialog.show()
        
        except Exception as e:
            QMessageBox.warning(person_dialog, "Error", f"Failed to load person details: {str(e)}")
    
    def show_title_from_filmography(self, row, column, table):
        """Show title details from a filmography table"""
        tconst = table.item(row, 0).data(Qt.ItemDataRole.UserRole)
        if tconst:
            self.current_tconst = tconst
            self.show_title_details(0, 0)  # The row and column don't matter here
    
    def download_image(self, tconst):
        """Download an image for the title from IMDB"""
        # Check if we already have an image
        image_path = self.db_manager.get_image_path(tconst)
        if image_path:
            QMessageBox.information(self, "Image Already Downloaded", 
                                   f"An image for this title already exists at:\n{image_path}")
            return
        
        # Construct IMDB URL
        imdb_url = f"https://www.imdb.com/title/{tconst}/"
        
        # Ask user to confirm
        reply = QMessageBox.question(self, "Download Image", 
                                    f"This will attempt to download the poster image from:\n{imdb_url}\n\nContinue?",
                                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        
        if reply == QMessageBox.StandardButton.No:
            return
        
        # Create a progress dialog
        progress_dialog = QMessageBox(self)
        progress_dialog.setWindowTitle("Downloading Image")
        progress_dialog.setText("Attempting to download image from IMDB...")
        progress_dialog.setStandardButtons(QMessageBox.StandardButton.Cancel)
        progress_dialog.setDefaultButton(QMessageBox.StandardButton.Cancel)
        progress_dialog.show()
        
        # Start a thread to download the image
        save_path = os.path.join(self.image_dir, f"{tconst}.jpg")
        
        # Store the parent dialog that initiated the download
        # Find the parent dialog by looking at the sender's parent
        sender = self.sender()
        parent_dialog = None
        while sender:
            if isinstance(sender, QMainWindow) and sender != self:
                parent_dialog = sender
                break
            sender = sender.parent()
        
        # Create and start the download thread
        self.download_thread = ImageDownloader(tconst, save_path)
        self.download_thread.progress_update.connect(lambda msg: progress_dialog.setText(msg))
        self.download_thread.download_complete.connect(
            lambda success, tconst, msg: self.handle_download_complete(success, tconst, msg, progress_dialog, parent_dialog)
        )
        self.download_thread.start()
        
        # Connect the cancel button
        progress_dialog.buttonClicked.connect(lambda: self.download_thread.cancel())
    
    def handle_download_complete(self, success, tconst, message, dialog, parent_dialog=None):
        """Handle completion of image download"""
        if success:
            dialog.setText(f"Image downloaded successfully to:\n{message}")
            dialog.setStandardButtons(QMessageBox.StandardButton.Ok)
            
            # Update the image in the UI
            pixmap = QPixmap(message)
            if not pixmap.isNull():
                pixmap = pixmap.scaled(300, 450, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
                
                # First try to update using the parent_dialog if provided
                if parent_dialog and hasattr(parent_dialog, 'image_label') and hasattr(parent_dialog, 'download_button'):
                    parent_dialog.image_label.setPixmap(pixmap)
                    parent_dialog.download_button.setText("Image Downloaded")
                    parent_dialog.download_button.setEnabled(False)
                else:
                    # Otherwise, search for the dialog that contains the image_label
                    updated = False
                    for child in self.findChildren(QMainWindow):
                        if hasattr(child, 'image_label') and hasattr(child, 'download_button'):
                            # Check if this dialog is for the current title
                            child.image_label.setPixmap(pixmap)
                            child.download_button.setText("Image Downloaded")
                            child.download_button.setEnabled(False)
                            updated = True
                            break
                    
                    if not updated:
                        # If we couldn't find the dialog, store the image path for later use
                        self.status_label.setText(f"Image downloaded to {message}")
                
                # Also update the results table if this title is in it
                self.update_image_in_results_table(tconst, message)
        else:
            dialog.setText(f"Failed to download image: {message}")
            dialog.setStandardButtons(QMessageBox.StandardButton.Ok)
    
    def update_image_in_results_table(self, tconst, image_path):
        """Update the image in the results table for a specific title"""
        # Find the row with this tconst
        for row in range(self.results_table.rowCount()):
            item = self.results_table.item(row, 0)
            if item and item.data(Qt.ItemDataRole.UserRole) == tconst:
                # Create a small thumbnail
                pixmap = QPixmap(image_path)
                if not pixmap.isNull():
                    pixmap = pixmap.scaled(60, 90, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
                    
                    # Create a label to hold the image
                    image_label = QLabel()
                    image_label.setPixmap(pixmap)
                    image_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
                    
                    # Replace the download button with the image
                    self.results_table.setCellWidget(row, 0, image_label)
                break
    
    def export_to_collection(self):
        """Export titles with descriptions and images to a separate collection database"""
        if not self.db_manager:
            QMessageBox.warning(self, "Database Not Ready", "Please wait for the database to initialize.")
            return
            
        try:
            # Show a progress dialog
            progress_dialog = QProgressDialog("Exporting to collection...", "Cancel", 0, 100, self)
            progress_dialog.setWindowTitle("Export Progress")
            progress_dialog.setWindowModality(Qt.WindowModality.WindowModal)
            progress_dialog.setMinimumDuration(0)
            progress_dialog.setValue(0)
            QApplication.processEvents()
            
            # Create or connect to the collection database
            collection_db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "media_collection.db")
            conn = sqlite3.connect(collection_db_path)
            cursor = conn.cursor()
            
            # Create the media_items table if it doesn't exist
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS media_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT NOT NULL,
                genre TEXT NOT NULL,
                title TEXT NOT NULL,
                year INTEGER NOT NULL,
                description TEXT,
                cover_image TEXT,
                date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')
            conn.commit()
            
            # Get titles with plot summaries from the PLOTS table
            imdb_conn, imdb_cursor = self.db_manager.connect()
            imdb_cursor.execute('''
            SELECT p.tconst, p.plot_summary, tb.primaryTitle, tb.titleType, tb.startYear, tb.genres
            FROM PLOTS p
            JOIN title_basics tb ON p.tconst = tb.tconst
            WHERE p.plot_summary IS NOT NULL AND p.plot_summary != ''
            ''')
            
            titles_with_plots = imdb_cursor.fetchall()
            total_titles = len(titles_with_plots)
            
            if total_titles == 0:
                progress_dialog.close()
                QMessageBox.information(self, "Export Complete", "No titles with plot summaries found to export.")
                return
                
            progress_dialog.setMaximum(total_titles)
            
            # Get existing titles in the collection to avoid duplicates
            cursor.execute("SELECT title, year FROM media_items")
            existing_titles = {(row[0], row[1]) for row in cursor.fetchall()}
            
            # Process each title
            exported_count = 0
            skipped_count = 0
            
            for i, (tconst, plot_summary, title, title_type, year, genres) in enumerate(titles_with_plots):
                if progress_dialog.wasCanceled():
                    break
                    
                # Update progress
                progress_dialog.setValue(i + 1)
                progress_dialog.setLabelText(f"Processing {i+1} of {total_titles}: {title}")
                QApplication.processEvents()
                
                # Skip if already in collection
                if (title, year) in existing_titles:
                    skipped_count += 1
                    continue
                
                # Determine category based on title type
                if title_type == 'movie':
                    category = 'Movie'
                elif title_type in ['tvSeries', 'tvMiniSeries', 'tvSpecial']:
                    category = 'TV Show'
                else:
                    category = 'Other'
                
                # Use all genres, not just the first one
                genre = genres if genres else 'Unknown'
                
                # Set the cover image path using the correct format
                cover_image = f"imdb_images/{tconst}.jpg"
                
                # Insert into collection database
                cursor.execute('''
                INSERT INTO media_items (category, genre, title, year, description, cover_image)
                VALUES (?, ?, ?, ?, ?, ?)
                ''', (category, genre, title, year or 0, plot_summary, cover_image))
                
                exported_count += 1
            
            conn.commit()
            conn.close()
            imdb_conn.close()
            
            progress_dialog.close()
            
            # Show completion message
            QMessageBox.information(
                self, 
                "Export Complete", 
                f"Export completed successfully!\n\n"
                f"Exported: {exported_count} titles\n"
                f"Skipped (already in collection): {skipped_count} titles\n\n"
                f"Collection saved to: {collection_db_path}"
            )
            
        except Exception as e:
            QMessageBox.critical(self, "Export Error", f"An error occurred during export: {str(e)}")
            print(f"Export error: {str(e)}")
            traceback.print_exc()

# Main application entry point
if __name__ == "__main__":
    app = QApplication(sys.argv)
    
    # Create and show the main window immediately
    window = IMDBBrowserApp()
    
    # Disable search button until database is initialized
    window.search_button.setEnabled(False)
    
    # Show the window immediately
    window.show()
    
    # Initialize database in the background after window is shown
    # Use a single-shot timer to ensure the window is fully rendered first
    QTimer.singleShot(100, window.initialize_database)
    
    sys.exit(app.exec())
