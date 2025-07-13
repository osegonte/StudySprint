import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import tempfile
import hashlib
import time
import logging
from contextlib import contextmanager

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.connection = None
        self.cursor = None
        self.has_file_data = False
        self.max_retry_attempts = 3
        self.retry_delay = 1  # seconds
        
    def connect_with_retry(self, max_attempts=None):
        """Connect to PostgreSQL with retry logic"""
        attempts = max_attempts or self.max_retry_attempts
        
        for attempt in range(attempts):
            try:
                if self.connection and not self.connection.closed:
                    return True
                    
                logger.info(f"Connecting to database (attempt {attempt + 1}/{attempts})")
                
                self.connection = psycopg2.connect(
                    dbname=os.getenv('DB_NAME'),
                    user=os.getenv('DB_USER'),
                    password=os.getenv('DB_PASSWORD'),
                    host=os.getenv('DB_HOST'),
                    port=os.getenv('DB_PORT'),
                    cursor_factory=RealDictCursor,
                    connect_timeout=10
                )
                self.cursor = self.connection.cursor()
                
                # Test connection
                self.cursor.execute("SELECT 1")
                self.cursor.fetchone()
                
                self.check_schema()
                logger.info("✅ Database connected successfully")
                return True
                
            except Exception as e:
                logger.error(f"Database connection attempt {attempt + 1} failed: {e}")
                
                if self.connection:
                    try:
                        self.connection.close()
                    except:
                        pass
                    self.connection = None
                    self.cursor = None
                
                if attempt < attempts - 1:
                    delay = self.retry_delay * (2 ** attempt)  # Exponential backoff
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logger.error("❌ All database connection attempts failed")
                    raise ConnectionError(f"Could not connect to database after {attempts} attempts: {e}")
        
        return False
    
    def connect(self):
        """Legacy connect method - calls new retry logic"""
        return self.connect_with_retry()
    
    def check_schema(self):
        """Check which schema version we have"""
        try:
            # Check if file_data column exists
            self.cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns 
                    WHERE table_name = 'pdfs' AND column_name = 'file_data'
                )
            """)
            result = self.cursor.fetchone()
            self.has_file_data = result['exists'] if result else False
            
            schema_type = "Database Storage" if self.has_file_data else "File Storage"
            logger.info(f"📊 Schema detected: {schema_type}")
            
        except Exception as e:
            logger.warning(f"Could not check schema: {e}")
            self.has_file_data = False
    
    def initialize_database(self):
        """Create tables if they don't exist"""
        self.connect()
        
        logger.info("Initializing database tables...")
        
        # Create topics table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS topics (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                description TEXT,
                color VARCHAR(7) DEFAULT '#3498db',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create PDFs table with new schema (database storage)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS pdfs (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                file_name VARCHAR(255) NOT NULL,
                file_data BYTEA,
                file_size BIGINT,
                content_hash VARCHAR(64),
                total_pages INTEGER DEFAULT 0,
                current_page INTEGER DEFAULT 1,
                topic_id INTEGER REFERENCES topics(id) ON DELETE CASCADE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create reading sessions table (for future phases)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS reading_sessions (
                id SERIAL PRIMARY KEY,
                pdf_id INTEGER REFERENCES pdfs(id) ON DELETE CASCADE,
                start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                end_time TIMESTAMP,
                pages_read INTEGER DEFAULT 0,
                time_spent INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create indexes for performance
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_pdfs_topic_id ON pdfs(topic_id)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_pdfs_content_hash ON pdfs(content_hash)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_reading_sessions_pdf_id ON reading_sessions(pdf_id)")
        
        self.connection.commit()
        logger.info("✅ Database tables created successfully")
        
        # Update schema detection
        self.check_schema()
    
    @contextmanager
    def transaction(self):
        """Context manager for database transactions"""
        if not self.connection or self.connection.closed:
            self.connect_with_retry()
        
        try:
            yield self.cursor
            self.connection.commit()
            logger.debug("Transaction committed successfully")
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Transaction rolled back due to error: {e}")
            raise
    
    def disconnect(self):
        """Disconnect from database"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            logger.info("Database disconnected")
        except Exception as e:
            logger.warning(f"Error during disconnect: {e}")
    
    def get_all_topics(self):
        """Get all topics"""
        self.connect()
        self.cursor.execute("SELECT * FROM topics ORDER BY name")
        topics = self.cursor.fetchall()
        logger.info(f"Database: Found {len(topics)} topics")
        return topics
        
    def create_topic(self, name, description="", color="#3498db"):
        """Create a new topic"""
        self.connect()
        try:
            with self.transaction():
                self.cursor.execute("""
                    INSERT INTO topics (name, description, color)
                    VALUES (%s, %s, %s) RETURNING id
                """, (name, description, color))
                topic_id = self.cursor.fetchone()['id']
                logger.info(f"Database: Created topic '{name}' with ID {topic_id}")
                return topic_id
        except Exception as e:
            logger.error(f"Failed to create topic '{name}': {e}")
            raise
        
    def get_pdfs_by_topic(self, topic_id):
        """Get all PDFs for a specific topic"""
        self.connect()
        
        # Always use new schema since we created it in initialize_database
        self.cursor.execute("""
            SELECT id, title, file_name, file_size, total_pages, current_page, 
                   topic_id, created_at, updated_at, 
                   LENGTH(file_data) as actual_size,
                   content_hash
            FROM pdfs 
            WHERE topic_id = %s 
            ORDER BY title
        """, (topic_id,))
            
        pdfs = self.cursor.fetchall()
        logger.info(f"Database: Found {len(pdfs)} PDFs for topic {topic_id}")
        
        # Verify data integrity for each PDF
        for pdf in pdfs:
            size_match = pdf['file_size'] == pdf['actual_size']
            logger.debug(f"  PDF ID {pdf['id']}: {pdf['title']}")
            logger.debug(f"    File: {pdf['file_name']}")
            logger.debug(f"    Stored size: {pdf['file_size']} bytes")
            logger.debug(f"    Data integrity: {'✅ OK' if size_match else '❌ CORRUPTED'}")
            
        return pdfs
    
    def add_pdf(self, title, file_path, topic_id, total_pages=0):
        """Add a new PDF to the database with proper error handling"""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"PDF file not found: {file_path}")
        
        if not file_path.lower().endswith('.pdf'):
            raise ValueError("File must be a PDF")
        
        file_size = os.path.getsize(file_path)
        if file_size > 100 * 1024 * 1024:  # 100MB limit
            raise ValueError("PDF file too large (max 100MB)")
        
        logger.info(f"Database: Adding PDF '{title}' ({file_size} bytes) to topic {topic_id}")
        
        try:
            with self.transaction():
                # Read file and calculate hash
                with open(file_path, 'rb') as f:
                    file_data = f.read()
                
                if len(file_data) != file_size:
                    raise ValueError("File size mismatch during read")
                
                content_hash = hashlib.sha256(file_data).hexdigest()
                file_name = os.path.basename(file_path)
                
                logger.info(f"  File size: {file_size} bytes")
                logger.info(f"  Content hash: {content_hash[:16]}...")
                
                # Check for duplicates
                self.cursor.execute("""
                    SELECT id, title FROM pdfs WHERE content_hash = %s
                """, (content_hash,))
                duplicate = self.cursor.fetchone()
                
                if duplicate:
                    logger.warning(f"  ⚠️  Duplicate detected: Same content as PDF {duplicate['id']} ({duplicate['title']})")
                    return None
                
                # Store PDF in database
                self.cursor.execute("""
                    INSERT INTO pdfs (title, file_name, file_data, file_size, content_hash, total_pages, topic_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
                """, (title, file_name, psycopg2.Binary(file_data), file_size, content_hash, total_pages, topic_id))
                
                pdf_id = self.cursor.fetchone()['id']
                logger.info(f"Database: PDF added with ID {pdf_id}")
                return pdf_id
                
        except Exception as e:
            logger.error(f"Database: Error adding PDF: {e}")
            raise
    
    def get_pdf_data(self, pdf_id):
        """Retrieve PDF data from database with integrity verification"""
        try:
            logger.info(f"Database: Retrieving PDF data for ID {pdf_id}")
            
            self.cursor.execute("""
                SELECT title, file_name, file_data, file_size, content_hash
                FROM pdfs 
                WHERE id = %s
            """, (pdf_id,))
            
            result = self.cursor.fetchone()
            
            if not result:
                logger.warning(f"Database: PDF {pdf_id} not found")
                return None
                
            file_data = bytes(result['file_data'])
            expected_size = result['file_size']
            actual_size = len(file_data)
            
            logger.info(f"  Title: {result['title']}")
            logger.info(f"  Expected size: {expected_size} bytes")
            logger.info(f"  Retrieved size: {actual_size} bytes")
            
            # Verify data integrity
            if actual_size != expected_size:
                logger.error(f"  ❌ Size mismatch!")
                raise ValueError("PDF data corruption detected (size mismatch)")
            
            actual_hash = hashlib.sha256(file_data).hexdigest()
            expected_hash = result['content_hash']
            
            if actual_hash == expected_hash:
                logger.info(f"  ✅ Data integrity verified")
            else:
                logger.error(f"  ❌ Hash mismatch!")
                raise ValueError("PDF data corruption detected (hash mismatch)")
            
            return {
                'title': result['title'],
                'file_name': result['file_name'],
                'data': file_data,
                'size': actual_size
            }
            
        except Exception as e:
            logger.error(f"Database: Error retrieving PDF data: {e}")
            raise
    
    def create_temp_pdf_file(self, pdf_id):
        """Create a temporary file with PDF data for viewing"""
        pdf_data = self.get_pdf_data(pdf_id)
        
        if not pdf_data:
            return None
            
        try:
            # Create a temporary file
            temp_fd, temp_path = tempfile.mkstemp(suffix='.pdf', prefix=f'studysprint_pdf_{pdf_id}_')
            
            # Write PDF data to temporary file
            with os.fdopen(temp_fd, 'wb') as temp_file:
                temp_file.write(pdf_data['data'])
                temp_file.flush()
                os.fsync(temp_file.fileno())  # Force write to disk
                
            logger.info(f"Database: Created temporary PDF file: {temp_path}")
            logger.info(f"  Size: {len(pdf_data['data'])} bytes")
            
            # Verify the temporary file
            if os.path.exists(temp_path) and os.path.getsize(temp_path) == pdf_data['size']:
                logger.info(f"  ✅ Temporary file verified")
                return temp_path
            else:
                logger.error(f"  ❌ Temporary file creation failed")
                try:
                    os.unlink(temp_path)
                except:
                    pass
                return None
                
        except Exception as e:
            logger.error(f"Database: Error creating temporary file: {e}")
            return None
            
    def update_pdf_page(self, pdf_id, current_page):
        """Update the current page for a PDF"""
        self.connect()
        logger.debug(f"Database: Updating PDF {pdf_id} current page to {current_page}")
        
        try:
            with self.transaction():
                self.cursor.execute("""
                    UPDATE pdfs SET current_page = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (current_page, pdf_id))
                logger.debug(f"Database: Page position saved")
        except Exception as e:
            logger.error(f"Failed to update PDF page: {e}")
            raise
        
    def get_pdf_by_id(self, pdf_id):
        """Get a specific PDF by ID (metadata only)"""
        self.connect()
        
        self.cursor.execute("""
            SELECT id, title, file_name, file_size, total_pages, current_page, topic_id, content_hash
            FROM pdfs 
            WHERE id = %s
        """, (pdf_id,))
            
        pdf = self.cursor.fetchone()
        
        if pdf:
            logger.info(f"Database: Found PDF {pdf_id}: {pdf['title']}")
            logger.info(f"  File: {pdf['file_name']}")
            logger.info(f"  Size: {pdf.get('file_size', 0)} bytes")
            logger.info(f"  Pages: {pdf.get('total_pages', 0)}")
            logger.info(f"  Current page: {pdf.get('current_page', 1)}")
        else:
            logger.warning(f"Database: PDF {pdf_id} not found")
            
        return pdf
        
    def cleanup_temp_files(self):
        """Clean up old temporary PDF files"""
        import glob
        import time
        
        temp_dir = tempfile.gettempdir()
        pattern = os.path.join(temp_dir, 'studysprint_pdf_*')
        
        cleaned_count = 0
        for temp_file in glob.glob(pattern):
            try:
                # Remove files older than 1 hour
                if os.path.getmtime(temp_file) < time.time() - 3600:
                    os.unlink(temp_file)
                    cleaned_count += 1
            except:
                pass
                
        if cleaned_count > 0:
            logger.info(f"Database: Cleaned up {cleaned_count} old temporary files")
            
    def get_database_stats(self):
        """Get database storage statistics"""
        self.connect()
        
        try:
            # Database storage stats
            self.cursor.execute("""
                SELECT 
                    COUNT(*) as total_pdfs,
                    COALESCE(SUM(file_size), 0) as total_size,
                    COALESCE(AVG(file_size), 0) as avg_size,
                    COALESCE(MAX(file_size), 0) as max_size
                FROM pdfs
            """)
                
            stats = self.cursor.fetchone()
            
            # Get size by topic
            self.cursor.execute("""
                SELECT 
                    t.name as topic_name,
                    COUNT(p.id) as pdf_count,
                    COALESCE(SUM(p.file_size), 0) as topic_size
                FROM topics t
                LEFT JOIN pdfs p ON t.id = p.topic_id
                GROUP BY t.id, t.name
                ORDER BY topic_size DESC
            """)
                
            topic_stats = self.cursor.fetchall()
            
            return {
                'total_pdfs': stats['total_pdfs'] or 0,
                'total_size': stats['total_size'] or 0,
                'avg_size': stats['avg_size'] or 0,
                'max_size': stats['max_size'] or 0,
                'topics': topic_stats
            }
            
        except Exception as e:
            logger.error(f"Database: Error getting stats: {e}")
            return None
    
    def health_check(self):
        """Check database health and connectivity"""
        try:
            if not self.connection or self.connection.closed:
                self.connect_with_retry()
            
            # Test basic functionality
            self.cursor.execute("SELECT COUNT(*) as count FROM topics")
            result = self.cursor.fetchone()
            
            self.cursor.execute("SELECT COUNT(*) as count FROM pdfs") 
            pdf_count = self.cursor.fetchone()
            
            return {
                'status': 'healthy',
                'topics': result['count'],
                'pdfs': pdf_count['count'],
                'connection': 'active'
            }
            
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return {
                'status': 'unhealthy',
                'error': str(e),
                'connection': 'failed'
            }
    
    def __del__(self):
        """Ensure proper cleanup"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
        except:
            pass
    def delete_topic(self, topic_id):
        """Delete a topic and all its PDFs"""
        self.connect()
        
        try:
            with self.transaction():
                # First check how many PDFs will be deleted
                self.cursor.execute("SELECT COUNT(*) as count FROM pdfs WHERE topic_id = %s", (topic_id,))
                pdf_count = self.cursor.fetchone()['count']
                
                # Get topic name for logging
                self.cursor.execute("SELECT name FROM topics WHERE id = %s", (topic_id,))
                topic_result = self.cursor.fetchone()
                topic_name = topic_result['name'] if topic_result else f"ID {topic_id}"
                
                logger.info(f"Deleting topic '{topic_name}' and {pdf_count} associated PDFs")
                
                # Delete the topic (CASCADE will delete associated PDFs)
                self.cursor.execute("DELETE FROM topics WHERE id = %s", (topic_id,))
                
                if self.cursor.rowcount > 0:
                    logger.info(f"✅ Topic '{topic_name}' deleted successfully")
                    return True
                else:
                    logger.warning(f"Topic {topic_id} not found")
                    return False
                    
        except Exception as e:
            logger.error(f"Failed to delete topic {topic_id}: {e}")
            raise

    def delete_pdf(self, pdf_id):
        """Delete a specific PDF"""
        self.connect()
        
        try:
            with self.transaction():
                # Get PDF info for logging
                self.cursor.execute("SELECT title, file_size FROM pdfs WHERE id = %s", (pdf_id,))
                pdf_result = self.cursor.fetchone()
                
                if not pdf_result:
                    logger.warning(f"PDF {pdf_id} not found")
                    return False
                
                pdf_title = pdf_result['title']
                file_size = pdf_result['file_size']
                size_mb = file_size / (1024 * 1024) if file_size else 0
                
                logger.info(f"Deleting PDF '{pdf_title}' ({size_mb:.1f} MB)")
                
                # Delete the PDF
                self.cursor.execute("DELETE FROM pdfs WHERE id = %s", (pdf_id,))
                
                if self.cursor.rowcount > 0:
                    logger.info(f"✅ PDF '{pdf_title}' deleted successfully")
                    return True
                else:
                    logger.warning(f"PDF {pdf_id} not found")
                    return False
                    
        except Exception as e:
            logger.error(f"Failed to delete PDF {pdf_id}: {e}")
            raise

    def rename_topic(self, topic_id, new_name):
        """Rename a topic"""
        self.connect()
        
        try:
            with self.transaction():
                # Get current name for logging
                self.cursor.execute("SELECT name FROM topics WHERE id = %s", (topic_id,))
                topic_result = self.cursor.fetchone()
                old_name = topic_result['name'] if topic_result else f"ID {topic_id}"
                
                # Update the topic name
                self.cursor.execute("""
                    UPDATE topics 
                    SET name = %s, updated_at = CURRENT_TIMESTAMP 
                    WHERE id = %s
                """, (new_name, topic_id))
                
                if self.cursor.rowcount > 0:
                    logger.info(f"✅ Topic renamed from '{old_name}' to '{new_name}'")
                    return True
                else:
                    logger.warning(f"Topic {topic_id} not found")
                    return False
                    
        except Exception as e:
            logger.error(f"Failed to rename topic {topic_id}: {e}")
            raise
