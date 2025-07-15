import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import tempfile
import hashlib
import time
import logging
from contextlib import contextmanager
from datetime import datetime, date, timedelta

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
        """Create tables if they don't exist - FIXED ORDER FOR PHASE 2"""
        self.connect()
        
        logger.info("Initializing database tables...")
        
        # Create topics table FIRST
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
        
        # Create PDFs table SECOND
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
        
        # Create exercise PDF tables THIRD (before Phase 2 tables that reference them)
        self.create_exercise_tables()
        
        # Create Phase 2 tables FOURTH (now they can reference exercise_pdfs)
        self.create_phase2_tables()
        
        # Create reading sessions table (legacy - for compatibility)
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
        
        # Create indexes for core tables
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
    
    def create_exercise_tables(self):
        """Create tables for exercise PDF linking system - MOVED BEFORE PHASE 2"""
        try:
            logger.info("Creating exercise PDF tables...")
            
            # Exercise PDFs table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS exercise_pdfs (
                    id SERIAL PRIMARY KEY,
                    parent_pdf_id INTEGER REFERENCES pdfs(id) ON DELETE CASCADE,
                    title VARCHAR(255) NOT NULL,
                    file_name VARCHAR(255) NOT NULL,
                    file_data BYTEA NOT NULL,
                    file_size BIGINT NOT NULL,
                    content_hash VARCHAR(64) NOT NULL,
                    total_pages INTEGER DEFAULT 0,
                    current_page INTEGER DEFAULT 1,
                    exercise_type VARCHAR(50) DEFAULT 'general',
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Exercise progress tracking
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS exercise_progress (
                    id SERIAL PRIMARY KEY,
                    exercise_pdf_id INTEGER REFERENCES exercise_pdfs(id) ON DELETE CASCADE,
                    parent_pdf_id INTEGER REFERENCES pdfs(id) ON DELETE CASCADE,
                    page_number INTEGER NOT NULL,
                    completed BOOLEAN DEFAULT FALSE,
                    difficulty_rating INTEGER CHECK (difficulty_rating >= 1 AND difficulty_rating <= 5),
                    time_spent_minutes INTEGER DEFAULT 0,
                    notes TEXT,
                    completed_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes for performance
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_exercise_pdfs_parent_id ON exercise_pdfs(parent_pdf_id)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_exercise_pdfs_content_hash ON exercise_pdfs(content_hash)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_exercise_progress_exercise_id ON exercise_progress(exercise_pdf_id)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_exercise_progress_parent_id ON exercise_progress(parent_pdf_id)")
            
            # Don't commit here - let the main function handle it
            logger.info("✅ Exercise PDF tables created successfully")
            
        except Exception as e:
            logger.error(f"Failed to create exercise tables: {e}")
            raise

    def create_phase2_tables(self):
        """Create Phase 2 timer and analytics tables - FIXED TRANSACTION HANDLING"""
        logger.info("Creating Phase 2 tables...")
        
        try:
            # Sessions table - now exercise_pdfs exists
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    id SERIAL PRIMARY KEY,
                    pdf_id INTEGER REFERENCES pdfs(id) ON DELETE CASCADE,
                    exercise_pdf_id INTEGER REFERENCES exercise_pdfs(id) ON DELETE CASCADE,
                    topic_id INTEGER,
                    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    end_time TIMESTAMP,
                    total_time_seconds INTEGER DEFAULT 0,
                    active_time_seconds INTEGER DEFAULT 0,
                    idle_time_seconds INTEGER DEFAULT 0,
                    pages_visited INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    
                    CONSTRAINT check_pdf_type CHECK (
                        (pdf_id IS NOT NULL AND exercise_pdf_id IS NULL) OR 
                        (pdf_id IS NULL AND exercise_pdf_id IS NOT NULL)
                    )
                )
            """)
            
            # Page times table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS page_times (
                    id SERIAL PRIMARY KEY,
                    session_id INTEGER,
                    pdf_id INTEGER,
                    exercise_pdf_id INTEGER,
                    page_number INTEGER NOT NULL,
                    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    end_time TIMESTAMP,
                    duration_seconds INTEGER DEFAULT 0,
                    interaction_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    
                    CONSTRAINT check_page_pdf_type CHECK (
                        (pdf_id IS NOT NULL AND exercise_pdf_id IS NULL) OR 
                        (pdf_id IS NULL AND exercise_pdf_id IS NOT NULL)
                    )
                )
            """)
            
            # Reading metrics table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS reading_metrics (
                    id SERIAL PRIMARY KEY,
                    pdf_id INTEGER,
                    exercise_pdf_id INTEGER,
                    topic_id INTEGER,
                    user_id VARCHAR(50) DEFAULT 'default_user',
                    pages_per_minute DECIMAL(8,2),
                    average_time_per_page_seconds INTEGER,
                    total_pages_read INTEGER DEFAULT 0,
                    total_time_spent_seconds INTEGER DEFAULT 0,
                    last_calculated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    
                    CONSTRAINT check_metrics_pdf_type CHECK (
                        (pdf_id IS NOT NULL AND exercise_pdf_id IS NULL) OR 
                        (pdf_id IS NULL AND exercise_pdf_id IS NOT NULL)
                    )
                )
            """)
            
            # Study goals table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS study_goals (
                    id SERIAL PRIMARY KEY,
                    topic_id INTEGER,
                    pdf_id INTEGER,
                    goal_type VARCHAR(50) NOT NULL,
                    target_value INTEGER NOT NULL,
                    current_value INTEGER DEFAULT 0,
                    target_date DATE,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes - ALL columns exist now
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_sessions_pdf_id ON sessions(pdf_id)",
                "CREATE INDEX IF NOT EXISTS idx_sessions_exercise_pdf_id ON sessions(exercise_pdf_id)",
                "CREATE INDEX IF NOT EXISTS idx_sessions_start_time ON sessions(start_time)",
                "CREATE INDEX IF NOT EXISTS idx_page_times_session_id ON page_times(session_id)",
                "CREATE INDEX IF NOT EXISTS idx_page_times_pdf_id ON page_times(pdf_id)",
                "CREATE INDEX IF NOT EXISTS idx_page_times_exercise_pdf_id ON page_times(exercise_pdf_id)",
                "CREATE INDEX IF NOT EXISTS idx_reading_metrics_pdf_id ON reading_metrics(pdf_id)",
                "CREATE INDEX IF NOT EXISTS idx_reading_metrics_exercise_pdf_id ON reading_metrics(exercise_pdf_id)",
                "CREATE INDEX IF NOT EXISTS idx_reading_metrics_topic_id ON reading_metrics(topic_id)",
            ]
            
            for index_sql in indexes:
                try:
                    self.cursor.execute(index_sql)
                except Exception as e:
                    logger.warning(f"Could not create index: {index_sql}, Error: {e}")
            
            # Create views
            self.cursor.execute("""
                CREATE OR REPLACE VIEW daily_reading_stats AS
                SELECT 
                    DATE(start_time) as reading_date,
                    COUNT(*) as sessions_count,
                    SUM(total_time_seconds) as total_time_seconds,
                    SUM(pages_visited) as total_pages_read,
                    AVG(total_time_seconds::DECIMAL / NULLIF(pages_visited, 0)) as avg_seconds_per_page
                FROM sessions 
                WHERE end_time IS NOT NULL
                GROUP BY DATE(start_time)
                ORDER BY reading_date DESC
            """)
            
            # Don't commit here - let the main function handle it
            logger.info("✅ Phase 2 tables created successfully")
            
        except Exception as e:
            logger.error(f"Failed to create Phase 2 tables: {e}")
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

    def create_session(self, pdf_id=None, exercise_pdf_id=None, topic_id=None):
        """Create a new reading session"""
        self.connect()
        
        try:
            with self.transaction():
                self.cursor.execute("""
                    INSERT INTO sessions (pdf_id, exercise_pdf_id, topic_id, start_time)
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP) RETURNING id
                """, (pdf_id, exercise_pdf_id, topic_id))
                
                session_id = self.cursor.fetchone()['id']
                logger.info(f"Created session {session_id} for {'exercise' if exercise_pdf_id else 'main'} PDF {pdf_id or exercise_pdf_id}")
                return session_id
                
        except Exception as e:
            logger.error(f"Failed to create session: {e}")
            raise

    def end_session(self, session_id, total_time_seconds, active_time_seconds, idle_time_seconds, pages_visited):
        """End a reading session with statistics"""
        self.connect()
        
        try:
            with self.transaction():
                self.cursor.execute("""
                    UPDATE sessions 
                    SET end_time = CURRENT_TIMESTAMP,
                        total_time_seconds = %s,
                        active_time_seconds = %s,
                        idle_time_seconds = %s,
                        pages_visited = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                    RETURNING pdf_id, exercise_pdf_id, topic_id
                """, (total_time_seconds, active_time_seconds, idle_time_seconds, pages_visited, session_id))
                
                result = self.cursor.fetchone()
                if result:
                    logger.info(f"Ended session {session_id}: {total_time_seconds}s total, {pages_visited} pages")
                    return {
                        'session_id': session_id,
                        'total_time_seconds': total_time_seconds,
                        'active_time_seconds': active_time_seconds,
                        'idle_time_seconds': idle_time_seconds,
                        'pages_visited': pages_visited,
                        'pdf_id': result['pdf_id'],
                        'exercise_pdf_id': result['exercise_pdf_id'],
                        'topic_id': result['topic_id']
                    }
                else:
                    logger.warning(f"Session {session_id} not found")
                    return None
                    
        except Exception as e:
            logger.error(f"Failed to end session {session_id}: {e}")
            raise

    def save_page_time(self, session_id, pdf_id=None, exercise_pdf_id=None, page_number=1, duration_seconds=0):
        """Save time spent on a specific page"""
        self.connect()
        
        try:
            with self.transaction():
                self.cursor.execute("""
                    INSERT INTO page_times (session_id, pdf_id, exercise_pdf_id, page_number, 
                                          duration_seconds, end_time)
                    VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """, (session_id, pdf_id, exercise_pdf_id, page_number, duration_seconds))
                
                logger.debug(f"Saved page {page_number} time: {duration_seconds}s")
                
        except Exception as e:
            logger.error(f"Failed to save page time: {e}")
            raise

    def update_reading_metrics(self, pdf_id=None, exercise_pdf_id=None, topic_id=None, 
                             pages_per_minute=0, average_time_per_page_seconds=0, 
                             pages_read=0, time_spent_seconds=0):
        """Update or create reading speed metrics"""
        self.connect()
        
        try:
            with self.transaction():
                # Check if metrics already exist
                self.cursor.execute("""
                    SELECT id, total_pages_read, total_time_spent_seconds 
                    FROM reading_metrics 
                    WHERE (pdf_id = %s OR pdf_id IS NULL) 
                    AND (exercise_pdf_id = %s OR exercise_pdf_id IS NULL)
                    AND (topic_id = %s OR topic_id IS NULL)
                    AND pdf_id IS NOT DISTINCT FROM %s
                    AND exercise_pdf_id IS NOT DISTINCT FROM %s
                    AND topic_id IS NOT DISTINCT FROM %s
                """, (pdf_id, exercise_pdf_id, topic_id, pdf_id, exercise_pdf_id, topic_id))
                
                existing = self.cursor.fetchone()
                
                if existing:
                    # Update existing metrics
                    new_total_pages = existing['total_pages_read'] + pages_read
                    new_total_time = existing['total_time_spent_seconds'] + time_spent_seconds
                    
                    # Recalculate averages
                    if new_total_pages > 0:
                        new_avg_time_per_page = new_total_time / new_total_pages
                        new_pages_per_minute = new_total_pages / (new_total_time / 60.0) if new_total_time > 0 else 0
                    else:
                        new_avg_time_per_page = average_time_per_page_seconds
                        new_pages_per_minute = pages_per_minute
                    
                    self.cursor.execute("""
                        UPDATE reading_metrics 
                        SET pages_per_minute = %s,
                            average_time_per_page_seconds = %s,
                            total_pages_read = %s,
                            total_time_spent_seconds = %s,
                            last_calculated = CURRENT_TIMESTAMP,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (new_pages_per_minute, new_avg_time_per_page, new_total_pages, 
                          new_total_time, existing['id']))
                else:
                    # Create new metrics
                    self.cursor.execute("""
                        INSERT INTO reading_metrics (pdf_id, exercise_pdf_id, topic_id, pages_per_minute,
                                                   average_time_per_page_seconds, total_pages_read,
                                                   total_time_spent_seconds)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (pdf_id, exercise_pdf_id, topic_id, pages_per_minute, 
                          average_time_per_page_seconds, pages_read, time_spent_seconds))
                
                logger.debug(f"Updated reading metrics for {'exercise' if exercise_pdf_id else 'main'} PDF")
                
        except Exception as e:
            logger.error(f"Failed to update reading metrics: {e}")
            raise

    def get_reading_metrics(self, pdf_id=None, exercise_pdf_id=None, topic_id=None, user_wide=False):
        """Get reading speed metrics"""
        self.connect()
        
        try:
            if user_wide:
                # Get user-wide averages
                self.cursor.execute("""
                    SELECT 
                        AVG(pages_per_minute) as pages_per_minute,
                        AVG(average_time_per_page_seconds) as average_time_per_page_seconds,
                        SUM(total_pages_read) as total_pages_read,
                        SUM(total_time_spent_seconds) as total_time_spent_seconds
                    FROM reading_metrics
                    WHERE total_pages_read > 0
                """)
            else:
                # Get specific metrics
                self.cursor.execute("""
                    SELECT pages_per_minute, average_time_per_page_seconds, 
                           total_pages_read, total_time_spent_seconds, last_calculated
                    FROM reading_metrics 
                    WHERE (pdf_id = %s OR (%s IS NULL AND pdf_id IS NULL))
                    AND (exercise_pdf_id = %s OR (%s IS NULL AND exercise_pdf_id IS NULL))
                    AND (topic_id = %s OR (%s IS NULL AND topic_id IS NULL))
                    ORDER BY last_calculated DESC
                    LIMIT 1
                """, (pdf_id, pdf_id, exercise_pdf_id, exercise_pdf_id, topic_id, topic_id))
            
            result = self.cursor.fetchone()
            return dict(result) if result else None
            
        except Exception as e:
            logger.error(f"Failed to get reading metrics: {e}")
            return None

    def get_daily_reading_stats(self, date):
        """Get reading statistics for a specific date"""
        self.connect()
        
        try:
            self.cursor.execute("""
                SELECT sessions_count, total_time_seconds, total_pages_read, avg_seconds_per_page
                FROM daily_reading_stats
                WHERE reading_date = %s
            """, (date,))
            
            result = self.cursor.fetchone()
            return dict(result) if result else None
            
        except Exception as e:
            logger.error(f"Failed to get daily stats for {date}: {e}")
            return None

    def get_session_history(self, days=7, pdf_id=None, exercise_pdf_id=None):
        """Get recent session history"""
        self.connect()
        
        try:
            base_query = """
                SELECT s.id, s.start_time, s.end_time, s.total_time_seconds, 
                       s.active_time_seconds, s.pages_visited,
                       p.title as pdf_title, e.title as exercise_title,
                       t.name as topic_name
                FROM sessions s
                LEFT JOIN pdfs p ON s.pdf_id = p.id
                LEFT JOIN exercise_pdfs e ON s.exercise_pdf_id = e.id
                LEFT JOIN topics t ON s.topic_id = t.id
                WHERE s.start_time >= CURRENT_DATE - INTERVAL '%s days'
                AND s.end_time IS NOT NULL
            """
            
            params = [days]
            
            if pdf_id:
                base_query += " AND s.pdf_id = %s"
                params.append(pdf_id)
            elif exercise_pdf_id:
                base_query += " AND s.exercise_pdf_id = %s"
                params.append(exercise_pdf_id)
            
            base_query += " ORDER BY s.start_time DESC"
            
            self.cursor.execute(base_query, params)
            results = self.cursor.fetchall()
            
            return [dict(row) for row in results]
            
        except Exception as e:
            logger.error(f"Failed to get session history: {e}")
            return []

    def get_reading_streaks(self, days=30):
        """Get reading streak information"""
        self.connect()
        
        try:
            self.cursor.execute("""
                WITH daily_sessions AS (
                    SELECT DATE(start_time) as session_date,
                           COUNT(*) as sessions_count,
                           SUM(total_time_seconds) as daily_time
                    FROM sessions
                    WHERE start_time >= CURRENT_DATE - INTERVAL '%s days'
                    AND end_time IS NOT NULL
                    GROUP BY DATE(start_time)
                    ORDER BY session_date DESC
                ),
                streak_data AS (
                    SELECT session_date, sessions_count, daily_time,
                           session_date - (ROW_NUMBER() OVER (ORDER BY session_date))::INTEGER as streak_group
                    FROM daily_sessions
                )
                SELECT COUNT(*) as current_streak_days,
                       SUM(sessions_count) as streak_sessions,
                       SUM(daily_time) as streak_total_time,
                       MIN(session_date) as streak_start,
                       MAX(session_date) as streak_end
                FROM streak_data
                WHERE streak_group = (
                    SELECT streak_group FROM streak_data 
                    WHERE session_date = (SELECT MAX(session_date) FROM daily_sessions)
                )
            """, (days,))
            
            result = self.cursor.fetchone()
            return dict(result) if result else None
            
        except Exception as e:
            logger.error(f"Failed to get reading streaks: {e}")
            return None

    def cleanup_old_sessions(self, days=90):
        """Clean up old session data beyond retention period"""
        self.connect()
        
        try:
            with self.transaction():
                # Clean up page times first (foreign key dependency)
                self.cursor.execute("""
                    DELETE FROM page_times 
                    WHERE session_id IN (
                        SELECT id FROM sessions 
                        WHERE start_time < CURRENT_DATE - INTERVAL '%s days'
                    )
                """, (days,))
                
                page_times_deleted = self.cursor.rowcount
                
                # Clean up old sessions
                self.cursor.execute("""
                    DELETE FROM sessions 
                    WHERE start_time < CURRENT_DATE - INTERVAL '%s days'
                """, (days,))
                
                sessions_deleted = self.cursor.rowcount
                
                logger.info(f"Cleaned up {sessions_deleted} old sessions and {page_times_deleted} page time records")
                return sessions_deleted
                
        except Exception as e:
            logger.error(f"Failed to cleanup old sessions: {e}")
            raise

    # All the existing exercise PDF, deletion, and other methods remain exactly the same
    def add_exercise_pdf(self, parent_pdf_id, title, file_path, exercise_type="general", description=""):
        """Add an exercise PDF linked to a parent study PDF"""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Exercise PDF file not found: {file_path}")
        
        if not file_path.lower().endswith('.pdf'):
            raise ValueError("File must be a PDF")
        
        file_size = os.path.getsize(file_path)
        if file_size > 50 * 1024 * 1024:  # 50MB limit for exercise PDFs
            raise ValueError("Exercise PDF file too large (max 50MB)")
        
        logger.info(f"Adding exercise PDF '{title}' ({file_size} bytes) to parent PDF {parent_pdf_id}")
        
        try:
            with self.transaction():
                # Verify parent PDF exists
                self.cursor.execute("SELECT title FROM pdfs WHERE id = %s", (parent_pdf_id,))
                parent = self.cursor.fetchone()
                if not parent:
                    raise ValueError(f"Parent PDF {parent_pdf_id} not found")
                
                # Read file and calculate hash
                with open(file_path, 'rb') as f:
                    file_data = f.read()
                
                if len(file_data) != file_size:
                    raise ValueError("File size mismatch during read")
                
                content_hash = hashlib.sha256(file_data).hexdigest()
                file_name = os.path.basename(file_path)
                
                logger.info(f"  File size: {file_size} bytes")
                logger.info(f"  Content hash: {content_hash[:16]}...")
                logger.info(f"  Exercise type: {exercise_type}")
                
                # Check for duplicates
                self.cursor.execute("""
                    SELECT id, title FROM exercise_pdfs WHERE content_hash = %s
                """, (content_hash,))
                duplicate = self.cursor.fetchone()
                
                if duplicate:
                    logger.warning(f"  ⚠️  Duplicate detected: Same content as exercise PDF {duplicate['id']} ({duplicate['title']})")
                    return None
                
                # Count pages using PyMuPDF
                total_pages = 0
                try:
                    import fitz
                    pdf_doc = fitz.open(file_path)
                    total_pages = len(pdf_doc)
                    pdf_doc.close()
                    logger.info(f"  Pages: {total_pages}")
                except Exception as e:
                    logger.warning(f"Could not count pages: {e}")
                
                # Store exercise PDF in database
                self.cursor.execute("""
                    INSERT INTO exercise_pdfs (parent_pdf_id, title, file_name, file_data, file_size, 
                                             content_hash, total_pages, exercise_type, description)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
                """, (parent_pdf_id, title, file_name, psycopg2.Binary(file_data), 
                      file_size, content_hash, total_pages, exercise_type, description))
                
                exercise_id = self.cursor.fetchone()['id']
                logger.info(f"Exercise PDF added with ID {exercise_id}")
                return exercise_id
                
        except Exception as e:
            logger.error(f"Database: Error adding exercise PDF: {e}")
            raise

    def get_exercise_pdfs_for_parent(self, parent_pdf_id):
        """Get all exercise PDFs linked to a parent PDF"""
        self.connect()
        
        self.cursor.execute("""
            SELECT id, title, file_name, file_size, total_pages, current_page,
                   exercise_type, description, created_at,
                   LENGTH(file_data) as actual_size,
                   content_hash
            FROM exercise_pdfs 
            WHERE parent_pdf_id = %s 
            ORDER BY exercise_type, title
        """, (parent_pdf_id,))
        
        exercises = self.cursor.fetchall()
        logger.info(f"Found {len(exercises)} exercise PDFs for parent PDF {parent_pdf_id}")
        
        return exercises

    def get_exercise_pdf_data(self, exercise_id):
        """Retrieve exercise PDF data from database"""
        try:
            logger.info(f"Retrieving exercise PDF data for ID {exercise_id}")
            
            self.cursor.execute("""
                SELECT title, file_name, file_data, file_size, content_hash, parent_pdf_id
                FROM exercise_pdfs 
                WHERE id = %s
            """, (exercise_id,))
            
            result = self.cursor.fetchone()
            
            if not result:
                logger.warning(f"Exercise PDF {exercise_id} not found")
                return None
                
            file_data = bytes(result['file_data'])
            expected_size = result['file_size']
            actual_size = len(file_data)
            
            # Verify data integrity
            if actual_size != expected_size:
                logger.error(f"Size mismatch for exercise PDF {exercise_id}")
                raise ValueError("Exercise PDF data corruption detected (size mismatch)")
            
            actual_hash = hashlib.sha256(file_data).hexdigest()
            expected_hash = result['content_hash']
            
            if actual_hash != expected_hash:
                logger.error(f"Hash mismatch for exercise PDF {exercise_id}")
                raise ValueError("Exercise PDF data corruption detected (hash mismatch)")
            
            logger.info(f"✅ Exercise PDF {exercise_id} data integrity verified")
            
            return {
                'title': result['title'],
                'file_name': result['file_name'],
                'data': file_data,
                'size': actual_size,
                'parent_pdf_id': result['parent_pdf_id']
            }
            
        except Exception as e:
            logger.error(f"Error retrieving exercise PDF data for {exercise_id}: {e}")
            raise

    def create_temp_exercise_pdf_file(self, exercise_id):
        """Create temporary file with exercise PDF data for viewing"""
        exercise_data = self.get_exercise_pdf_data(exercise_id)
        
        if not exercise_data:
            return None
            
        try:
            # Create a temporary file
            temp_fd, temp_path = tempfile.mkstemp(
                suffix='.pdf', 
                prefix=f'studysprint_exercise_{exercise_id}_'
            )
            
            # Write PDF data to temporary file
            with os.fdopen(temp_fd, 'wb') as temp_file:
                temp_file.write(exercise_data['data'])
                temp_file.flush()
                os.fsync(temp_file.fileno())
                
            # Verify the temporary file
            if os.path.exists(temp_path) and os.path.getsize(temp_path) == exercise_data['size']:
                logger.info(f"✅ Created temporary exercise PDF file: {temp_path}")
                return temp_path
            else:
                logger.error(f"❌ Temporary exercise file creation failed")
                try:
                    os.unlink(temp_path)
                except:
                    pass
                return None
                
        except Exception as e:
            logger.error(f"Error creating temporary exercise file: {e}")
            return None

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

    def delete_exercise_pdf(self, exercise_id):
        """Delete an exercise PDF"""
        self.connect()
        
        try:
            with self.transaction():
                # Get exercise info for logging
                self.cursor.execute("""
                    SELECT title, file_size, parent_pdf_id 
                    FROM exercise_pdfs WHERE id = %s
                """, (exercise_id,))
                exercise_result = self.cursor.fetchone()
                
                if not exercise_result:
                    logger.warning(f"Exercise PDF {exercise_id} not found")
                    return False
                
                exercise_title = exercise_result['title']
                parent_id = exercise_result['parent_pdf_id']
                
                logger.info(f"Deleting exercise PDF '{exercise_title}' (parent: {parent_id})")
                
                # Delete the exercise PDF
                self.cursor.execute("DELETE FROM exercise_pdfs WHERE id = %s", (exercise_id,))
                
                if self.cursor.rowcount > 0:
                    logger.info(f"✅ Exercise PDF '{exercise_title}' deleted successfully")
                    return True
                else:
                    logger.warning(f"Exercise PDF {exercise_id} not found")
                    return False
                    
        except Exception as e:
            logger.error(f"Failed to delete exercise PDF {exercise_id}: {e}")
            raise

    def update_exercise_pdf_page(self, exercise_id, current_page):
        """Update the current page for an exercise PDF"""
        self.connect()
        logger.debug(f"Updating exercise PDF {exercise_id} current page to {current_page}")
        
        try:
            with self.transaction():
                self.cursor.execute("""
                    UPDATE exercise_pdfs 
                    SET current_page = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (current_page, exercise_id))
                logger.debug(f"Exercise PDF page position saved")
        except Exception as e:
            logger.error(f"Failed to update exercise PDF page: {e}")
            raise

    def get_exercise_pdf_by_id(self, exercise_id):
        """Get exercise PDF metadata by ID"""
        self.connect()
        
        self.cursor.execute("""
            SELECT id, parent_pdf_id, title, file_name, file_size, total_pages, 
                   current_page, exercise_type, description, content_hash
            FROM exercise_pdfs 
            WHERE id = %s
        """, (exercise_id,))
            
        exercise = self.cursor.fetchone()
        
        if exercise:
            logger.info(f"Found exercise PDF {exercise_id}: {exercise['title']}")
        else:
            logger.warning(f"Exercise PDF {exercise_id} not found")
            
        return exercise
    
    def __del__(self):
        """Ensure proper cleanup"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
        except:
            pass
# Additional methods for src/database/db_manager.py - Enhanced Timer Support

def get_session_analytics(self, session_id):
    """Get comprehensive analytics for a specific session"""
    self.connect()
    
    try:
        # Get session details
        self.cursor.execute("""
            SELECT s.*, p.title as pdf_title, e.title as exercise_title, t.name as topic_name
            FROM sessions s
            LEFT JOIN pdfs p ON s.pdf_id = p.id
            LEFT JOIN exercise_pdfs e ON s.exercise_pdf_id = e.id
            LEFT JOIN topics t ON s.topic_id = t.id
            WHERE s.id = %s
        """, (session_id,))
        
        session = self.cursor.fetchone()
        if not session:
            return None
        
        # Get page times for this session
        self.cursor.execute("""
            SELECT page_number, duration_seconds, start_time, end_time
            FROM page_times
            WHERE session_id = %s
            ORDER BY start_time
        """, (session_id,))
        
        page_times = self.cursor.fetchall()
        
        # Calculate analytics
        total_pages = len(page_times)
        if total_pages > 0:
            avg_time_per_page = sum(pt['duration_seconds'] for pt in page_times) / total_pages
            fastest_page = min(pt['duration_seconds'] for pt in page_times)
            slowest_page = max(pt['duration_seconds'] for pt in page_times)
        else:
            avg_time_per_page = 0
            fastest_page = 0
            slowest_page = 0
        
        return {
            'session': dict(session),
            'page_times': [dict(pt) for pt in page_times],
            'analytics': {
                'total_pages_timed': total_pages,
                'average_time_per_page': avg_time_per_page,
                'fastest_page_time': fastest_page,
                'slowest_page_time': slowest_page,
                'reading_consistency': self._calculate_reading_consistency(page_times)
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting session analytics: {e}")
        return None

def _calculate_reading_consistency(self, page_times):
    """Calculate reading consistency score based on page time variance"""
    if len(page_times) < 3:
        return 0
    
    times = [pt['duration_seconds'] for pt in page_times]
    mean_time = sum(times) / len(times)
    
    if mean_time == 0:
        return 0
    
    # Calculate coefficient of variation
    variance = sum((t - mean_time) ** 2 for t in times) / len(times)
    std_dev = variance ** 0.5
    cv = std_dev / mean_time
    
    # Convert to consistency score (0-100, lower CV = higher consistency)
    consistency = max(0, 100 - (cv * 50))
    return round(consistency)

def get_reading_streaks(self, days=90):
    """Get detailed reading streak information"""
    self.connect()
    
    try:
        self.cursor.execute("""
            WITH daily_sessions AS (
                SELECT DATE(start_time) as session_date,
                       COUNT(*) as sessions_count,
                       SUM(total_time_seconds) as daily_time,
                       SUM(pages_visited) as daily_pages
                FROM sessions
                WHERE start_time >= CURRENT_DATE - INTERVAL '%s days'
                AND end_time IS NOT NULL
                GROUP BY DATE(start_time)
                ORDER BY session_date DESC
            ),
            streak_analysis AS (
                SELECT session_date, sessions_count, daily_time, daily_pages,
                       session_date - (ROW_NUMBER() OVER (ORDER BY session_date))::INTEGER as streak_group,
                       ROW_NUMBER() OVER (ORDER BY session_date DESC) as days_ago
                FROM daily_sessions
            ),
            current_streak AS (
                SELECT COUNT(*) as current_streak_days,
                       SUM(sessions_count) as streak_sessions,
                       SUM(daily_time) as streak_total_time,
                       SUM(daily_pages) as streak_total_pages,
                       MIN(session_date) as streak_start,
                       MAX(session_date) as streak_end,
                       AVG(daily_time) as avg_daily_time
                FROM streak_analysis
                WHERE streak_group = (
                    SELECT streak_group FROM streak_analysis 
                    WHERE session_date = (SELECT MAX(session_date) FROM daily_sessions)
                )
            ),
            longest_streak AS (
                SELECT streak_group, COUNT(*) as streak_length,
                       SUM(daily_time) as total_time,
                       MIN(session_date) as start_date,
                       MAX(session_date) as end_date
                FROM streak_analysis
                GROUP BY streak_group
                ORDER BY streak_length DESC
                LIMIT 1
            )
            SELECT 
                cs.current_streak_days,
                cs.streak_sessions,
                cs.streak_total_time,
                cs.streak_total_pages,
                cs.streak_start,
                cs.streak_end,
                cs.avg_daily_time,
                ls.streak_length as longest_streak_days,
                ls.total_time as longest_streak_time,
                ls.start_date as longest_streak_start,
                ls.end_date as longest_streak_end
            FROM current_streak cs
            CROSS JOIN longest_streak ls
        """, (days,))
        
        result = self.cursor.fetchone()
        return dict(result) if result else None
        
    except Exception as e:
        logger.error(f"Error getting reading streaks: {e}")
        return None

def get_topic_progress_summary(self, topic_id):
    """Get comprehensive topic progress summary"""
    self.connect()
    
    try:
        # Get topic basic info
        self.cursor.execute("SELECT * FROM topics WHERE id = %s", (topic_id,))
        topic = self.cursor.fetchone()
        
        if not topic:
            return None
        
        # Get PDFs in topic
        pdfs = self.get_pdfs_by_topic(topic_id)
        
        # Calculate progress metrics
        total_pages = sum(pdf.get('total_pages', 0) for pdf in pdfs)
        read_pages = sum(pdf.get('current_page', 1) - 1 for pdf in pdfs)
        progress_percent = (read_pages / total_pages * 100) if total_pages > 0 else 0
        
        # Get session data for this topic
        self.cursor.execute("""
            SELECT COUNT(*) as session_count,
                   SUM(total_time_seconds) as total_study_time,
                   SUM(active_time_seconds) as total_active_time,
                   SUM(pages_visited) as total_pages_visited,
                   AVG(total_time_seconds) as avg_session_length,
                   MAX(start_time) as last_session_date
            FROM sessions
            WHERE topic_id = %s AND end_time IS NOT NULL
        """, (topic_id,))
        
        session_stats = self.cursor.fetchone()
        
        # Get reading metrics for topic
        topic_metrics = self.get_reading_metrics(topic_id=topic_id)
        
        return {
            'topic': dict(topic),
            'pdfs': pdfs,
            'progress': {
                'total_pages': total_pages,
                'pages_read': read_pages,
                'progress_percent': progress_percent,
                'pdfs_completed': len([p for p in pdfs if p.get('current_page', 1) >= p.get('total_pages', 1)]),
                'pdfs_in_progress': len([p for p in pdfs if 1 < p.get('current_page', 1) < p.get('total_pages', 1)]),
                'pdfs_not_started': len([p for p in pdfs if p.get('current_page', 1) <= 1])
            },
            'session_stats': dict(session_stats) if session_stats else {},
            'reading_metrics': topic_metrics or {}
        }
        
    except Exception as e:
        logger.error(f"Error getting topic progress summary: {e}")
        return None

def get_user_reading_patterns(self, days=30):
    """Analyze user reading patterns and habits"""
    self.connect()
    
    try:
        # Get hourly reading patterns
        self.cursor.execute("""
            SELECT EXTRACT(HOUR FROM start_time) as hour,
                   COUNT(*) as session_count,
                   SUM(total_time_seconds) as total_time,
                   AVG(total_time_seconds) as avg_session_length
            FROM sessions
            WHERE start_time >= CURRENT_DATE - INTERVAL '%s days'
            AND end_time IS NOT NULL
            GROUP BY EXTRACT(HOUR FROM start_time)
            ORDER BY hour
        """, (days,))
        
        hourly_patterns = self.cursor.fetchall()
        
        # Get weekly reading patterns
        self.cursor.execute("""
            SELECT EXTRACT(DOW FROM start_time) as day_of_week,
                   COUNT(*) as session_count,
                   SUM(total_time_seconds) as total_time,
                   AVG(total_time_seconds) as avg_session_length
            FROM sessions
            WHERE start_time >= CURRENT_DATE - INTERVAL '%s days'
            AND end_time IS NOT NULL
            GROUP BY EXTRACT(DOW FROM start_time)
            ORDER BY day_of_week
        """, (days,))
        
        weekly_patterns = self.cursor.fetchall()
        
        # Get session length distribution
        self.cursor.execute("""
            SELECT 
                CASE 
                    WHEN total_time_seconds < 600 THEN 'short'  -- < 10 min
                    WHEN total_time_seconds < 1800 THEN 'medium'  -- 10-30 min
                    WHEN total_time_seconds < 3600 THEN 'long'   -- 30-60 min
                    ELSE 'extended'  -- > 60 min
                END as session_type,
                COUNT(*) as count,
                AVG(total_time_seconds) as avg_duration,
                AVG(pages_visited) as avg_pages
            FROM sessions
            WHERE start_time >= CURRENT_DATE - INTERVAL '%s days'
            AND end_time IS NOT NULL
            GROUP BY session_type
        """, (days,))
        
        session_distribution = self.cursor.fetchall()
        
        # Get efficiency patterns
        self.cursor.execute("""
            SELECT 
                CASE 
                    WHEN (active_time_seconds::FLOAT / total_time_seconds) >= 0.8 THEN 'high_efficiency'
                    WHEN (active_time_seconds::FLOAT / total_time_seconds) >= 0.6 THEN 'medium_efficiency'
                    ELSE 'low_efficiency'
                END as efficiency_category,
                COUNT(*) as session_count,
                AVG(active_time_seconds::FLOAT / total_time_seconds) as avg_efficiency
            FROM sessions
            WHERE start_time >= CURRENT_DATE - INTERVAL '%s days'
            AND end_time IS NOT NULL
            AND total_time_seconds > 0
            GROUP BY efficiency_category
        """, (days,))
        
        efficiency_patterns = self.cursor.fetchall()
        
        return {
            'analysis_period_days': days,
            'hourly_patterns': [dict(row) for row in hourly_patterns],
            'weekly_patterns': [dict(row) for row in weekly_patterns],
            'session_distribution': [dict(row) for row in session_distribution],
            'efficiency_patterns': [dict(row) for row in efficiency_patterns],
            'insights': self._generate_reading_insights(hourly_patterns, weekly_patterns, session_distribution)
        }
        
    except Exception as e:
        logger.error(f"Error analyzing reading patterns: {e}")
        return None

def _generate_reading_insights(self, hourly_patterns, weekly_patterns, session_distribution):
    """Generate intelligent insights from reading patterns"""
    insights = []
    
    # Peak reading hours
    if hourly_patterns:
        peak_hour = max(hourly_patterns, key=lambda x: x['total_time'])
        insights.append(f"Your most productive reading hour is {peak_hour['hour']}:00")
    
    # Best reading day
    if weekly_patterns:
        day_names = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
        peak_day = max(weekly_patterns, key=lambda x: x['total_time'])
        day_name = day_names[int(peak_day['day_of_week'])]
        insights.append(f"You read most on {day_name}s")
    
    # Session length preference
    if session_distribution:
        preferred_type = max(session_distribution, key=lambda x: x['count'])
        type_descriptions = {
            'short': 'short sessions (under 10 minutes)',
            'medium': 'medium sessions (10-30 minutes)',
            'long': 'long sessions (30-60 minutes)',
            'extended': 'extended sessions (over 1 hour)'
        }
        insights.append(f"You prefer {type_descriptions.get(preferred_type['session_type'], 'varied')} sessions")
    
    return insights

def get_reading_goals_progress(self, user_id='default_user'):
    """Get progress toward reading goals"""
    self.connect()
    
    try:
        # Get active goals
        self.cursor.execute("""
            SELECT * FROM study_goals
            WHERE is_active = TRUE
            ORDER BY created_at DESC
        """)
        
        goals = self.cursor.fetchall()
        
        goal_progress = []
        
        for goal in goals:
            goal_dict = dict(goal)
            
            # Calculate current progress based on goal type
            if goal['goal_type'] == 'daily_time':
                # Daily time goal
                today_stats = self.get_daily_reading_stats(datetime.now().date())
                current_value = today_stats.get('total_time_seconds', 0) // 60 if today_stats else 0  # Convert to minutes
                
            elif goal['goal_type'] == 'daily_pages':
                # Daily pages goal
                today_stats = self.get_daily_reading_stats(datetime.now().date())
                current_value = today_stats.get('total_pages_read', 0) if today_stats else 0
                
            elif goal['goal_type'] == 'weekly_sessions':
                # Weekly sessions goal
                self.cursor.execute("""
                    SELECT COUNT(*) as session_count
                    FROM sessions
                    WHERE start_time >= DATE_TRUNC('week', CURRENT_DATE)
                    AND end_time IS NOT NULL
                """)
                result = self.cursor.fetchone()
                current_value = result['session_count'] if result else 0
                
            elif goal['goal_type'] == 'topic_completion':
                # Topic completion goal
                if goal['topic_id']:
                    topic_progress = self.get_topic_progress_summary(goal['topic_id'])
                    current_value = topic_progress['progress']['progress_percent'] if topic_progress else 0
                else:
                    current_value = 0
                    
            else:
                current_value = goal['current_value']
            
            # Update current value in database
            self.cursor.execute("""
                UPDATE study_goals 
                SET current_value = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (current_value, goal['id']))
            
            goal_dict['current_value'] = current_value
            goal_dict['progress_percent'] = (current_value / goal['target_value'] * 100) if goal['target_value'] > 0 else 0
            goal_dict['is_completed'] = current_value >= goal['target_value']
            
            goal_progress.append(goal_dict)
        
        self.connection.commit()
        return goal_progress
        
    except Exception as e:
        logger.error(f"Error getting reading goals progress: {e}")
        return []

def create_reading_goal(self, goal_type, target_value, target_date=None, topic_id=None, pdf_id=None):
    """Create a new reading goal"""
    self.connect()
    
    try:
        with self.transaction():
            self.cursor.execute("""
                INSERT INTO study_goals (goal_type, target_value, target_date, topic_id, pdf_id)
                VALUES (%s, %s, %s, %s, %s) RETURNING id
            """, (goal_type, target_value, target_date, topic_id, pdf_id))
            
            goal_id = self.cursor.fetchone()['id']
            logger.info(f"Created reading goal: {goal_type} = {target_value}")
            return goal_id
            
    except Exception as e:
        logger.error(f"Error creating reading goal: {e}")
        raise

def get_productivity_metrics(self, days=7):
    """Calculate comprehensive productivity metrics"""
    self.connect()
    
    try:
        # Get basic metrics
        self.cursor.execute("""
            SELECT 
                COUNT(*) as total_sessions,
                SUM(total_time_seconds) as total_time,
                SUM(active_time_seconds) as total_active_time,
                SUM(pages_visited) as total_pages,
                AVG(total_time_seconds) as avg_session_length,
                AVG(active_time_seconds::FLOAT / NULLIF(total_time_seconds, 0)) as avg_efficiency
            FROM sessions
            WHERE start_time >= CURRENT_DATE - INTERVAL '%s days'
            AND end_time IS NOT NULL
        """, (days,))
        
        basic_metrics = self.cursor.fetchone()
        
        # Get daily breakdown
        self.cursor.execute("""
            SELECT 
                DATE(start_time) as study_date,
                COUNT(*) as sessions,
                SUM(total_time_seconds) as daily_time,
                SUM(active_time_seconds) as daily_active_time,
                SUM(pages_visited) as daily_pages
            FROM sessions
            WHERE start_time >= CURRENT_DATE - INTERVAL '%s days'
            AND end_time IS NOT NULL
            GROUP BY DATE(start_time)
            ORDER BY study_date DESC
        """, (days,))
        
        daily_breakdown = self.cursor.fetchall()
        
        # Calculate derived metrics
        metrics = dict(basic_metrics) if basic_metrics else {}
        
        if metrics.get('total_active_time', 0) > 0 and metrics.get('total_pages', 0) > 0:
            metrics['pages_per_minute'] = metrics['total_pages'] / (metrics['total_active_time'] / 60)
            metrics['avg_time_per_page'] = metrics['total_active_time'] / metrics['total_pages']
        else:
            metrics['pages_per_minute'] = 0
            metrics['avg_time_per_page'] = 0
        
        # Calculate consistency score
        if len(daily_breakdown) >= 3:
            daily_times = [day['daily_time'] for day in daily_breakdown]
            mean_time = sum(daily_times) / len(daily_times)
            if mean_time > 0:
                variance = sum((t - mean_time) ** 2 for t in daily_times) / len(daily_times)
                cv = (variance ** 0.5) / mean_time
                metrics['consistency_score'] = max(0, 100 - (cv * 100))
            else:
                metrics['consistency_score'] = 0
        else:
            metrics['consistency_score'] = 0
        
        return {
            'period_days': days,
            'metrics': metrics,
            'daily_breakdown': [dict(day) for day in daily_breakdown],
            'productivity_rating': self._calculate_productivity_rating(metrics)
        }
        
    except Exception as e:
        logger.error(f"Error calculating productivity metrics: {e}")
        return None

def _calculate_productivity_rating(self, metrics):
    """Calculate overall productivity rating"""
    if not metrics:
        return 'insufficient_data'
    
    # Scoring factors
    efficiency = metrics.get('avg_efficiency', 0) * 100
    consistency = metrics.get('consistency_score', 0)
    daily_time = metrics.get('total_time', 0) / 7 / 60  # Average daily minutes
    reading_speed = metrics.get('pages_per_minute', 0)
    
    # Calculate weighted score
    score = 0
    score += min(30, efficiency * 0.3)  # Up to 30 points for efficiency
    score += min(25, consistency * 0.25)  # Up to 25 points for consistency
    score += min(25, daily_time * 0.4)  # Up to 25 points for time (1 hour = 24 points)
    score += min(20, reading_speed * 10)  # Up to 20 points for speed
    
    if score >= 80:
        return 'excellent'
    elif score >= 60:
        return 'good'
    elif score >= 40:
        return 'fair'
    elif score >= 20:
        return 'poor'
    else:
        return 'very_poor'

def export_session_data(self, start_date=None, end_date=None, format='csv'):
    """Export session data for external analysis"""
    self.connect()
    
    try:
        # Build query with date filters
        query = """
            SELECT 
                s.id as session_id,
                s.start_time,
                s.end_time,
                s.total_time_seconds,
                s.active_time_seconds,
                s.idle_time_seconds,
                s.pages_visited,
                p.title as pdf_title,
                e.title as exercise_title,
                t.name as topic_name,
                CASE WHEN s.exercise_pdf_id IS NOT NULL THEN 'exercise' ELSE 'main' END as document_type
            FROM sessions s
            LEFT JOIN pdfs p ON s.pdf_id = p.id
            LEFT JOIN exercise_pdfs e ON s.exercise_pdf_id = e.id
            LEFT JOIN topics t ON s.topic_id = t.id
            WHERE s.end_time IS NOT NULL
        """
        
        params = []
        if start_date:
            query += " AND s.start_time >= %s"
            params.append(start_date)
        if end_date:
            query += " AND s.end_time <= %s"
            params.append(end_date)
            
        query += " ORDER BY s.start_time DESC"
        
        self.cursor.execute(query, params)
        sessions = self.cursor.fetchall()
        
        if format == 'csv':
            import csv
            import io
            
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=sessions[0].keys() if sessions else [])
            writer.writeheader()
            for session in sessions:
                writer.writerow(dict(session))
            
            return output.getvalue()
            
        elif format == 'json':
            import json
            return json.dumps([dict(session) for session in sessions], default=str, indent=2)
            
        else:
            return [dict(session) for session in sessions]
            
    except Exception as e:
        logger.error(f"Error exporting session data: {e}")
        return None

def get_session_heatmap_data(self, days=30):
    """Get data for session activity heatmap visualization"""
    self.connect()
    
    try:
        self.cursor.execute("""
            SELECT 
                DATE(start_time) as date,
                EXTRACT(HOUR FROM start_time) as hour,
                COUNT(*) as session_count,
                SUM(total_time_seconds) as total_time,
                AVG(active_time_seconds::FLOAT / NULLIF(total_time_seconds, 0)) as avg_efficiency
            FROM sessions
            WHERE start_time >= CURRENT_DATE - INTERVAL '%s days'
            AND end_time IS NOT NULL
            GROUP BY DATE(start_time), EXTRACT(HOUR FROM start_time)
            ORDER BY date DESC, hour
        """, (days,))
        
        heatmap_data = self.cursor.fetchall()
        
        # Format for visualization
        formatted_data = []
        for row in heatmap_data:
            formatted_data.append({
                'date': row['date'].isoformat(),
                'hour': int(row['hour']),
                'session_count': row['session_count'],
                'total_minutes': row['total_time'] // 60,
                'efficiency_percent': round((row['avg_efficiency'] or 0) * 100, 1)
            })
        
        return formatted_data
        
    except Exception as e:
        logger.error(f"Error getting heatmap data: {e}")
        return []

def cleanup_old_sessions(self, days=90):
    """Enhanced cleanup with detailed logging"""
    self.connect()
    
    try:
        with self.transaction():
            # Get count before cleanup
            self.cursor.execute("""
                SELECT COUNT(*) as total_sessions,
                       COUNT(CASE WHEN start_time < CURRENT_DATE - INTERVAL '%s days' THEN 1 END) as old_sessions
                FROM sessions
            """, (days,))
            
            counts = self.cursor.fetchone()
            old_sessions_count = counts['old_sessions']
            
            if old_sessions_count == 0:
                logger.info("No old sessions to clean up")
                return 0
            
            # Clean up page times first (foreign key dependency)
            self.cursor.execute("""
                DELETE FROM page_times 
                WHERE session_id IN (
                    SELECT id FROM sessions 
                    WHERE start_time < CURRENT_DATE - INTERVAL '%s days'
                )
            """, (days,))
            
            page_times_deleted = self.cursor.rowcount
            
            # Clean up old sessions
            self.cursor.execute("""
                DELETE FROM sessions 
                WHERE start_time < CURRENT_DATE - INTERVAL '%s days'
            """, (days,))
            
            sessions_deleted = self.cursor.rowcount
            
            logger.info(f"🧹 Cleaned up {sessions_deleted} old sessions and {page_times_deleted} page time records (retention: {days} days)")
            return sessions_deleted
            
    except Exception as e:
        logger.error(f"Failed to cleanup old sessions: {e}")
        raise

def optimize_database_performance(self):
    """Optimize database performance for timer operations"""
    self.connect()
    
    try:
        # Create additional indexes for timer queries
        performance_indexes = [
            "CREATE INDEX IF NOT EXISTS idx_sessions_start_time_desc ON sessions(start_time DESC)",
            "CREATE INDEX IF NOT EXISTS idx_sessions_end_time_not_null ON sessions(end_time) WHERE end_time IS NOT NULL",
            "CREATE INDEX IF NOT EXISTS idx_sessions_active_composite ON sessions(start_time, end_time, total_time_seconds) WHERE end_time IS NOT NULL",
            "CREATE INDEX IF NOT EXISTS idx_page_times_session_page ON page_times(session_id, page_number)",
            "CREATE INDEX IF NOT EXISTS idx_page_times_duration ON page_times(duration_seconds) WHERE duration_seconds > 0",
            "CREATE INDEX IF NOT EXISTS idx_reading_metrics_composite ON reading_metrics(pdf_id, exercise_pdf_id, topic_id, last_calculated)",
            "CREATE INDEX IF NOT EXISTS idx_study_goals_active ON study_goals(is_active, goal_type) WHERE is_active = TRUE"
        ]
        
        for index_sql in performance_indexes:
            try:
                self.cursor.execute(index_sql)
                logger.debug(f"Created index: {index_sql.split('idx_')[1].split(' ')[0]}")
            except Exception as e:
                logger.warning(f"Could not create index: {index_sql}, Error: {e}")
        
        # Update table statistics
        self.cursor.execute("ANALYZE sessions, page_times, reading_metrics")
        
        self.connection.commit()
        logger.info("✅ Database performance optimization completed")
        
    except Exception as e:
        logger.error(f"Error optimizing database performance: {e}")
        raise

def get_database_health_report(self):
    """Generate comprehensive database health report for timer system"""
    self.connect()
    
    try:
        health_report = {}
        
        # Basic table counts
        tables = ['sessions', 'page_times', 'reading_metrics', 'study_goals']
        for table in tables:
            self.cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
            result = self.cursor.fetchone()
            health_report[f'{table}_count'] = result['count']
        
        # Session data integrity
        self.cursor.execute("""
            SELECT 
                COUNT(*) as total_sessions,
                COUNT(CASE WHEN end_time IS NULL THEN 1 END) as incomplete_sessions,
                COUNT(CASE WHEN total_time_seconds <= 0 THEN 1 END) as zero_time_sessions,
                COUNT(CASE WHEN active_time_seconds > total_time_seconds THEN 1 END) as invalid_time_sessions
            FROM sessions
        """)
        
        session_health = self.cursor.fetchone()
        health_report['session_integrity'] = dict(session_health)
        
        # Database size information
        self.cursor.execute("""
            SELECT 
                pg_size_pretty(pg_total_relation_size('sessions')) as sessions_size,
                pg_size_pretty(pg_total_relation_size('page_times')) as page_times_size,
                pg_size_pretty(pg_total_relation_size('reading_metrics')) as metrics_size
        """)
        
        size_info = self.cursor.fetchone()
        health_report['table_sizes'] = dict(size_info)
        
        # Performance metrics
        recent_cutoff = datetime.now() - timedelta(days=30)
        self.cursor.execute("""
            SELECT 
                AVG(total_time_seconds) as avg_session_duration,
                AVG(pages_visited) as avg_pages_per_session,
                COUNT(DISTINCT DATE(start_time)) as active_days
            FROM sessions
            WHERE start_time >= %s AND end_time IS NOT NULL
        """, (recent_cutoff,))
        
        performance_metrics = self.cursor.fetchone()
        health_report['performance_metrics'] = dict(performance_metrics) if performance_metrics else {}
        
        # Calculate overall health score
        health_score = self._calculate_database_health_score(health_report)
        health_report['overall_health_score'] = health_score
        health_report['health_status'] = self._get_health_status(health_score)
        
        return health_report
        
    except Exception as e:
        logger.error(f"Error generating database health report: {e}")
        return {'error': str(e), 'health_status': 'error'}

def _calculate_database_health_score(self, report):
    """Calculate overall database health score (0-100)"""
    score = 100
    
    # Deduct points for data integrity issues
    session_integrity = report.get('session_integrity', {})
    total_sessions = session_integrity.get('total_sessions', 1)
    
    if total_sessions > 0:
        incomplete_ratio = session_integrity.get('incomplete_sessions', 0) / total_sessions
        invalid_time_ratio = session_integrity.get('invalid_time_sessions', 0) / total_sessions
        
        score -= incomplete_ratio * 30  # Up to 30 points for incomplete sessions
        score -= invalid_time_ratio * 40  # Up to 40 points for invalid time data
    
    # Deduct points for low activity
    performance = report.get('performance_metrics', {})
    active_days = performance.get('active_days', 0)
    if active_days < 5:  # Less than 5 active days in last 30
        score -= (5 - active_days) * 5  # Up to 25 points
    
    return max(0, min(100, round(score)))

def _get_health_status(self, score):
    """Get health status description from score"""
    if score >= 90:
        return 'excellent'
    elif score >= 75:
        return 'good'
    elif score >= 50:
        return 'fair'
    elif score >= 25:
        return 'poor'
    else:
        return 'critical'
def create_goals_tables(self):
        """Create goals system tables if they don't exist"""
        try:
            logger.info("Creating goals system tables...")
            
            # Goals table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS goals (
                    id SERIAL PRIMARY KEY,
                    topic_id INTEGER REFERENCES topics(id) ON DELETE CASCADE,
                    target_type TEXT CHECK (target_type IN ('finish_by_date', 'daily_time', 'daily_pages')),
                    target_value INTEGER NOT NULL DEFAULT 0,
                    deadline DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT TRUE,
                    is_completed BOOLEAN DEFAULT FALSE,
                    completion_date TIMESTAMP,
                    
                    CONSTRAINT valid_deadline CHECK (
                        (target_type = 'finish_by_date' AND deadline IS NOT NULL) OR
                        (target_type != 'finish_by_date')
                    ),
                    CONSTRAINT valid_target_value CHECK (
                        (target_type = 'finish_by_date' AND target_value >= 0) OR
                        (target_type != 'finish_by_date' AND target_value > 0)
                    )
                )
            """)
            
            # Goal progress table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS goal_progress (
                    id SERIAL PRIMARY KEY,
                    goal_id INTEGER REFERENCES goals(id) ON DELETE CASCADE,
                    date DATE NOT NULL,
                    pages_read INTEGER DEFAULT 0,
                    time_spent_minutes INTEGER DEFAULT 0,
                    sessions_count INTEGER DEFAULT 0,
                    target_met BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    
                    UNIQUE (goal_id, date),
                    
                    CONSTRAINT non_negative_pages CHECK (pages_read >= 0),
                    CONSTRAINT non_negative_time CHECK (time_spent_minutes >= 0),
                    CONSTRAINT non_negative_sessions CHECK (sessions_count >= 0)
                )
            """)
            
            # Goal adjustments table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS goal_adjustments (
                    id SERIAL PRIMARY KEY,
                    goal_id INTEGER REFERENCES goals(id) ON DELETE CASCADE,
                    adjustment_date DATE NOT NULL,
                    old_daily_target INTEGER,
                    new_daily_target INTEGER,
                    reason TEXT,
                    pages_behind INTEGER DEFAULT 0,
                    days_remaining INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create basic indexes
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_goals_topic_id ON goals(topic_id)",
                "CREATE INDEX IF NOT EXISTS idx_goals_active ON goals(is_active) WHERE is_active = TRUE",
                "CREATE INDEX IF NOT EXISTS idx_goal_progress_goal_id ON goal_progress(goal_id)",
                "CREATE INDEX IF NOT EXISTS idx_goal_progress_date ON goal_progress(date)"
            ]
            
            for index_sql in indexes:
                try:
                    self.cursor.execute(index_sql)
                except Exception as e:
                    logger.warning(f"Could not create index: {e}")
            
            self.connection.commit()
            logger.info("✅ Goals system tables created successfully")
        
        except Exception as e:
            logger.error(f"Error creating goals tables: {e}")
            raise

def update_goal_progress_after_session(self, topic_id, pages_read, time_spent_minutes, session_date=None):
    """Update goal progress after a study session"""
    try:
        if session_date is None:
            session_date = datetime.now().date()
        
        # Get all active goals for this topic
        self.cursor.execute("""
            SELECT id, target_type, target_value 
            FROM goals 
            WHERE topic_id = %s AND is_active = TRUE AND is_completed = FALSE
        """, (topic_id,))
        
        goals = self.cursor.fetchall()
        
        for goal in goals:
            goal_id = goal['id']
            target_type = goal['target_type']
            target_value = goal['target_value']
            
            # Insert or update today's progress
            self.cursor.execute("""
                INSERT INTO goal_progress (goal_id, date, pages_read, time_spent_minutes, sessions_count)
                VALUES (%s, %s, %s, %s, 1)
                ON CONFLICT (goal_id, date) 
                DO UPDATE SET
                    pages_read = goal_progress.pages_read + EXCLUDED.pages_read,
                    time_spent_minutes = goal_progress.time_spent_minutes + EXCLUDED.time_spent_minutes,
                    sessions_count = goal_progress.sessions_count + EXCLUDED.sessions_count,
                    updated_at = CURRENT_TIMESTAMP
            """, (goal_id, session_date, pages_read, time_spent_minutes))
            
            # Update target_met status
            self.cursor.execute("""
                UPDATE goal_progress SET
                    target_met = CASE 
                        WHEN %s = 'daily_pages' THEN pages_read >= %s
                        WHEN %s = 'daily_time' THEN time_spent_minutes >= %s
                        ELSE target_met
                    END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE goal_id = %s AND date = %s
            """, (target_type, target_value, target_type, target_value, goal_id, session_date))
        
        self.connection.commit()
        logger.info(f"Updated goal progress for topic {topic_id}: {pages_read} pages, {time_spent_minutes}m")
        
    except Exception as e:
        logger.error(f"Error updating goal progress: {e}")
        raise

def get_active_goals(self, topic_id=None):
    """Get all active goals, optionally filtered by topic"""
    try:
        base_query = """
            SELECT g.*, t.name as topic_name,
                   COALESCE(SUM(gp.pages_read), 0) as total_pages_read,
                   COALESCE(SUM(gp.time_spent_minutes), 0) as total_time_spent,
                   COALESCE(COUNT(gp.date), 0) as days_tracked
            FROM goals g
            LEFT JOIN topics t ON g.topic_id = t.id
            LEFT JOIN goal_progress gp ON g.id = gp.goal_id
            WHERE g.is_active = TRUE AND g.is_completed = FALSE
        """
        
        params = []
        if topic_id:
            base_query += " AND g.topic_id = %s"
            params.append(topic_id)
        
        base_query += " GROUP BY g.id, t.name ORDER BY g.created_at DESC"
        
        self.cursor.execute(base_query, params)
        return [dict(row) for row in self.cursor.fetchall()]
        
    except Exception as e:
        logger.error(f"Error getting active goals: {e}")
        return []

def get_today_goal_progress(self, topic_id=None):
    """Get today's goal progress"""
    try:
        base_query = """
            SELECT * FROM daily_goal_status
        """
        
        params = []
        if topic_id:
            base_query += " WHERE goal_id IN (SELECT id FROM goals WHERE topic_id = %s)"
            params.append(topic_id)
        
        self.cursor.execute(base_query, params)
        return [dict(row) for row in self.cursor.fetchall()]
        
    except Exception as e:
        logger.error(f"Error getting today's goal progress: {e}")
        return []

def get_goal_analytics(self, goal_id, days=30):
    """Get comprehensive analytics for a specific goal"""
    try:
        # Get goal progress over time
        self.cursor.execute("""
            SELECT date, pages_read, time_spent_minutes, target_met, sessions_count
            FROM goal_progress
            WHERE goal_id = %s AND date >= CURRENT_DATE - INTERVAL '%s days'
            ORDER BY date DESC
        """, (goal_id, days))
        
        progress_data = [dict(row) for row in self.cursor.fetchall()]
        
        # Get goal adjustments
        self.cursor.execute("""
            SELECT adjustment_date, old_daily_target, new_daily_target, 
                   reason, pages_behind, days_remaining
            FROM goal_adjustments
            WHERE goal_id = %s
            ORDER BY adjustment_date DESC
        """, (goal_id,))
        
        adjustments = [dict(row) for row in self.cursor.fetchall()]
        
        return {
            'goal_id': goal_id,
            'progress_data': progress_data,
            'adjustments': adjustments
        }
        
    except Exception as e:
        logger.error(f"Error getting goal analytics: {e}")
        return {}

def calculate_pages_behind_schedule(self, goal_id):
    """Calculate how many pages behind schedule for deadline goals"""
    try:
        # Get goal details
        self.cursor.execute("""
            SELECT g.*, t.name as topic_name
            FROM goals g
            LEFT JOIN topics t ON g.topic_id = t.id
            WHERE g.id = %s AND g.target_type = 'finish_by_date'
        """, (goal_id,))
        
        goal = self.cursor.fetchone()
        if not goal:
            return 0
        
        # Calculate days elapsed since goal creation
        days_elapsed = (datetime.now().date() - goal['created_at'].date()).days + 1
        
        # Get total pages needed for topic
        self.cursor.execute("""
            SELECT COALESCE(SUM(total_pages - GREATEST(current_page - 1, 0)), 0) as total_pages_needed
            FROM pdfs 
            WHERE topic_id = %s
        """, (goal['topic_id'],))
        
        total_pages_needed = self.cursor.fetchone()['total_pages_needed']
        
        # Get actual pages read
        self.cursor.execute("""
            SELECT COALESCE(SUM(pages_read), 0) as actual_pages
            FROM goal_progress
            WHERE goal_id = %s
        """, (goal_id,))
        
        actual_pages = self.cursor.fetchone()['actual_pages']
        
        # Calculate daily target and expected progress
        total_days = (goal['deadline'] - goal['created_at'].date()).days + 1
        daily_target = total_pages_needed / total_days if total_days > 0 else 0
        expected_progress = daily_target * days_elapsed
        
        return max(0, int(expected_progress - actual_pages))
        
    except Exception as e:
        logger.error(f"Error calculating pages behind: {e}")
        return 0

def create_goal(self, topic_id, target_type, target_value, deadline=None):
    """Create a new study goal"""
    try:
        with self.transaction():
            self.cursor.execute("""
                INSERT INTO goals (topic_id, target_type, target_value, deadline)
                VALUES (%s, %s, %s, %s) RETURNING id
            """, (topic_id, target_type, target_value, deadline))
            
            goal_id = self.cursor.fetchone()['id']
            logger.info(f"Created {target_type} goal for topic {topic_id}: {target_value}")
            return goal_id
            
    except Exception as e:
        logger.error(f"Error creating goal: {e}")
        return None

def update_goal(self, goal_id, **kwargs):
    """Update goal properties"""
    try:
        if not kwargs:
            return False
        
        # Build update query
        set_clauses = []
        params = []
        
        for key, value in kwargs.items():
            if key in ['target_value', 'deadline', 'is_active', 'is_completed']:
                set_clauses.append(f"{key} = %s")
                params.append(value)
        
        if not set_clauses:
            return False
        
        set_clauses.append("updated_at = CURRENT_TIMESTAMP")
        params.append(goal_id)
        
        with self.transaction():
            query = f"UPDATE goals SET {', '.join(set_clauses)} WHERE id = %s"
            self.cursor.execute(query, params)
            
            return self.cursor.rowcount > 0
            
    except Exception as e:
        logger.error(f"Error updating goal {goal_id}: {e}")
        return False

def deactivate_goal(self, goal_id):
    """Deactivate a goal"""
    try:
        return self.update_goal(goal_id, is_active=False)
    except Exception as e:
        logger.error(f"Error deactivating goal {goal_id}: {e}")
        return False

def complete_goal(self, goal_id):
    """Mark a goal as completed"""
    try:
        return self.update_goal(goal_id, is_completed=True, completion_date=datetime.now())
    except Exception as e:
        logger.error(f"Error completing goal {goal_id}: {e}")
        return False

def delete_goal(self, goal_id):
    """Delete a goal and all its progress data"""
    try:
        with self.transaction():
            # Delete progress data first (foreign key dependency)
            self.cursor.execute("DELETE FROM goal_progress WHERE goal_id = %s", (goal_id,))
            progress_deleted = self.cursor.rowcount
            
            # Delete adjustments
            self.cursor.execute("DELETE FROM goal_adjustments WHERE goal_id = %s", (goal_id,))
            adjustments_deleted = self.cursor.rowcount
            
            # Delete the goal
            self.cursor.execute("DELETE FROM goals WHERE id = %s", (goal_id,))
            goal_deleted = self.cursor.rowcount > 0
            
            if goal_deleted:
                logger.info(f"Deleted goal {goal_id} with {progress_deleted} progress records and {adjustments_deleted} adjustments")
                return True
            else:
                logger.warning(f"Goal {goal_id} not found for deletion")
                return False
                
    except Exception as e:
        logger.error(f"Error deleting goal {goal_id}: {e}")
        raise

def get_goal_summary_stats(self):
    """Get summary statistics for all goals"""
    try:
        self.cursor.execute("""
            SELECT 
                COUNT(*) as total_goals,
                COUNT(CASE WHEN is_active = TRUE AND is_completed = FALSE THEN 1 END) as active_goals,
                COUNT(CASE WHEN is_completed = TRUE THEN 1 END) as completed_goals,
                COUNT(CASE WHEN target_type = 'finish_by_date' THEN 1 END) as deadline_goals,
                COUNT(CASE WHEN target_type = 'daily_time' THEN 1 END) as daily_time_goals,
                COUNT(CASE WHEN target_type = 'daily_pages' THEN 1 END) as daily_page_goals
            FROM goals
        """)
        
        stats = self.cursor.fetchone()
        
        # Get today's completion rate
        self.cursor.execute("""
            SELECT 
                COUNT(*) as daily_goals_today,
                COUNT(CASE WHEN target_met = TRUE THEN 1 END) as completed_today
            FROM goal_progress gp
            JOIN goals g ON gp.goal_id = g.id
            WHERE gp.date = CURRENT_DATE 
            AND g.target_type IN ('daily_time', 'daily_pages')
            AND g.is_active = TRUE
        """)
        
        today_stats = self.cursor.fetchone()
        
        return {
            **dict(stats),
            **dict(today_stats),
            'completion_rate_today': (
                today_stats['completed_today'] / today_stats['daily_goals_today'] * 100
                if today_stats['daily_goals_today'] > 0 else 0
            )
        }
        
    except Exception as e:
        logger.error(f"Error getting goal summary stats: {e}")
        return {}

def cleanup_old_goal_data(self, days=90):
    """Clean up old goal progress data"""
    try:
        with self.transaction():
            # Clean up old progress records
            self.cursor.execute("""
                DELETE FROM goal_progress 
                WHERE date < CURRENT_DATE - INTERVAL '%s days'
                AND goal_id IN (
                    SELECT id FROM goals WHERE is_completed = TRUE OR is_active = FALSE
                )
            """, (days,))
            
            progress_deleted = self.cursor.rowcount
            
            # Clean up old adjustments
            self.cursor.execute("""
                DELETE FROM goal_adjustments 
                WHERE adjustment_date < CURRENT_DATE - INTERVAL '%s days'
                AND goal_id IN (
                    SELECT id FROM goals WHERE is_completed = TRUE OR is_active = FALSE
                )
            """, (days,))
            
            adjustments_deleted = self.cursor.rowcount
            
            logger.info(f"Cleaned up {progress_deleted} old progress records and {adjustments_deleted} old adjustments")
            return progress_deleted + adjustments_deleted
            
    except Exception as e:
        logger.error(f"Error cleaning up old goal data: {e}")
        raise

def get_goals_health_report(self):
    """Generate goals system health report"""
    try:
        health_report = {}
        
        # Basic counts
        stats = self.get_goal_summary_stats()
        health_report['goal_counts'] = stats
        
        # Data integrity checks
        self.cursor.execute("""
            SELECT 
                COUNT(*) as total_progress_records,
                COUNT(CASE WHEN pages_read < 0 THEN 1 END) as negative_pages,
                COUNT(CASE WHEN time_spent_minutes < 0 THEN 1 END) as negative_time,
                COUNT(CASE WHEN sessions_count < 0 THEN 1 END) as negative_sessions
            FROM goal_progress
        """)
        
        integrity_stats = self.cursor.fetchone()
        health_report['data_integrity'] = dict(integrity_stats)
        
        # Orphaned records check
        self.cursor.execute("""
            SELECT COUNT(*) as orphaned_progress
            FROM goal_progress gp
            LEFT JOIN goals g ON gp.goal_id = g.id
            WHERE g.id IS NULL
        """)
        
        orphaned = self.cursor.fetchone()['orphaned_progress']
        health_report['orphaned_records'] = orphaned
        
        # Performance metrics
        self.cursor.execute("""
            SELECT 
                AVG(EXTRACT(EPOCH FROM (updated_at - created_at))) as avg_goal_lifetime_days,
                COUNT(CASE WHEN deadline < CURRENT_DATE AND is_completed = FALSE THEN 1 END) as overdue_goals
            FROM goals
            WHERE target_type = 'finish_by_date'
        """)
        
        performance = self.cursor.fetchone()
        health_report['performance_metrics'] = dict(performance)
        
        # Calculate overall health score
        health_score = self._calculate_goals_health_score(health_report)
        health_report['overall_health_score'] = health_score
        health_report['health_status'] = self._get_goals_health_status(health_score)
        
        return health_report
        
    except Exception as e:
        logger.error(f"Error generating goals health report: {e}")
        return {'error': str(e), 'health_status': 'error'}

def _calculate_goals_health_score(self, report):
    """Calculate overall goals system health score (0-100)"""
    score = 100
    
    # Deduct points for data integrity issues
    integrity = report.get('data_integrity', {})
    total_records = integrity.get('total_progress_records', 1)
    
    if total_records > 0:
        negative_ratio = (
            integrity.get('negative_pages', 0) + 
            integrity.get('negative_time', 0) + 
            integrity.get('negative_sessions', 0)
        ) / (total_records * 3)  # 3 fields to check
        
        score -= negative_ratio * 30  # Up to 30 points for data integrity
    
    # Deduct points for orphaned records
    orphaned = report.get('orphaned_records', 0)
    if orphaned > 0:
        score -= min(20, orphaned * 2)  # Up to 20 points for orphaned records
    
    # Deduct points for overdue goals
    performance = report.get('performance_metrics', {})
    overdue = performance.get('overdue_goals', 0)
    if overdue > 0:
        score -= min(25, overdue * 5)  # Up to 25 points for overdue goals
    
    return max(0, min(100, round(score)))

def _get_goals_health_status(self, score):
    """Get health status description from score"""
    if score >= 90:
        return 'excellent'
    elif score >= 75:
        return 'good'
    elif score >= 50:
        return 'fair'
    elif score >= 25:
        return 'poor'
    else:
        return 'critical'

def initialize_optimized_system(self):
    """Initialize the optimized timer and goals system tables"""
    self.connect()
    
    logger.info("Initializing optimized timer and goals system...")
    
    # Ensure core tables exist first
    self.initialize_database()
    
    # Create goals system tables
    self.create_goals_tables()
    
    # Create timer system tables  
    self.create_timer_tables()
    
    logger.info("✅ Optimized system initialized successfully")

def create_timer_tables(self):
    """Create optimized timer system tables"""
    try:
        logger.info("Creating optimized timer tables...")
        
        # Sessions table (already exists but ensure it's optimized)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                id SERIAL PRIMARY KEY,
                pdf_id INTEGER REFERENCES pdfs(id) ON DELETE CASCADE,
                exercise_pdf_id INTEGER REFERENCES exercise_pdfs(id) ON DELETE CASCADE,
                topic_id INTEGER,
                start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                end_time TIMESTAMP,
                total_time_seconds INTEGER DEFAULT 0,
                active_time_seconds INTEGER DEFAULT 0,
                idle_time_seconds INTEGER DEFAULT 0,
                pages_visited INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                CONSTRAINT check_pdf_type CHECK (
                    (pdf_id IS NOT NULL AND exercise_pdf_id IS NULL) OR 
                    (pdf_id IS NULL AND exercise_pdf_id IS NOT NULL)
                )
            )
        """)
        
        # Page times table (optimized)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS page_times (
                id SERIAL PRIMARY KEY,
                session_id INTEGER REFERENCES sessions(id) ON DELETE CASCADE,
                pdf_id INTEGER,
                exercise_pdf_id INTEGER,
                page_number INTEGER NOT NULL,
                duration_seconds INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                CONSTRAINT check_page_pdf_type CHECK (
                    (pdf_id IS NOT NULL AND exercise_pdf_id IS NULL) OR 
                    (pdf_id IS NULL AND exercise_pdf_id IS NOT NULL)
                )
            )
        """)
        
        # Reading metrics table (optimized)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS reading_metrics (
                id SERIAL PRIMARY KEY,
                pdf_id INTEGER,
                exercise_pdf_id INTEGER,
                topic_id INTEGER,
                user_id VARCHAR(50) DEFAULT 'default_user',
                pages_per_minute DECIMAL(8,2),
                average_time_per_page_seconds INTEGER,
                total_pages_read INTEGER DEFAULT 0,
                total_time_spent_seconds INTEGER DEFAULT 0,
                last_calculated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                CONSTRAINT check_metrics_pdf_type CHECK (
                    (pdf_id IS NOT NULL AND exercise_pdf_id IS NULL) OR 
                    (pdf_id IS NULL AND exercise_pdf_id IS NOT NULL)
                )
            )
        """)
        
        # Create optimized indexes
        timer_indexes = [
            "CREATE INDEX IF NOT EXISTS idx_sessions_start_time ON sessions(start_time DESC)",
            "CREATE INDEX IF NOT EXISTS idx_sessions_topic_id ON sessions(topic_id)",
            "CREATE INDEX IF NOT EXISTS idx_page_times_session_id ON page_times(session_id)",
            "CREATE INDEX IF NOT EXISTS idx_reading_metrics_user_topic ON reading_metrics(user_id, topic_id)"
        ]
        
        for index_sql in timer_indexes:
            try:
                self.cursor.execute(index_sql)
            except Exception as e:
                logger.warning(f"Could not create index: {e}")
        
        self.connection.commit()
        logger.info("✅ Optimized timer tables created")
        
    except Exception as e:
        logger.error(f"Error creating timer tables: {e}")
        raise

def create_goals_tables(self):
    """Create optimized goals system tables"""
    try:
        logger.info("Creating optimized goals tables...")
        
        # Goals table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS goals (
                id SERIAL PRIMARY KEY,
                topic_id INTEGER REFERENCES topics(id) ON DELETE CASCADE,
                target_type TEXT CHECK (target_type IN ('finish_by_date', 'daily_time', 'daily_pages')),
                target_value INTEGER NOT NULL DEFAULT 0,
                deadline DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE,
                is_completed BOOLEAN DEFAULT FALSE,
                completion_date TIMESTAMP,
                
                CONSTRAINT valid_deadline CHECK (
                    (target_type = 'finish_by_date' AND deadline IS NOT NULL) OR
                    (target_type != 'finish_by_date')
                ),
                CONSTRAINT valid_target_value CHECK (
                    (target_type = 'finish_by_date' AND target_value >= 0) OR
                    (target_type != 'finish_by_date' AND target_value > 0)
                )
            )
        """)
        
        # Goal progress table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS goal_progress (
                id SERIAL PRIMARY KEY,
                goal_id INTEGER REFERENCES goals(id) ON DELETE CASCADE,
                date DATE NOT NULL,
                pages_read INTEGER DEFAULT 0,
                time_spent_minutes INTEGER DEFAULT 0,
                sessions_count INTEGER DEFAULT 0,
                target_met BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                UNIQUE (goal_id, date),
                
                CONSTRAINT non_negative_pages CHECK (pages_read >= 0),
                CONSTRAINT non_negative_time CHECK (time_spent_minutes >= 0),
                CONSTRAINT non_negative_sessions CHECK (sessions_count >= 0)
            )
        """)
        
        # Create optimized indexes
        goals_indexes = [
            "CREATE INDEX IF NOT EXISTS idx_goals_topic_active ON goals(topic_id, is_active)",
            "CREATE INDEX IF NOT EXISTS idx_goal_progress_date ON goal_progress(goal_id, date)",
            "CREATE INDEX IF NOT EXISTS idx_goal_progress_recent ON goal_progress(date DESC)"
        ]
        
        for index_sql in goals_indexes:
            try:
                self.cursor.execute(index_sql)
            except Exception as e:
                logger.warning(f"Could not create index: {e}")
        
        self.connection.commit()
        logger.info("✅ Optimized goals tables created")
        
    except Exception as e:
        logger.error(f"Error creating goals tables: {e}")
        raise

# Optimized session management methods
def create_session_optimized(self, pdf_id=None, exercise_pdf_id=None, topic_id=None):
    """Optimized session creation"""
    try:
        with self.transaction():
            self.cursor.execute("""
                INSERT INTO sessions (pdf_id, exercise_pdf_id, topic_id, start_time)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP) RETURNING id
            """, (pdf_id, exercise_pdf_id, topic_id))
            
            session_id = self.cursor.fetchone()['id']
            logger.debug(f"Created session {session_id}")
            return session_id
            
    except Exception as e:
        logger.error(f"Failed to create session: {e}")
        raise

def end_session_optimized(self, session_id, total_time_seconds, active_time_seconds, 
                         idle_time_seconds, pages_visited):
    """Optimized session completion"""
    try:
        with self.transaction():
            self.cursor.execute("""
                UPDATE sessions 
                SET end_time = CURRENT_TIMESTAMP,
                    total_time_seconds = %s,
                    active_time_seconds = %s,
                    idle_time_seconds = %s,
                    pages_visited = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                RETURNING pdf_id, exercise_pdf_id, topic_id
            """, (total_time_seconds, active_time_seconds, idle_time_seconds, 
                  pages_visited, session_id))
            
            result = self.cursor.fetchone()
            if result:
                logger.debug(f"Ended session {session_id}")
                return dict(result)
            return None
            
    except Exception as e:
        logger.error(f"Failed to end session: {e}")
        raise

def save_page_time_optimized(self, session_id, pdf_id=None, exercise_pdf_id=None, 
                            page_number=1, duration_seconds=0):
    """Optimized page time saving"""
    try:
        with self.transaction():
            self.cursor.execute("""
                INSERT INTO page_times (session_id, pdf_id, exercise_pdf_id, page_number, duration_seconds)
                VALUES (%s, %s, %s, %s, %s)
            """, (session_id, pdf_id, exercise_pdf_id, page_number, duration_seconds))
            
    except Exception as e:
        logger.error(f"Failed to save page time: {e}")
        raise

def update_reading_metrics_optimized(self, pdf_id=None, exercise_pdf_id=None, topic_id=None,
                                   pages_per_minute=0, average_time_per_page_seconds=0,
                                   pages_read=0, time_spent_seconds=0):
    """Optimized reading metrics update"""
    try:
        with self.transaction():
            # Check for existing metrics
            self.cursor.execute("""
                SELECT id, total_pages_read, total_time_spent_seconds 
                FROM reading_metrics 
                WHERE pdf_id IS NOT DISTINCT FROM %s
                AND exercise_pdf_id IS NOT DISTINCT FROM %s
                AND topic_id IS NOT DISTINCT FROM %s
                LIMIT 1
            """, (pdf_id, exercise_pdf_id, topic_id))
            
            existing = self.cursor.fetchone()
            
            if existing:
                # Update existing
                new_total_pages = existing['total_pages_read'] + pages_read
                new_total_time = existing['total_time_spent_seconds'] + time_spent_seconds
                
                if new_total_pages > 0 and new_total_time > 0:
                    new_avg_time = new_total_time / new_total_pages
                    new_pages_per_minute = new_total_pages / (new_total_time / 60.0)
                else:
                    new_avg_time = average_time_per_page_seconds
                    new_pages_per_minute = pages_per_minute
                
                self.cursor.execute("""
                    UPDATE reading_metrics 
                    SET pages_per_minute = %s,
                        average_time_per_page_seconds = %s,
                        total_pages_read = %s,
                        total_time_spent_seconds = %s,
                        last_calculated = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (new_pages_per_minute, new_avg_time, new_total_pages, 
                      new_total_time, existing['id']))
            else:
                # Create new
                self.cursor.execute("""
                    INSERT INTO reading_metrics 
                    (pdf_id, exercise_pdf_id, topic_id, pages_per_minute,
                     average_time_per_page_seconds, total_pages_read, total_time_spent_seconds)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (pdf_id, exercise_pdf_id, topic_id, pages_per_minute,
                      average_time_per_page_seconds, pages_read, time_spent_seconds))
            
    except Exception as e:
        logger.error(f"Failed to update reading metrics: {e}")
        raise

def get_reading_metrics_optimized(self, pdf_id=None, exercise_pdf_id=None, topic_id=None, user_wide=False):
    """Optimized reading metrics retrieval"""
    try:
        if user_wide:
            self.cursor.execute("""
                SELECT 
                    AVG(pages_per_minute) as pages_per_minute,
                    AVG(average_time_per_page_seconds) as average_time_per_page_seconds,
                    SUM(total_pages_read) as total_pages_read,
                    SUM(total_time_spent_seconds) as total_time_spent_seconds
                FROM reading_metrics
                WHERE total_pages_read > 0
            """)
        else:
            self.cursor.execute("""
                SELECT pages_per_minute, average_time_per_page_seconds, 
                       total_pages_read, total_time_spent_seconds, last_calculated
                FROM reading_metrics 
                WHERE pdf_id IS NOT DISTINCT FROM %s
                AND exercise_pdf_id IS NOT DISTINCT FROM %s
                AND topic_id IS NOT DISTINCT FROM %s
                ORDER BY last_calculated DESC
                LIMIT 1
            """, (pdf_id, exercise_pdf_id, topic_id))
        
        result = self.cursor.fetchone()
        return dict(result) if result else None
        
    except Exception as e:
        logger.error(f"Failed to get reading metrics: {e}")
        return None

def get_session_history_optimized(self, days=7, pdf_id=None, exercise_pdf_id=None):
    """Optimized session history retrieval"""
    try:
        base_query = """
            SELECT s.id, s.start_time, s.end_time, s.total_time_seconds, 
                   s.active_time_seconds, s.pages_visited,
                   p.title as pdf_title, e.title as exercise_title
            FROM sessions s
            LEFT JOIN pdfs p ON s.pdf_id = p.id
            LEFT JOIN exercise_pdfs e ON s.exercise_pdf_id = e.id
            WHERE s.start_time >= CURRENT_DATE - INTERVAL '%s days'
            AND s.end_time IS NOT NULL
        """
        
        params = [days]
        
        if pdf_id:
            base_query += " AND s.pdf_id = %s"
            params.append(pdf_id)
        elif exercise_pdf_id:
            base_query += " AND s.exercise_pdf_id = %s"
            params.append(exercise_pdf_id)
        
        base_query += " ORDER BY s.start_time DESC LIMIT 50"
        
        self.cursor.execute(base_query, params)
        results = self.cursor.fetchall()
        
        return [dict(row) for row in results]
        
    except Exception as e:
        logger.error(f"Failed to get session history: {e}")
        return []

def get_daily_reading_stats_optimized(self, target_date):
    """Optimized daily stats retrieval"""
    try:
        self.cursor.execute("""
            SELECT 
                COUNT(*) as sessions_count,
                COALESCE(SUM(total_time_seconds), 0) as total_time_seconds,
                COALESCE(SUM(pages_visited), 0) as total_pages_read,
                COALESCE(AVG(total_time_seconds), 0) as avg_session_time
            FROM sessions
            WHERE DATE(start_time) = %s AND end_time IS NOT NULL
        """, (target_date,))
        
        result = self.cursor.fetchone()
        return dict(result) if result else None
        
    except Exception as e:
        logger.error(f"Failed to get daily stats: {e}")
        return None

# Alias the optimized methods to the standard names for compatibility
create_session = create_session_optimized
end_session = end_session_optimized
save_page_time = save_page_time_optimized
update_reading_metrics = update_reading_metrics_optimized
get_reading_metrics = get_reading_metrics_optimized
get_session_history = get_session_history_optimized
get_daily_reading_stats = get_daily_reading_stats_optimized
# IMPORTANT: Add this call to your existing initialize_database method
def initialize_database_with_goals(self):
    """Enhanced initialize_database method that includes goals"""
    # Call your existing initialize_database method
    self.initialize_database()
    
    # Then add goals system
    self.create_goals_tables()
    
    logger.info("✅ Database initialized with goals system")