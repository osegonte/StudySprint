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
        # Create exercise PDF tables
        self.create_exercise_tables()
        
        # Create Phase 2 tables
        # self.create_phase2_tables()  # Temporarily disabled
        
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
    def create_phase2_tables(self):
        """Create Phase 2 timer and analytics tables"""
        self.connect()
        
        logger.info("Creating Phase 2 tables...")
        
        try:
            # Sessions table
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
            
            # Create indexes
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_sessions_pdf_id ON sessions(pdf_id)",
                # "CREATE INDEX IF NOT EXISTS idx_sessions_exercise_pdf_id ON sessions(exercise_pdf_id)",  # Will be created when exercise_pdfs table exists
                "CREATE INDEX IF NOT EXISTS idx_sessions_start_time ON sessions(start_time)",
                "CREATE INDEX IF NOT EXISTS idx_page_times_session_id ON page_times(session_id)",
                "CREATE INDEX IF NOT EXISTS idx_page_times_pdf_id ON page_times(pdf_id)",
                # "CREATE INDEX IF NOT EXISTS idx_page_times_exercise_pdf_id ON page_times(exercise_pdf_id)",  # Will be created when exercise_pdfs table exists
                "CREATE INDEX IF NOT EXISTS idx_reading_metrics_pdf_id ON reading_metrics(pdf_id)",
                # "CREATE INDEX IF NOT EXISTS idx_reading_metrics_exercise_pdf_id ON reading_metrics(exercise_pdf_id)",  # Will be created when exercise_pdfs table exists
                "CREATE INDEX IF NOT EXISTS idx_reading_metrics_topic_id ON reading_metrics(topic_id)",
            ]
            
            for index_sql in indexes:
                self.cursor.execute(index_sql)
            
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
            
            self.connection.commit()
            logger.info("✅ Phase 2 tables created successfully")
            
        except Exception as e:
            logger.error(f"Failed to create Phase 2 tables: {e}")
            self.connection.rollback()
            raise

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

    def create_exercise_tables(self):
        """Create tables for exercise PDF linking system"""
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
            
            self.connection.commit()
            logger.info("✅ Exercise PDF tables created successfully")
            
        except Exception as e:
            logger.error(f"Failed to create exercise tables: {e}")
            self.connection.rollback()
            raise

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
