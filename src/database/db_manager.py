import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import tempfile
import hashlib

load_dotenv()

class DatabaseManager:
    def __init__(self):
        self.connection = None
        self.cursor = None
        self.has_file_data = False  # Track if we have the new schema
        
    def connect(self):
        """Connect to PostgreSQL database"""
        try:
            if self.connection and not self.connection.closed:
                return  # Already connected
                
            self.connection = psycopg2.connect(
                dbname=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT'),
                cursor_factory=RealDictCursor
            )
            self.cursor = self.connection.cursor()
            
            # Check schema version
            self.check_schema()
            
            print("✅ Database connected successfully")
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            
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
            print(f"📊 Schema detected: {schema_type}")
            
        except Exception as e:
            print(f"Could not check schema: {e}")
            self.has_file_data = False
            
    def disconnect(self):
        """Disconnect from database"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            
    def initialize_database(self):
        """Create tables if they don't exist"""
        self.connect()
        
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
        
        # Create PDFs table - check which schema we have
        if self.has_file_data:
            # We already have the new schema, just ensure it exists
            print("Database tables already exist with new schema")
        else:
            # Create with old schema for compatibility
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS pdfs (
                    id SERIAL PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    file_path VARCHAR(500) NOT NULL,
                    file_size BIGINT,
                    total_pages INTEGER,
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
        
        # Create indexes
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_pdfs_topic_id ON pdfs(topic_id)")
        
        if self.has_file_data:
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_pdfs_content_hash ON pdfs(content_hash)")
        
        self.connection.commit()
        print("✅ Database tables created successfully")
        
        # Check schema again after table creation
        self.check_schema()
        
    def get_all_topics(self):
        """Get all topics"""
        self.connect()
        self.cursor.execute("SELECT * FROM topics ORDER BY name")
        topics = self.cursor.fetchall()
        print(f"Database: Found {len(topics)} topics")
        return topics
        
    def create_topic(self, name, description="", color="#3498db"):
        """Create a new topic"""
        self.connect()
        self.cursor.execute("""
            INSERT INTO topics (name, description, color)
            VALUES (%s, %s, %s) RETURNING id
        """, (name, description, color))
        self.connection.commit()
        topic_id = self.cursor.fetchone()['id']
        print(f"Database: Created topic '{name}' with ID {topic_id}")
        return topic_id
        
    def get_pdfs_by_topic(self, topic_id):
        """Get all PDFs for a specific topic"""
        self.connect()
        
        if self.has_file_data:
            # New schema - database storage
            self.cursor.execute("""
                SELECT id, title, file_name, file_size, total_pages, current_page, 
                       topic_id, created_at, updated_at, 
                       LENGTH(file_data) as actual_size,
                       content_hash
                FROM pdfs 
                WHERE topic_id = %s 
                ORDER BY title
            """, (topic_id,))
        else:
            # Old schema - file storage (shouldn't happen after migration)
            self.cursor.execute("""
                SELECT id, title, file_path, file_size, total_pages, current_page, 
                       topic_id, created_at, updated_at,
                       file_size as actual_size
                FROM pdfs 
                WHERE topic_id = %s 
                ORDER BY title
            """, (topic_id,))
            
        pdfs = self.cursor.fetchall()
        print(f"Database: Found {len(pdfs)} PDFs for topic {topic_id}")
        
        # Add schema-specific verification
        for pdf in pdfs:
            if self.has_file_data:
                size_match = pdf['file_size'] == pdf['actual_size']
                print(f"  PDF ID {pdf['id']}: {pdf['title']}")
                print(f"    File: {pdf['file_name']}")
                print(f"    Stored size: {pdf['file_size']} bytes")
                print(f"    Data integrity: {'✅ OK' if size_match else '❌ CORRUPTED'}")
            else:
                # This shouldn't happen after migration, but handle it
                file_path = pdf.get('file_path', '')
                file_exists = os.path.exists(file_path) if file_path else False
                print(f"  PDF ID {pdf['id']}: {pdf['title']}")
                print(f"    File path: {file_path}")
                print(f"    File exists: {'✅ OK' if file_exists else '❌ MISSING'}")
            
        return pdfs
        
    def add_pdf(self, title, file_path, topic_id, total_pages=0):
        """Add a new PDF to the database"""
        self.connect()
        
        try:
            print(f"Database: Adding PDF '{title}' to topic {topic_id}")
            print(f"  Reading file: {file_path}")
            
            # Always store in database now (we have the new schema)
            with open(file_path, 'rb') as f:
                file_data = f.read()
                
            file_size = len(file_data)
            file_name = os.path.basename(file_path)
            
            # Generate content hash
            content_hash = hashlib.sha256(file_data).hexdigest()
            
            print(f"  File size: {file_size} bytes")
            print(f"  Content hash: {content_hash[:16]}...")
            
            # Check for duplicates
            self.cursor.execute("""
                SELECT id, title FROM pdfs WHERE content_hash = %s
            """, (content_hash,))
            duplicate = self.cursor.fetchone()
            
            if duplicate:
                print(f"  ⚠️  Duplicate detected: Same content as PDF {duplicate['id']} ({duplicate['title']})")
                # Skip duplicates automatically
                print("  Skipping duplicate PDF")
                return None
            
            # Store PDF in database
            self.cursor.execute("""
                INSERT INTO pdfs (title, file_name, file_data, file_size, content_hash, total_pages, topic_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
            """, (title, file_name, psycopg2.Binary(file_data), file_size, content_hash, total_pages, topic_id))
            
            self.connection.commit()
            
            pdf_id = self.cursor.fetchone()['id']
            print(f"Database: PDF added with ID {pdf_id}")
            return pdf_id
            
        except Exception as e:
            print(f"Database: Error adding PDF: {e}")
            self.connection.rollback()
            raise
        
    def get_pdf_data(self, pdf_id):
        """Retrieve PDF data from database"""
        self.connect()
        
        try:
            print(f"Database: Retrieving PDF data for ID {pdf_id}")
            
            self.cursor.execute("""
                SELECT title, file_name, file_data, file_size, content_hash
                FROM pdfs 
                WHERE id = %s
            """, (pdf_id,))
            
            result = self.cursor.fetchone()
            
            if not result:
                print(f"Database: PDF {pdf_id} not found")
                return None
                
            file_data = bytes(result['file_data'])
            expected_size = result['file_size']
            actual_size = len(file_data)
            
            print(f"  Title: {result['title']}")
            print(f"  Expected size: {expected_size} bytes")
            print(f"  Retrieved size: {actual_size} bytes")
            
            # Verify data integrity
            actual_hash = hashlib.sha256(file_data).hexdigest()
            expected_hash = result['content_hash']
            
            if actual_hash == expected_hash:
                print(f"  ✅ Data integrity verified")
            else:
                print(f"  ❌ Data corruption detected!")
                return None
            
            return {
                'title': result['title'],
                'file_name': result['file_name'],
                'data': file_data,
                'size': actual_size
            }
            
        except Exception as e:
            print(f"Database: Error retrieving PDF data: {e}")
            return None
    
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
                
            print(f"Database: Created temporary PDF file: {temp_path}")
            print(f"  Size: {len(pdf_data['data'])} bytes")
            
            # Verify the temporary file
            if os.path.exists(temp_path) and os.path.getsize(temp_path) == pdf_data['size']:
                print(f"  ✅ Temporary file verified")
                return temp_path
            else:
                print(f"  ❌ Temporary file creation failed")
                try:
                    os.unlink(temp_path)
                except:
                    pass
                return None
                
        except Exception as e:
            print(f"Database: Error creating temporary file: {e}")
            return None
            
    def update_pdf_page(self, pdf_id, current_page):
        """Update the current page for a PDF"""
        self.connect()
        print(f"Database: Updating PDF {pdf_id} current page to {current_page}")
        
        self.cursor.execute("""
            UPDATE pdfs SET current_page = %s, updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (current_page, pdf_id))
        self.connection.commit()
        
        print(f"Database: Page position saved")
        
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
            print(f"Database: Found PDF {pdf_id}: {pdf['title']}")
            print(f"  File: {pdf['file_name']}")
            print(f"  Size: {pdf.get('file_size', 0)} bytes")
            print(f"  Pages: {pdf.get('total_pages', 0)}")
            print(f"  Current page: {pdf.get('current_page', 1)}")
        else:
            print(f"Database: PDF {pdf_id} not found")
            
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
            print(f"Database: Cleaned up {cleaned_count} old temporary files")
            
    def get_database_stats(self):
        """Get database storage statistics"""
        self.connect()
        
        try:
            # Database storage stats
            self.cursor.execute("""
                SELECT 
                    COUNT(*) as total_pdfs,
                    SUM(file_size) as total_size,
                    AVG(file_size) as avg_size,
                    MAX(file_size) as max_size
                FROM pdfs
            """)
                
            stats = self.cursor.fetchone()
            
            # Get size by topic
            self.cursor.execute("""
                SELECT 
                    t.name as topic_name,
                    COUNT(p.id) as pdf_count,
                    SUM(p.file_size) as topic_size
                FROM topics t
                LEFT JOIN pdfs p ON t.id = p.topic_id
                GROUP BY t.id, t.name
                ORDER BY topic_size DESC NULLS LAST
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
            print(f"Database: Error getting stats: {e}")
            return None