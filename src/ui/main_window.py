import os
import tempfile
import logging
from PyQt6.QtWidgets import (QMainWindow, QWidget, QHBoxLayout, QVBoxLayout, 
                            QSplitter, QLabel, QPushButton, QFileDialog, 
                            QMessageBox, QStatusBar, QMenuBar, QApplication,
                            QInputDialog, QTabWidget)
from PyQt6.QtCore import Qt, pyqtSignal, QTimer
from PyQt6.QtGui import QAction, QFont, QDragEnterEvent, QDropEvent, QKeySequence
from ui.goals_widget import GoalsMainWidget
from database.db_manager import DatabaseManager
from ui.pdf_viewer import PDFViewer
from ui.topic_manager import TopicManager
from ui.timer_widget import TimerWidget, StudyDashboardWidget
from utils.session_timer import SessionTimer, ReadingIntelligence

logger = logging.getLogger(__name__)

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.db_manager = DatabaseManager()
        self.current_pdf_id = None
        self.current_temp_file = None
        self.temp_files_created = []
        
        # Phase 2: Timer and Intelligence
        self.session_timer = SessionTimer(self.db_manager)
        self.reading_intelligence = ReadingIntelligence(self.db_manager)
        self.current_session_id = None
        
        # Timers
        self.page_save_timer = QTimer()
        self.cleanup_timer = QTimer()
        
        self.setup_ui()
        self.setup_menu()
        self.setup_connections()
        self.apply_styles()
        self.start_background_tasks()
        self.load_topics()
        
    def setup_ui(self):
        """Set up the main user interface with Phase 2.1 enhancements"""
        self.setWindowTitle("StudySprint Phase 2.1 - Professional PDF Study Manager with Goals & Timer")
        self.setMinimumSize(1400, 900)
        self.resize(1600, 1000)
        
        # Create central widget and main layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # Create main horizontal splitter
        self.main_splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # Left sidebar with tabs for topics and timer
        self.left_sidebar = QTabWidget()
        self.left_sidebar.setMaximumWidth(450)
        self.left_sidebar.setMinimumWidth(300)
        
        # Topics tab
        self.topic_manager = TopicManager(self.db_manager)
        self.left_sidebar.addTab(self.topic_manager, "📚 Library")
        
        # Timer tab (Phase 2)
        self.timer_widget = TimerWidget()
        self.timer_widget.set_session_timer(self.session_timer)
        self.timer_widget.set_reading_intelligence(self.reading_intelligence)
        self.left_sidebar.addTab(self.timer_widget, "⏱️ Timer")
        
        # Dashboard tab (Phase 2)
        self.dashboard_widget = StudyDashboardWidget(self.db_manager)
        self.dashboard_widget.set_reading_intelligence(self.reading_intelligence)
        self.left_sidebar.addTab(self.dashboard_widget, "📊 Dashboard")
        
        # Center: PDF viewer
        self.pdf_viewer = PDFViewer()
        self.pdf_viewer.set_session_timer(self.session_timer)  # Phase 2: Connect timer
        
        # Add widgets to main splitter
        self.main_splitter.addWidget(self.left_sidebar)
        self.main_splitter.addWidget(self.pdf_viewer)
        
        # Set splitter proportions (30% sidebar, 70% viewer)
        self.main_splitter.setSizes([400, 1200])
        
        # Create main layout
        layout = QHBoxLayout()
        layout.addWidget(self.main_splitter)
        layout.setContentsMargins(5, 5, 5, 5)
        
        central_widget.setLayout(layout)
        
        # Status bar with Phase 2 enhancements
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        
        # Status bar widgets
        self.current_file_label = QLabel("No PDF loaded")
        self.status_bar.addWidget(self.current_file_label)
        
        self.session_status_label = QLabel("No active session")
        self.status_bar.addPermanentWidget(self.session_status_label)
        
        self.page_info_label = QLabel("")
        self.status_bar.addPermanentWidget(self.page_info_label)
        
        self.storage_info_label = QLabel("Database Storage")
        self.status_bar.addPermanentWidget(self.storage_info_label)
        
        self.db_status_label = QLabel("Database: Connected")
        self.status_bar.addPermanentWidget(self.db_status_label)
        
        self.status_bar.showMessage("Ready - Phase 2 with Timer Integration Active")
        
        self.goals_widget = GoalsMainWidget(self.db_manager)
        self.left_sidebar.addTab(self.goals_widget, "🎯 Goals")

    def setup_menu(self):
        """Set up the application menu with Phase 2 additions"""
        menubar = self.menuBar()
        
        # File menu
        file_menu = menubar.addMenu('&File')
        
        add_pdf_action = QAction('&Add PDF...', self)
        add_pdf_action.setShortcut('Ctrl+O')
        add_pdf_action.setStatusTip('Add PDF files to the current topic')
        add_pdf_action.triggered.connect(self.add_pdf)
        file_menu.addAction(add_pdf_action)
        
        add_topic_action = QAction('Add &Topic...', self)
        add_topic_action.setShortcut('Ctrl+T')
        add_topic_action.setStatusTip('Create a new topic')
        add_topic_action.triggered.connect(self.topic_manager.add_topic)
        file_menu.addAction(add_topic_action)
        
        file_menu.addSeparator()
        
        export_action = QAction('&Export PDF...', self)
        export_action.setShortcut('Ctrl+E')
        export_action.setStatusTip('Export current PDF to file system')
        export_action.triggered.connect(self.export_current_pdf)
        file_menu.addAction(export_action)
        
        file_menu.addSeparator()
        
        exit_action = QAction('E&xit', self)
        exit_action.setShortcut('Ctrl+Q')
        exit_action.setStatusTip('Exit the application')
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)
        
        # View menu
        view_menu = menubar.addMenu('&View')
        
        # Zoom actions
        zoom_in_action = QAction('Zoom &In', self)
        zoom_in_action.setShortcut('Ctrl++')
        zoom_in_action.triggered.connect(self.pdf_viewer.zoom_in)
        view_menu.addAction(zoom_in_action)
        
        zoom_out_action = QAction('Zoom &Out', self)
        zoom_out_action.setShortcut('Ctrl+-')
        zoom_out_action.triggered.connect(self.pdf_viewer.zoom_out)
        view_menu.addAction(zoom_out_action)
        
        view_menu.addSeparator()
        
        # Tab switching
        library_action = QAction('Show &Library', self)
        library_action.setShortcut('Ctrl+1')
        library_action.triggered.connect(lambda: self.left_sidebar.setCurrentIndex(0))
        view_menu.addAction(library_action)
        
        timer_action = QAction('Show &Timer', self)
        timer_action.setShortcut('Ctrl+2')
        timer_action.triggered.connect(lambda: self.left_sidebar.setCurrentIndex(1))
        view_menu.addAction(timer_action)
        
        dashboard_action = QAction('Show &Dashboard', self)
        dashboard_action.setShortcut('Ctrl+3')
        dashboard_action.triggered.connect(lambda: self.left_sidebar.setCurrentIndex(2))
        view_menu.addAction(dashboard_action)
        
        # Navigation menu
        nav_menu = menubar.addMenu('&Navigate')
        
        prev_page_action = QAction('&Previous Page', self)
        prev_page_action.setShortcut('Left')
        prev_page_action.triggered.connect(self.pdf_viewer.previous_page)
        nav_menu.addAction(prev_page_action)
        
        next_page_action = QAction('&Next Page', self)
        next_page_action.setShortcut('Right')
        next_page_action.triggered.connect(self.pdf_viewer.next_page)
        nav_menu.addAction(next_page_action)
        
        # Session menu (Phase 2)
        session_menu = menubar.addMenu('&Session')
        
        pause_session_action = QAction('&Pause/Resume Session', self)
        pause_session_action.setShortcut('Ctrl+P')
        pause_session_action.triggered.connect(self.toggle_session)
        session_menu.addAction(pause_session_action)
        
        end_session_action = QAction('&End Session', self)
        end_session_action.setShortcut('Ctrl+Shift+E')
        end_session_action.triggered.connect(self.end_current_session)
        session_menu.addAction(end_session_action)
        
        session_menu.addSeparator()
        
        session_stats_action = QAction('Session &Statistics', self)
        session_stats_action.triggered.connect(self.show_session_stats)
        session_menu.addAction(session_stats_action)
        
        # Database menu
        db_menu = menubar.addMenu('&Database')
        
        stats_action = QAction('&Statistics...', self)
        stats_action.triggered.connect(self.topic_manager.show_stats)
        db_menu.addAction(stats_action)
        
        cleanup_action = QAction('&Cleanup Temp Files', self)
        cleanup_action.triggered.connect(self.cleanup_temp_files)
        db_menu.addAction(cleanup_action)
        
        # Help menu
        help_menu = menubar.addMenu('&Help')
        
        about_action = QAction('&About StudySprint Phase 2', self)
        about_action.triggered.connect(self.show_about)
        help_menu.addAction(about_action)
        
        shortcuts_action = QAction('&Keyboard Shortcuts', self)
        shortcuts_action.triggered.connect(self.show_shortcuts)
        help_menu.addAction(shortcuts_action)
        
        goals_action = QAction('Show &Goals', self)
        goals_action.setShortcut('Ctrl+4')
        goals_action.triggered.connect(lambda: self.left_sidebar.setCurrentIndex(3))
        view_menu.addAction(goals_action)

    def setup_connections(self):
        """Set up signal connections including Phase 2 timer connections"""
        print("Setting up Phase 2 signal connections...")
        
        # Connect the PDF selection signals
        self.topic_manager.pdf_selected.connect(self.load_pdf_from_database)
        self.topic_manager.exercise_pdf_selected.connect(self.load_exercise_pdf_from_database)
        print("Connected PDF selection signals")
        
        # Connect page change signal
        self.pdf_viewer.page_changed.connect(self.on_page_changed)
        print("Connected page_changed signal")
        
        # Phase 2: Connect session timer signals
        self.session_timer.session_started.connect(self.on_session_started)
        self.session_timer.session_ended.connect(self.on_session_ended)
        self.session_timer.page_changed.connect(self.on_timer_page_changed)
        print("Connected session timer signals")
        print("Connecting goals system...")

    def start_background_tasks(self):
        """Start background timers including Phase 2 enhancements"""
        # Auto-save page position every 5 seconds
        self.page_save_timer.timeout.connect(self.save_current_page)
        self.page_save_timer.start(5000)
        
        # Clean up temp files every 30 minutes
        self.cleanup_timer.timeout.connect(self.cleanup_temp_files)
        self.cleanup_timer.start(1800000)
        
    def load_topics(self):
        """Load topics from database"""
        try:
            self.topic_manager.refresh_topics()
            self.db_status_label.setText("Database: Connected")
            self.update_storage_info()
        except Exception as e:
            self.db_status_label.setText("Database: Error")
            QMessageBox.critical(self, "Database Error", f"Failed to load topics: {str(e)}")
    
    def update_storage_info(self):
        """Update storage information in status bar"""
        try:
            stats = self.db_manager.get_database_stats()
            if stats:
                total_size_mb = stats['total_size'] / (1024 * 1024)
                self.storage_info_label.setText(f"💾 {stats['total_pdfs']} PDFs, {total_size_mb:.1f} MB")
            else:
                self.storage_info_label.setText("💾 Database Storage")
        except:
            self.storage_info_label.setText("💾 Storage Error")
                               
    def load_pdf_from_database(self, pdf_id):
        """Load PDF from database and display in viewer with Phase 2 session tracking"""
        print(f"\n=== LOADING PDF FROM DATABASE (Phase 2) ===")
        print(f"PDF ID: {pdf_id}")
        
        try:
            # End current session before starting new one
            if self.current_session_id:
                self.session_timer.end_session()
                
            # Save current position before switching
            if self.current_pdf_id:
                self.save_current_page()
                
            # Clean up previous temporary file
            if self.current_temp_file and os.path.exists(self.current_temp_file):
                try:
                    os.unlink(self.current_temp_file)
                    print(f"Cleaned up previous temp file: {self.current_temp_file}")
                except:
                    pass
                    
            # Create temporary file from database
            print(f"Creating temporary file...")
            temp_file_path = self.db_manager.create_temp_pdf_file(pdf_id)
            
            if not temp_file_path:
                QMessageBox.critical(self, "Error", "Failed to create temporary PDF file from database")
                return
                
            print(f"Temporary file created: {temp_file_path}")
            
            # Load PDF into viewer
            if self.pdf_viewer.load_pdf(temp_file_path, pdf_id):
                self.current_pdf_id = pdf_id
                self.current_temp_file = temp_file_path
                
                # Get PDF info for display
                pdf_info = self.db_manager.get_pdf_by_id(pdf_id)
                if pdf_info:
                    self.current_file_label.setText(f"Loaded: {pdf_info['title']}")
                    
                    # Phase 2: Start session timer
                    topic_id = pdf_info.get('topic_id')
                    self.current_session_id = self.session_timer.start_session(
                        pdf_id=pdf_id, 
                        topic_id=topic_id
                    )
                    
                    # Update timer widget with PDF info
                    self.timer_widget.set_current_pdf_info(pdf_info, is_exercise=False)
                    
                    # Restore reading position
                    self.restore_reading_position(pdf_id)
                    
                    self.status_bar.showMessage(f"Opened {pdf_info['title']} - Session started", 3000)
                    print(f"PDF loaded successfully, session {self.current_session_id} started")
                else:
                    self.current_file_label.setText(f"Loaded: PDF ID {pdf_id}")
                    
            else:
                print(f"Failed to load PDF")
                self.current_pdf_id = None
                self.current_temp_file = None
                self.current_file_label.setText("Failed to load PDF")
                
                # Clean up failed temp file
                try:
                    os.unlink(temp_file_path)
                except:
                    pass
                
        except Exception as e:
            print(f"Error loading PDF: {e}")
            QMessageBox.critical(self, "Error", f"Failed to load PDF from database: {str(e)}")
        
        print(f"=== END LOAD PDF ===\n")

    def load_exercise_pdf_from_database(self, exercise_id):
        """Load exercise PDF from database with Phase 2 session tracking"""
        print(f"\n=== LOADING EXERCISE PDF FROM DATABASE (Phase 2) ===")
        print(f"Exercise PDF ID: {exercise_id}")
        
        try:
            # End current session before starting new one
            if self.current_session_id:
                self.session_timer.end_session()
                
            # Save current position before switching
            if self.current_pdf_id:
                self.save_current_page()
                
            # Clean up previous temporary file
            if self.current_temp_file and os.path.exists(self.current_temp_file):
                try:
                    os.unlink(self.current_temp_file)
                    print(f"Cleaned up previous temp file: {self.current_temp_file}")
                except:
                    pass
                    
            # Create temporary file from database
            print(f"Creating temporary exercise file...")
            temp_file_path = self.db_manager.create_temp_exercise_pdf_file(exercise_id)
            
            if not temp_file_path:
                QMessageBox.critical(self, "Error", "Failed to create temporary exercise PDF file from database")
                return
                
            print(f"Temporary exercise file created: {temp_file_path}")
            
            # Load exercise PDF into viewer
            if self.pdf_viewer.load_pdf(temp_file_path, exercise_id, is_exercise=True):
                self.current_pdf_id = f"exercise_{exercise_id}"  # Mark as exercise
                self.current_temp_file = temp_file_path
                
                # Get exercise PDF info for display
                exercise_info = self.db_manager.get_exercise_pdf_by_id(exercise_id)
                if exercise_info:
                    # Get parent PDF info for topic_id
                    parent_info = self.db_manager.get_pdf_by_id(exercise_info['parent_pdf_id'])
                    parent_title = parent_info['title'] if parent_info else "Unknown"
                    topic_id = parent_info.get('topic_id') if parent_info else None
                    
                    self.current_file_label.setText(f"📝 {exercise_info['title']} (Exercise for: {parent_title})")
                    
                    # Phase 2: Start session timer for exercise
                    self.current_session_id = self.session_timer.start_session(
                        exercise_pdf_id=exercise_id,
                        topic_id=topic_id
                    )
                    
                    # Update timer widget with exercise info
                    exercise_info_enhanced = dict(exercise_info)
                    exercise_info_enhanced['is_exercise'] = True
                    self.timer_widget.set_current_pdf_info(exercise_info_enhanced, is_exercise=True)
                    
                    # Restore reading position for exercise
                    self.restore_exercise_reading_position(exercise_id)
                    
                    self.status_bar.showMessage(f"Opened exercise: {exercise_info['title']} - Session started", 3000)
                    print(f"Exercise PDF loaded successfully, session {self.current_session_id} started")
                else:
                    self.current_file_label.setText(f"Loaded: Exercise PDF ID {exercise_id}")
                    
            else:
                print(f"Failed to load exercise PDF")
                self.current_pdf_id = None
                self.current_temp_file = None
                self.current_file_label.setText("Failed to load exercise PDF")
                
                # Clean up failed temp file
                try:
                    os.unlink(temp_file_path)
                except:
                    pass
                
        except Exception as e:
            print(f"Error loading exercise PDF: {e}")
            QMessageBox.critical(self, "Error", f"Failed to load exercise PDF from database: {str(e)}")
        
        print(f"=== END LOAD EXERCISE PDF ===\n")
            
    def restore_reading_position(self, pdf_id):
        """Restore the last reading position for a PDF"""
        try:
            print(f"Restoring position for PDF {pdf_id}")
            pdf_info = self.db_manager.get_pdf_by_id(pdf_id)
            
            if pdf_info and pdf_info['current_page'] > 1:
                print(f"Restoring to page {pdf_info['current_page']}")
                self.pdf_viewer.set_page(pdf_info['current_page'])
                
                # Phase 2: Notify session timer of page change
                self.session_timer.change_page(pdf_info['current_page'])
                
                self.status_bar.showMessage(f"Resumed at page {pdf_info['current_page']}", 2000)
            else:
                print(f"No saved position or starting from page 1")
                self.session_timer.change_page(1)
                
        except Exception as e:
            print(f"Error restoring reading position: {e}")

    def restore_exercise_reading_position(self, exercise_id):
        """Restore the last reading position for an exercise PDF"""
        try:
            print(f"Restoring position for exercise PDF {exercise_id}")
            exercise_info = self.db_manager.get_exercise_pdf_by_id(exercise_id)
            
            if exercise_info and exercise_info['current_page'] > 1:
                print(f"Restoring exercise to page {exercise_info['current_page']}")
                self.pdf_viewer.set_page(exercise_info['current_page'])
                
                # Phase 2: Notify session timer of page change
                self.session_timer.change_page(exercise_info['current_page'])
                
                self.status_bar.showMessage(f"Resumed exercise at page {exercise_info['current_page']}", 2000)
            else:
                print(f"No saved position for exercise or starting from page 1")
                self.session_timer.change_page(1)
                
        except Exception as e:
            print(f"Error restoring exercise reading position: {e}")

    def save_current_page(self):
        """Save current page position to database (handles both main and exercise PDFs)"""
        if not self.current_pdf_id or not self.pdf_viewer.pdf_document:
            return
            
        try:
            current_page = self.pdf_viewer.get_current_page()
            
            if str(self.current_pdf_id).startswith("exercise_"):
                # This is an exercise PDF
                exercise_id = int(str(self.current_pdf_id).replace("exercise_", ""))
                print(f"Saving page {current_page} for exercise PDF {exercise_id}")
                self.db_manager.update_exercise_pdf_page(exercise_id, current_page)
            else:
                # This is a main PDF
                print(f"Saving page {current_page} for main PDF {self.current_pdf_id}")
                self.db_manager.update_pdf_page(self.current_pdf_id, current_page)
                
        except Exception as e:
            print(f"Error saving page position: {e}")
            
    def on_page_changed(self, page_num):
        """Handle page changes with Phase 2 session tracking"""
        if self.pdf_viewer.total_pages > 0:
            self.page_info_label.setText(f"Page {page_num} of {self.pdf_viewer.total_pages}")
            progress = (page_num / self.pdf_viewer.total_pages) * 100
            self.status_bar.showMessage(f"Progress: {progress:.1f}%", 1000)
            
            # Phase 2: Notify session timer of page change
            if self.current_session_id:
                self.session_timer.change_page(page_num)
    
    # Phase 2: Session timer signal handlers
    def on_session_started(self, session_id):
        """Handle session started signal"""
        self.current_session_id = session_id
        self.session_status_label.setText(f"📖 Session {session_id}")
        print(f"Session {session_id} started successfully")
    
    def on_session_ended(self, session_id, stats):
        """Handle session ended with comprehensive cleanup and goals update"""
        self.current_session_id = None
        self.session_status_label.setText("No active session")
        
        if stats:
            # Show session summary
            total_time = stats.get('total_time_seconds', 0)
            pages_visited = stats.get('pages_visited', 0)
            minutes = total_time // 60
            seconds = total_time % 60
            
            self.status_bar.showMessage(
                f"Session ended: {minutes}m {seconds}s, {pages_visited} pages", 
                5000
            )
            
            # Update goals progress (Phase 2.1)
            try:
                if hasattr(self, 'goals_widget') and self.current_pdf_id:
                    # Get topic ID from current PDF
                    if str(self.current_pdf_id).startswith("exercise_"):
                        exercise_id = int(str(self.current_pdf_id).replace("exercise_", ""))
                        exercise_info = self.db_manager.get_exercise_pdf_by_id(exercise_id)
                        if exercise_info:
                            parent_info = self.db_manager.get_pdf_by_id(exercise_info['parent_pdf_id'])
                            topic_id = parent_info.get('topic_id') if parent_info else None
                        else:
                            topic_id = None
                    else:
                        pdf_info = self.db_manager.get_pdf_by_id(self.current_pdf_id)
                        topic_id = pdf_info.get('topic_id') if pdf_info else None
                    
                    if topic_id:
                        self.goals_widget.update_after_session(
                            topic_id=topic_id,
                            pages_read=pages_visited,
                            time_spent_seconds=total_time
                        )
                        logger.info(f"Updated goals for topic {topic_id}")
                        
            except Exception as e:
                logger.error(f"Error updating goals after session: {e}")
        
        print(f"Session {session_id} ended")
    
    def on_timer_page_changed(self, session_id, old_page, new_page):
        """Handle page changes from session timer"""
        # This is called by the session timer, we already handle UI updates in on_page_changed
        pass
    
    def toggle_session(self):
        """Toggle pause/resume session"""
        if self.current_session_id:
            self.timer_widget.toggle_pause_resume()
    
    def end_current_session(self):
        """End the current session manually"""
        if self.current_session_id:
            self.session_timer.end_session()
    
    def show_session_stats(self):
        """Show current session statistics"""
        if not self.current_session_id:
            QMessageBox.information(self, "No Session", "No active session to show statistics for")
            return
        
        stats = self.session_timer.get_current_stats()
        if stats:
            total_time = stats['total_time_seconds']
            active_time = stats['active_time_seconds']
            pages_visited = stats['pages_visited']
            current_page = stats['current_page']
            
            hours = total_time // 3600
            minutes = (total_time % 3600) // 60
            seconds = total_time % 60
            
            active_hours = active_time // 3600
            active_minutes = (active_time % 3600) // 60
            active_secs = active_time % 60
            
            stats_text = f"""
            <h3>📊 Current Session Statistics</h3>
            
            <p><b>Session ID:</b> {self.current_session_id}</p>
            <p><b>Document Type:</b> {'Exercise PDF' if stats['is_exercise'] else 'Main PDF'}</p>
            
            <h4>⏱️ Time Statistics:</h4>
            <ul>
            <li><b>Total Time:</b> {hours:02d}:{minutes:02d}:{seconds:02d}</li>
            <li><b>Active Time:</b> {active_hours:02d}:{active_minutes:02d}:{active_secs:02d}</li>
            <li><b>Idle Time:</b> {(total_time - active_time) // 60} minutes</li>
            </ul>
            
            <h4>📖 Reading Statistics:</h4>
            <ul>
            <li><b>Current Page:</b> {current_page}</li>
            <li><b>Pages Visited:</b> {pages_visited}</li>
            <li><b>Status:</b> {'🟡 Idle' if stats['is_idle'] else '🟢 Active'}</li>
            </ul>
            """
            
            QMessageBox.information(self, "Session Statistics", stats_text)
    
    def add_pdf(self):
        """Add a new PDF file"""
        current_item = self.topic_manager.topic_tree.currentItem()
        if not current_item:
            reply = QMessageBox.question(
                self, "No Topic Selected", 
                "No topic is selected. Would you like to create a new topic first?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            
            if reply == QMessageBox.StandardButton.Yes:
                self.topic_manager.add_topic()
                return
            else:
                QMessageBox.information(
                    self, "Select Topic", 
                    "Please select a topic from the sidebar before adding PDFs."
                )
                return
                
        self.topic_manager.add_pdf()
        
    def export_current_pdf(self):
        """Export the currently viewed PDF (handles both main and exercise PDFs)"""
        if not self.current_pdf_id:
            QMessageBox.information(self, "No PDF", "No PDF is currently loaded")
            return
            
        try:
            if str(self.current_pdf_id).startswith("exercise_"):
                # Export exercise PDF
                exercise_id = int(str(self.current_pdf_id).replace("exercise_", ""))
                pdf_data = self.db_manager.get_exercise_pdf_data(exercise_id)
                if not pdf_data:
                    QMessageBox.warning(self, "Export Error", "Could not retrieve exercise PDF data")
                    return
            else:
                # Export main PDF
                pdf_data = self.db_manager.get_pdf_data(self.current_pdf_id)
                if not pdf_data:
                    QMessageBox.warning(self, "Export Error", "Could not retrieve PDF data")
                    return
                
            # Ask user where to save
            suggested_name = pdf_data['file_name']
            file_path, _ = QFileDialog.getSaveFileName(
                self, "Export PDF", suggested_name, "PDF Files (*.pdf)"
            )
            
            if file_path:
                with open(file_path, 'wb') as f:
                    f.write(pdf_data['data'])
                    
                QMessageBox.information(self, "Export Complete", 
                                      f"PDF exported successfully to:\n{file_path}")
                self.status_bar.showMessage(f"Exported to {file_path}", 3000)
                
        except Exception as e:
            QMessageBox.critical(self, "Export Error", f"Failed to export PDF: {str(e)}")
        
    def cleanup_temp_files(self):
        """Clean up temporary files"""
        try:
            self.db_manager.cleanup_temp_files()
            self.status_bar.showMessage("Temporary files cleaned up", 2000)
        except Exception as e:
            print(f"Error cleaning up temp files: {e}")
        
    
    def show_about(self):
        """Enhanced about dialog with Phase 2.1 features"""
        QMessageBox.about(
            self, "About StudySprint Phase 2.1",
            "<h3>StudySprint v2.1.0 - Phase 2.1: Goal Setting & Progress Tracking</h3>"
            "<p>A powerful PDF study management application with advanced session tracking, reading intelligence, and comprehensive goal setting.</p>"
            "<p><b>Phase 2.1 Features (NEW!):</b></p>"
            "<ul>"
            "<li>🎯 <b>Study Goals</b> - Set and track finish-by-date, daily time, and daily pages goals</li>"
            "<li>📊 <b>Progress Tracking</b> - Automatic progress updates after each study session</li>"
            "<li>📈 <b>Smart Adjustments</b> - Dynamic daily plan adjustments when behind schedule</li>"
            "<li>📅 <b>Daily Progress</b> - See today's progress across all goals at a glance</li>"
            "<li>💡 <b>Goal Analytics</b> - Detailed insights, trends, and success metrics</li>"
            "<li>🔥 <b>Streak Tracking</b> - Monitor consistency and reading habits</li>"
            "</ul>"
            "<p><b>Phase 2 Features:</b></p>"
            "<ul>"
            "<li>⏱️ <b>Session Timer</b> - Automatic session tracking with start/end times</li>"
            "<li>📄 <b>Per-Page Timing</b> - Track time spent on each page</li>"
            "<li>🧠 <b>Reading Intelligence</b> - Calculate reading speed and estimates</li>"
            "<li>😴 <b>Idle Detection</b> - Automatic pause when inactive for 2+ minutes</li>"
            "<li>📊 <b>Progress Dashboard</b> - View reading statistics and analytics</li>"
            "<li>🎯 <b>Finish Time Estimation</b> - Smart estimates based on your reading speed</li>"
            "<li>📈 <b>Daily Statistics</b> - Track your reading habits over time</li>"
            "</ul>"
            "<p><b>Core Features:</b></p>"
            "<ul>"
            "<li>✅ Complete PDF storage in database with integrity verification</li>"
            "<li>✅ Exercise PDF linking system</li>"
            "<li>✅ Automatic reading position saving</li>"
            "<li>✅ Full PDF viewing with zoom and navigation</li>"
            "<li>✅ Topic-based organization</li>"
            "</ul>"
            "<p><b>How to Use Goals (Phase 2.1):</b></p>"
            "<ol>"
            "<li>Click the Goals tab to access goal management</li>"
            "<li>Create goals: finish-by-date, daily time, or daily pages</li>"
            "<li>Study as usual - progress updates automatically</li>"
            "<li>Check Today's Progress to see how you're doing</li>"
            "<li>View Analytics for detailed insights and trends</li>"
            "</ol>"
            "<p><b>Keyboard Shortcuts:</b></p>"
            "<ul>"
            "<li><b>Ctrl+1:</b> Show Library tab</li>"
            "<li><b>Ctrl+2:</b> Show Timer tab</li>"
            "<li><b>Ctrl+3:</b> Show Dashboard tab</li>"
            "<li><b>Ctrl+4:</b> Show Goals tab (NEW!)</li>"
            "<li><b>Ctrl+P:</b> Pause/Resume session</li>"
            "<li><b>Ctrl+Shift+E:</b> End current session</li>"
            "</ul>"
            "<p>Built with PyQt6, PostgreSQL, and advanced analytics algorithms</p>"
        )
    
    def show_shortcuts(self):
        """Show keyboard shortcuts dialog"""
        shortcuts_text = """
        <h3>⌨️ StudySprint Phase 2.1 Keyboard Shortcuts</h3>
        
        <h4>📁 File Operations:</h4>
        <ul>
        <li><b>Ctrl+O:</b> Add PDF files</li>
        <li><b>Ctrl+T:</b> Add new topic</li>
        <li><b>Ctrl+E:</b> Export current PDF</li>
        <li><b>Ctrl+Q:</b> Exit application</li>
        </ul>
        
        <h4>👁️ View Controls:</h4>
        <ul>
        <li><b>Ctrl++:</b> Zoom in</li>
        <li><b>Ctrl+-:</b> Zoom out</li>
        <li><b>Ctrl+1:</b> Show Library tab</li>
        <li><b>Ctrl+2:</b> Show Timer tab</li>
        <li><b>Ctrl+3:</b> Show Dashboard tab</li>
        <li><b>Ctrl+4:</b> Show Goals tab (NEW!)</li>
        </ul>
        
        <h4>📖 Navigation:</h4>
        <ul>
        <li><b>Left Arrow:</b> Previous page</li>
        <li><b>Right Arrow:</b> Next page</li>
        <li><b>Esc:</b> Clear selection</li>
        </ul>
        
        <h4>⏱️ Session Controls (Phase 2):</h4>
        <ul>
        <li><b>Ctrl+P:</b> Pause/Resume current session</li>
        <li><b>Ctrl+Shift+E:</b> End current session</li>
        </ul>
        
        <h4>🎯 Goals (Phase 2.1):</h4>
        <ul>
        <li><b>Ctrl+4:</b> Open Goals tab</li>
        <li>Goals update automatically after each study session</li>
        <li>Check Today's Progress for daily goal status</li>
        </ul>
        
        <p><i>Sessions start automatically when you open a PDF and end when you close it or go idle for 2+ minutes. Goals track your progress automatically!</i></p>
        """
        
        QMessageBox.information(self, "Keyboard Shortcuts", shortcuts_text)

    def apply_styles(self):
        """Apply consistent styling with Phase 2 enhancements"""
        self.setStyleSheet("""
            QMainWindow {
                background-color: #ffffff;
                color: #000000;
            }
            QSplitter::handle {
                background-color: #cccccc;
                width: 3px;
            }
            QSplitter::handle:hover {
                background-color: #007acc;
            }
            QStatusBar {
                background-color: #f5f5f5;
                border-top: 1px solid #cccccc;
                color: #000000;
                font-size: 12px;
            }
            QStatusBar QLabel {
                color: #000000;
                padding: 2px 8px;
            }
            QMenuBar {
                background-color: #ffffff;
                color: #000000;
                border-bottom: 1px solid #cccccc;
            }
            QMenuBar::item {
                background-color: transparent;
                color: #000000;
                padding: 6px 12px;
            }
            QMenuBar::item:selected {
                background-color: #007acc;
                color: white;
            }
            QMenu {
                background-color: #ffffff;
                color: #000000;
                border: 1px solid #cccccc;
            }
            QMenu::item {
                padding: 6px 12px;
                color: #000000;
            }
            QMenu::item:selected {
                background-color: #007acc;
                color: white;
            }
            QTabWidget::pane {
                border: 1px solid #cccccc;
                background-color: #ffffff;
            }
            QTabBar::tab {
                background-color: #f0f0f0;
                color: #000000;
                padding: 8px 16px;
                margin-right: 2px;
                border-top-left-radius: 4px;
                border-top-right-radius: 4px;
            }
            QTabBar::tab:selected {
                background-color: #007acc;
                color: white;
            }
            QTabBar::tab:hover {
                background-color: #e6f3ff;
            }
        """)
        
    def keyPressEvent(self, event):
        """Handle keyboard events with Phase 2 shortcuts"""
        if event.key() == Qt.Key.Key_Left:
            self.pdf_viewer.previous_page()
        elif event.key() == Qt.Key.Key_Right:
            self.pdf_viewer.next_page()
        elif event.key() == Qt.Key.Key_Escape:
            self.topic_manager.topic_tree.clearSelection()
        elif event.modifiers() == Qt.KeyboardModifier.ControlModifier:
            if event.key() == Qt.Key.Key_1:
                self.left_sidebar.setCurrentIndex(0)  # Library
            elif event.key() == Qt.Key.Key_2:
                self.left_sidebar.setCurrentIndex(1)  # Timer
            elif event.key() == Qt.Key.Key_3:
                self.left_sidebar.setCurrentIndex(2)  # Dashboard
            elif event.key() == Qt.Key.Key_4:
                self.left_sidebar.setCurrentIndex(3)  # Goals
            elif event.key() == Qt.Key.Key_P:
                self.toggle_session()
        elif event.modifiers() == (Qt.KeyboardModifier.ControlModifier | Qt.KeyboardModifier.ShiftModifier):
            if event.key() == Qt.Key.Key_E:
                self.end_current_session()
        else:
            super().keyPressEvent(event)

            
    def closeEvent(self, event):
        """Handle application close with Phase 2 session cleanup"""
        # End current session
        if self.current_session_id:
            print("Ending session before closing...")
            self.session_timer.end_session()
        
        # Save current page position
        if self.current_pdf_id:
            self.save_current_page()
            
        # Stop timers
        self.page_save_timer.stop()
        self.cleanup_timer.stop()
        
        # Clean up temporary file
        if self.current_temp_file and os.path.exists(self.current_temp_file):
            try:
                os.unlink(self.current_temp_file)
                print(f"Cleaned up temp file on exit: {self.current_temp_file}")
            except:
                pass
        
        # Clean up all temp files
        try:
            self.db_manager.cleanup_temp_files()
        except:
            pass
        
        # Close database connection
        self.db_manager.disconnect()
        
        # Close PDF document
        if self.pdf_viewer.pdf_document:
            self.pdf_viewer.pdf_document.close()
            
        self.status_bar.showMessage("Shutting down Phase 2...", 1000)
        event.accept()
    
    def show_session_summary_with_goals(self, session_stats):
        """Show session summary with goals progress update"""
        if not session_stats:
            return
        
        total_time = session_stats.get('total_time_seconds', 0)
        pages_visited = session_stats.get('pages_visited', 0)
        active_time = session_stats.get('active_time_seconds', 0)
        
        minutes = total_time // 60
        seconds = total_time % 60
        efficiency = (active_time / total_time * 100) if total_time > 0 else 0
        
        # Get updated goal status if applicable
        goals_update = ""
        try:
            if hasattr(self, 'goals_widget'):
                today_progress = self.goals_widget.goals_manager.get_today_progress()
                completed_goals = len([g for g in today_progress['daily_goals'] if g.get('target_met_today')])
                total_daily_goals = len(today_progress['daily_goals'])
                
                if total_daily_goals > 0:
                    goals_update = f"\n\n🎯 Goals Progress:\n{completed_goals}/{total_daily_goals} daily goals completed today"
        except:
            pass
        
        summary_text = f"""
        <h3>📖 Study Session Complete</h3>
        
        <h4>⏱️ Session Statistics:</h4>
        <ul>
        <li><b>Total Time:</b> {minutes:02d}:{seconds:02d}</li>
        <li><b>Active Time:</b> {active_time // 60}:{active_time % 60:02d}</li>
        <li><b>Pages Read:</b> {pages_visited}</li>
        <li><b>Efficiency:</b> {efficiency:.1f}%</li>
        </ul>
        
        {goals_update}
        
        <p><i>Great work! Your progress has been automatically saved and goals updated.</i></p>
        """
        
        QMessageBox.information(self, "Session Summary", summary_text)
    def __init__(self):
        super().__init__()
        self.db_manager = DatabaseManager()
        self.current_pdf_id = None
        self.current_temp_file = None
        self.temp_files_created = []
        
        # Phase 2: Timer and Intelligence
        self.session_timer = SessionTimer(self.db_manager)
        self.reading_intelligence = ReadingIntelligence(self.db_manager)
        self.current_session_id = None
        
        # Timers
        self.page_save_timer = QTimer()
        self.cleanup_timer = QTimer()
        
        self.setup_ui()
        self.setup_menu()
        self.setup_connections()
        self.apply_styles()
        self.start_background_tasks()
        self.load_topics()