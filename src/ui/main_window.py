import os
import tempfile
import logging
from PyQt6.QtWidgets import (QMainWindow, QWidget, QHBoxLayout, QVBoxLayout, 
                            QSplitter, QLabel, QPushButton, QFileDialog, 
                            QMessageBox, QStatusBar, QMenuBar, QApplication,
                            QInputDialog, QTabWidget)
from PyQt6.QtCore import Qt, pyqtSignal, QTimer
from PyQt6.QtGui import QAction, QFont, QKeySequence
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
        
        # Core components
        self.db_manager = DatabaseManager()
        self.current_pdf_id = None
        self.current_temp_file = None
        self.temp_files_created = []
        
        # Timer and Intelligence (Phase 2)
        self.session_timer = SessionTimer(self.db_manager)
        self.reading_intelligence = ReadingIntelligence(self.db_manager)
        self.current_session_id = None
        
        # Background timers
        self.page_save_timer = QTimer()
        self.cleanup_timer = QTimer()
        
        # Initialize UI and connections
        self.setup_ui()
        self.setup_menu()
        self.setup_connections()
        self.apply_styles()
        self.start_background_tasks()
        self.load_topics()
        
    def setup_ui(self):
        """Set up the complete Phase 2.1 user interface"""
        self.setWindowTitle("StudySprint Phase 2.1 - Complete Study Management System")
        self.setMinimumSize(1400, 900)
        self.resize(1600, 1000)
        
        # Create central widget and main layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # Create main horizontal splitter
        self.main_splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # Left sidebar with tabbed interface
        self.left_sidebar = QTabWidget()
        self.left_sidebar.setMaximumWidth(450)
        self.left_sidebar.setMinimumWidth(300)
        
        # Initialize all tabs
        self._setup_library_tab()
        self._setup_timer_tab()
        self._setup_dashboard_tab()
        self._setup_goals_tab()
        
        # Center: PDF viewer with enhanced features
        self.pdf_viewer = PDFViewer()
        self.pdf_viewer.set_session_timer(self.session_timer)
        
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
        
        # Enhanced status bar
        self._setup_status_bar()
        
    def _setup_library_tab(self):
        """Set up the Library tab"""
        self.topic_manager = TopicManager(self.db_manager)
        self.left_sidebar.addTab(self.topic_manager, "📚 Library")
        
    def _setup_timer_tab(self):
        """Set up the Timer tab"""
        self.timer_widget = TimerWidget()
        self.timer_widget.set_session_timer(self.session_timer)
        self.timer_widget.set_reading_intelligence(self.reading_intelligence)
        self.left_sidebar.addTab(self.timer_widget, "⏱️ Timer")
        
    def _setup_dashboard_tab(self):
        """Set up the Dashboard tab"""
        self.dashboard_widget = StudyDashboardWidget(self.db_manager)
        self.dashboard_widget.set_reading_intelligence(self.reading_intelligence)
        self.left_sidebar.addTab(self.dashboard_widget, "📊 Dashboard")
        
    def _setup_goals_tab(self):
        """Set up the Goals tab"""
        self.goals_widget = GoalsMainWidget(self.db_manager)
        self.left_sidebar.addTab(self.goals_widget, "🎯 Goals")
        
    def _setup_status_bar(self):
        """Set up the enhanced status bar"""
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
        
        self.status_bar.showMessage("Ready - StudySprint Phase 2.1 Active")

    def setup_menu(self):
        """Set up the comprehensive application menu"""
        menubar = self.menuBar()
        
        # File menu
        self._setup_file_menu(menubar)
        
        # View menu
        self._setup_view_menu(menubar)
        
        # Navigation menu
        self._setup_navigation_menu(menubar)
        
        # Session menu (Phase 2)
        self._setup_session_menu(menubar)
        
        # Goals menu (Phase 2.1)
        self._setup_goals_menu(menubar)
        
        # Database menu
        self._setup_database_menu(menubar)
        
        # Help menu
        self._setup_help_menu(menubar)
        
    def _setup_file_menu(self, menubar):
        """Set up File menu"""
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
        
    def _setup_view_menu(self, menubar):
        """Set up View menu"""
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
        
        goals_action = QAction('Show &Goals', self)
        goals_action.setShortcut('Ctrl+4')
        goals_action.triggered.connect(lambda: self.left_sidebar.setCurrentIndex(3))
        view_menu.addAction(goals_action)
        
    def _setup_navigation_menu(self, menubar):
        """Set up Navigation menu"""
        nav_menu = menubar.addMenu('&Navigate')
        
        prev_page_action = QAction('&Previous Page', self)
        prev_page_action.setShortcut('Left')
        prev_page_action.triggered.connect(self.pdf_viewer.previous_page)
        nav_menu.addAction(prev_page_action)
        
        next_page_action = QAction('&Next Page', self)
        next_page_action.setShortcut('Right')
        next_page_action.triggered.connect(self.pdf_viewer.next_page)
        nav_menu.addAction(next_page_action)
        
    def _setup_session_menu(self, menubar):
        """Set up Session menu (Phase 2)"""
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
        
    def _setup_goals_menu(self, menubar):
        """Set up Goals menu (Phase 2.1)"""
        goals_menu = menubar.addMenu('&Goals')
        
        create_goal_action = QAction('&Create Goal...', self)
        create_goal_action.setShortcut('Ctrl+G')
        create_goal_action.triggered.connect(self.create_new_goal)
        goals_menu.addAction(create_goal_action)
        
        goals_menu.addSeparator()
        
        today_progress_action = QAction("Today's &Progress", self)
        today_progress_action.triggered.connect(self.show_today_progress)
        goals_menu.addAction(today_progress_action)
        
        goals_analytics_action = QAction('Goals &Analytics', self)
        goals_analytics_action.triggered.connect(self.show_goals_analytics)
        goals_menu.addAction(goals_analytics_action)
        
    def _setup_database_menu(self, menubar):
        """Set up Database menu"""
        db_menu = menubar.addMenu('&Database')
        
        stats_action = QAction('&Statistics...', self)
        stats_action.triggered.connect(self.topic_manager.show_stats)
        db_menu.addAction(stats_action)
        
        cleanup_action = QAction('&Cleanup Temp Files', self)
        cleanup_action.triggered.connect(self.cleanup_temp_files)
        db_menu.addAction(cleanup_action)
        
        health_check_action = QAction('&Health Check', self)
        health_check_action.triggered.connect(self.show_database_health)
        db_menu.addAction(health_check_action)
        
    def _setup_help_menu(self, menubar):
        """Set up Help menu"""
        help_menu = menubar.addMenu('&Help')
        
        about_action = QAction('&About StudySprint Phase 2.1', self)
        about_action.triggered.connect(self.show_about)
        help_menu.addAction(about_action)
        
        shortcuts_action = QAction('&Keyboard Shortcuts', self)
        shortcuts_action.triggered.connect(self.show_shortcuts)
        help_menu.addAction(shortcuts_action)

    def setup_connections(self):
        """Set up comprehensive signal connections"""
        logger.info("Setting up Phase 2.1 signal connections...")
        
        # PDF selection signals
        self.topic_manager.pdf_selected.connect(self.load_pdf_from_database)
        self.topic_manager.exercise_pdf_selected.connect(self.load_exercise_pdf_from_database)
        
        # PDF viewer signals
        self.pdf_viewer.page_changed.connect(self.on_page_changed)
        
        # Session timer signals (Phase 2)
        self.session_timer.session_started.connect(self.on_session_started)
        self.session_timer.session_ended.connect(self.on_session_ended)
        self.session_timer.page_changed.connect(self.on_timer_page_changed)
        
        logger.info("✅ All signal connections established")

    def start_background_tasks(self):
        """Start background timers and tasks"""
        # Auto-save page position every 5 seconds
        self.page_save_timer.timeout.connect(self.save_current_page)
        self.page_save_timer.start(5000)
        
        # Clean up temp files every 30 minutes
        self.cleanup_timer.timeout.connect(self.cleanup_temp_files)
        self.cleanup_timer.start(1800000)
        
        logger.info("✅ Background tasks started")

    def load_topics(self):
        """Initialize topic loading and database status"""
        try:
            self.topic_manager.refresh_topics()
            self.db_status_label.setText("Database: Connected")
            self.update_storage_info()
            logger.info("✅ Topics loaded successfully")
        except Exception as e:
            self.db_status_label.setText("Database: Error")
            QMessageBox.critical(self, "Database Error", f"Failed to load topics: {str(e)}")
            logger.error(f"Failed to load topics: {e}")

    def update_storage_info(self):
        """Update storage information in status bar"""
        try:
            stats = self.db_manager.get_database_stats()
            if stats:
                total_size_mb = stats['total_size'] / (1024 * 1024)
                self.storage_info_label.setText(f"💾 {stats['total_pdfs']} PDFs, {total_size_mb:.1f} MB")
            else:
                self.storage_info_label.setText("💾 Database Storage")
        except Exception as e:
            self.storage_info_label.setText("💾 Storage Error")
            logger.warning(f"Error updating storage info: {e}")

    # PDF Loading and Management
    def load_pdf_from_database(self, pdf_id):
        """Load main PDF from database with comprehensive session tracking"""
        logger.info(f"Loading PDF from database: ID {pdf_id}")
        
        try:
            # End current session before starting new one
            if self.current_session_id:
                self.session_timer.end_session()
                
            # Save current position before switching
            if self.current_pdf_id:
                self.save_current_page()
                
            # Clean up previous temporary file
            self._cleanup_current_temp_file()
                    
            # Create temporary file from database
            temp_file_path = self.db_manager.create_temp_pdf_file(pdf_id)
            
            if not temp_file_path:
                QMessageBox.critical(self, "Error", "Failed to create temporary PDF file from database")
                return
                
            logger.info(f"Temporary file created: {temp_file_path}")
            
            # Load PDF into viewer
            if self.pdf_viewer.load_pdf(temp_file_path, pdf_id):
                self.current_pdf_id = pdf_id
                self.current_temp_file = temp_file_path
                
                # Get PDF info for display and session
                pdf_info = self.db_manager.get_pdf_by_id(pdf_id)
                if pdf_info:
                    self.current_file_label.setText(f"📄 {pdf_info['title']}")
                    
                    # Start session timer (Phase 2)
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
                    logger.info(f"PDF loaded successfully, session {self.current_session_id} started")
                else:
                    self.current_file_label.setText(f"PDF ID {pdf_id}")
                    
            else:
                logger.error(f"Failed to load PDF in viewer")
                self.current_pdf_id = None
                self.current_temp_file = None
                self.current_file_label.setText("Failed to load PDF")
                self._cleanup_temp_file(temp_file_path)
                
        except Exception as e:
            logger.error(f"Error loading PDF: {e}")
            QMessageBox.critical(self, "Error", f"Failed to load PDF from database: {str(e)}")

    def load_exercise_pdf_from_database(self, exercise_id):
        """Load exercise PDF from database with session tracking"""
        logger.info(f"Loading exercise PDF from database: ID {exercise_id}")
        
        try:
            # End current session before starting new one
            if self.current_session_id:
                self.session_timer.end_session()
                
            # Save current position before switching
            if self.current_pdf_id:
                self.save_current_page()
                
            # Clean up previous temporary file
            self._cleanup_current_temp_file()
                    
            # Create temporary file from database
            temp_file_path = self.db_manager.create_temp_exercise_pdf_file(exercise_id)
            
            if not temp_file_path:
                QMessageBox.critical(self, "Error", "Failed to create temporary exercise PDF file")
                return
                
            logger.info(f"Temporary exercise file created: {temp_file_path}")
            
            # Load exercise PDF into viewer
            if self.pdf_viewer.load_pdf(temp_file_path, exercise_id, is_exercise=True):
                self.current_pdf_id = f"exercise_{exercise_id}"  # Mark as exercise
                self.current_temp_file = temp_file_path
                
                # Get exercise PDF info
                exercise_info = self.db_manager.get_exercise_pdf_by_id(exercise_id)
                if exercise_info:
                    # Get parent PDF info for topic_id
                    parent_info = self.db_manager.get_pdf_by_id(exercise_info['parent_pdf_id'])
                    parent_title = parent_info['title'] if parent_info else "Unknown"
                    topic_id = parent_info.get('topic_id') if parent_info else None
                    
                    self.current_file_label.setText(f"🏋️ {exercise_info['title']} (Exercise for: {parent_title})")
                    
                    # Start session timer for exercise (Phase 2)
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
                    logger.info(f"Exercise PDF loaded successfully, session {self.current_session_id} started")
                else:
                    self.current_file_label.setText(f"Exercise PDF ID {exercise_id}")
                    
            else:
                logger.error(f"Failed to load exercise PDF in viewer")
                self.current_pdf_id = None
                self.current_temp_file = None
                self.current_file_label.setText("Failed to load exercise PDF")
                self._cleanup_temp_file(temp_file_path)
                
        except Exception as e:
            logger.error(f"Error loading exercise PDF: {e}")
            QMessageBox.critical(self, "Error", f"Failed to load exercise PDF: {str(e)}")

    def _cleanup_current_temp_file(self):
        """Clean up the current temporary file"""
        if self.current_temp_file and os.path.exists(self.current_temp_file):
            try:
                os.unlink(self.current_temp_file)
                logger.info(f"Cleaned up previous temp file: {self.current_temp_file}")
            except Exception as e:
                logger.warning(f"Could not clean up temp file: {e}")

    def _cleanup_temp_file(self, temp_path):
        """Clean up a specific temporary file"""
        try:
            if temp_path and os.path.exists(temp_path):
                os.unlink(temp_path)
        except Exception as e:
            logger.warning(f"Could not clean up temp file {temp_path}: {e}")

    def restore_reading_position(self, pdf_id):
        """Restore the last reading position for a PDF"""
        try:
            logger.info(f"Restoring position for PDF {pdf_id}")
            pdf_info = self.db_manager.get_pdf_by_id(pdf_id)
            
            if pdf_info and pdf_info['current_page'] > 1:
                logger.info(f"Restoring to page {pdf_info['current_page']}")
                self.pdf_viewer.set_page(pdf_info['current_page'])
                
                # Notify session timer of page change
                self.session_timer.change_page(pdf_info['current_page'])
                
                self.status_bar.showMessage(f"Resumed at page {pdf_info['current_page']}", 2000)
            else:
                logger.info(f"Starting from page 1")
                self.session_timer.change_page(1)
                
        except Exception as e:
            logger.error(f"Error restoring reading position: {e}")

    def restore_exercise_reading_position(self, exercise_id):
        """Restore the last reading position for an exercise PDF"""
        try:
            logger.info(f"Restoring position for exercise PDF {exercise_id}")
            exercise_info = self.db_manager.get_exercise_pdf_by_id(exercise_id)
            
            if exercise_info and exercise_info['current_page'] > 1:
                logger.info(f"Restoring exercise to page {exercise_info['current_page']}")
                self.pdf_viewer.set_page(exercise_info['current_page'])
                
                # Notify session timer of page change
                self.session_timer.change_page(exercise_info['current_page'])
                
                self.status_bar.showMessage(f"Resumed exercise at page {exercise_info['current_page']}", 2000)
            else:
                logger.info(f"Starting exercise from page 1")
                self.session_timer.change_page(1)
                
        except Exception as e:
            logger.error(f"Error restoring exercise reading position: {e}")

    def save_current_page(self):
        """Save current page position to database (handles both main and exercise PDFs)"""
        if not self.current_pdf_id or not self.pdf_viewer.pdf_document:
            return
            
        try:
            current_page = self.pdf_viewer.get_current_page()
            
            if str(self.current_pdf_id).startswith("exercise_"):
                # This is an exercise PDF
                exercise_id = int(str(self.current_pdf_id).replace("exercise_", ""))
                logger.debug(f"Saving page {current_page} for exercise PDF {exercise_id}")
                self.db_manager.update_exercise_pdf_page(exercise_id, current_page)
            else:
                # This is a main PDF
                logger.debug(f"Saving page {current_page} for main PDF {self.current_pdf_id}")
                self.db_manager.update_pdf_page(self.current_pdf_id, current_page)
                
        except Exception as e:
            logger.error(f"Error saving page position: {e}")

    # Event Handlers
    def on_page_changed(self, page_num):
        """Handle page changes with comprehensive tracking"""
        if self.pdf_viewer.total_pages > 0:
            self.page_info_label.setText(f"Page {page_num} of {self.pdf_viewer.total_pages}")
            progress = (page_num / self.pdf_viewer.total_pages) * 100
            self.status_bar.showMessage(f"Progress: {progress:.1f}%", 1000)
            
            # Notify session timer of page change (Phase 2)
            if self.current_session_id:
                self.session_timer.change_page(page_num)

    # Session Timer Signal Handlers (Phase 2)
    def on_session_started(self, session_id):
        """Handle session started signal"""
        self.current_session_id = session_id
        self.session_status_label.setText(f"🟢 Session {session_id}")
        logger.info(f"Session {session_id} started successfully")

    def on_session_ended(self, session_id, stats):
        """Handle session ended with comprehensive cleanup and goals update"""
        self.current_session_id = None
        self.session_status_label.setText("⚫ No active session")
        
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
            self._update_goals_after_session(stats, total_time, pages_visited)
        
        logger.info(f"Session {session_id} ended")

    def on_timer_page_changed(self, session_id, old_page, new_page):
        """Handle page changes from session timer"""
        # Already handled in on_page_changed
        pass

    def _update_goals_after_session(self, stats, total_time, pages_visited):
        """Update goals after session completion"""
        try:
            if hasattr(self, 'goals_widget') and self.current_pdf_id:
                # Get topic ID from current PDF
                topic_id = self._get_current_topic_id()
                
                if topic_id:
                    self.goals_widget.update_after_session(
                        topic_id=topic_id,
                        pages_read=pages_visited,
                        time_spent_seconds=total_time
                    )
                    logger.info(f"Updated goals for topic {topic_id}")
                    
        except Exception as e:
            logger.error(f"Error updating goals after session: {e}")

    def _get_current_topic_id(self):
        """Get topic ID from current PDF"""
        try:
            if str(self.current_pdf_id).startswith("exercise_"):
                exercise_id = int(str(self.current_pdf_id).replace("exercise_", ""))
                exercise_info = self.db_manager.get_exercise_pdf_by_id(exercise_id)
                if exercise_info:
                    parent_info = self.db_manager.get_pdf_by_id(exercise_info['parent_pdf_id'])
                    return parent_info.get('topic_id') if parent_info else None
            else:
                pdf_info = self.db_manager.get_pdf_by_id(self.current_pdf_id)
                return pdf_info.get('topic_id') if pdf_info else None
        except Exception as e:
            logger.error(f"Error getting current topic ID: {e}")
            return None

    # Menu Action Handlers
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

    def create_new_goal(self):
        """Open create goal dialog"""
        try:
            topics = self.db_manager.get_all_topics()
            
            if not topics:
                QMessageBox.information(
                    self, "No Topics", 
                    "Please create at least one topic before setting goals."
                )
                return
            
            # Switch to goals tab and trigger create goal
            self.left_sidebar.setCurrentIndex(3)  # Goals tab
            self.goals_widget.create_new_goal()
            
        except Exception as e:
            logger.error(f"Error opening create goal dialog: {e}")

    def show_today_progress(self):
        """Show today's goal progress"""
        self.left_sidebar.setCurrentIndex(3)  # Switch to Goals tab
        # Goals tab will show today's progress automatically

    def show_goals_analytics(self):
        """Show goals analytics"""
        self.left_sidebar.setCurrentIndex(3)  # Switch to Goals tab
        QMessageBox.information(
            self, "Goals Analytics", 
            "Goals analytics are available in the Goals tab. Use the goal cards to view detailed analytics for each goal."
        )

    def cleanup_temp_files(self):
        """Clean up temporary files"""
        try:
            self.db_manager.cleanup_temp_files()
            self.status_bar.showMessage("Temporary files cleaned up", 2000)
        except Exception as e:
            logger.error(f"Error cleaning up temp files: {e}")

    def show_database_health(self):
        """Show database health check"""
        try:
            health = self.db_manager.health_check()
            
            if health['status'] == 'healthy':
                QMessageBox.information(
                    self, "Database Health Check", 
                    f"✅ Database is healthy\n\n"
                    f"Topics: {health['topics']}\n"
                    f"PDFs: {health['pdfs']}\n"
                    f"Connection: {health['connection']}"
                )
            else:
                QMessageBox.warning(
                    self, "Database Health Check", 
                    f"⚠️ Database health issues detected\n\n"
                    f"Status: {health['status']}\n"
                    f"Error: {health.get('error', 'Unknown')}"
                )
                
        except Exception as e:
            QMessageBox.critical(self, "Health Check Error", f"Failed to check database health: {str(e)}")

    def show_about(self):
        """Enhanced about dialog with Phase 2.1 features"""
        QMessageBox.about(
            self, "About StudySprint Phase 2.1",
            "<h3>StudySprint v2.1.0 - Phase 2.1: Complete Study Management System</h3>"
            "<p>A comprehensive PDF study management application with advanced session tracking, reading intelligence, and goal setting.</p>"
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
            "<p><b>Keyboard Shortcuts:</b></p>"
            "<ul>"
            "<li><b>Ctrl+1:</b> Show Library tab</li>"
            "<li><b>Ctrl+2:</b> Show Timer tab</li>"
            "<li><b>Ctrl+3:</b> Show Dashboard tab</li>"
            "<li><b>Ctrl+4:</b> Show Goals tab</li>"
            "<li><b>Ctrl+G:</b> Create new goal</li>"
            "<li><b>Ctrl+P:</b> Pause/Resume session</li>"
            "<li><b>Ctrl+Shift+E:</b> End current session</li>"
            "</ul>"
            "<p>Built with PyQt6, PostgreSQL, and advanced analytics algorithms</p>"
        )

    def show_shortcuts(self):
        """Show comprehensive keyboard shortcuts dialog"""
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
        <li><b>Ctrl+4:</b> Show Goals tab</li>
        </ul>
        
        <h4>📖 Navigation:</h4>
        <ul>
        <li><b>Left Arrow:</b> Previous page</li>
        <li><b>Right Arrow:</b> Next page</li>
        <li><b>Home:</b> First page</li>
        <li><b>End:</b> Last page</li>
        <li><b>F11:</b> Toggle focus mode</li>
        </ul>
        
        <h4>⏱️ Session Controls (Phase 2):</h4>
        <ul>
        <li><b>Ctrl+P:</b> Pause/Resume current session</li>
        <li><b>Ctrl+Shift+E:</b> End current session</li>
        </ul>
        
        <h4>🎯 Goals (Phase 2.1):</h4>
        <ul>
        <li><b>Ctrl+G:</b> Create new goal</li>
        <li><b>Ctrl+4:</b> Open Goals tab</li>
        </ul>
        
        <p><i>Sessions start automatically when you open a PDF and track your reading progress for goals!</i></p>
        """
        
        QMessageBox.information(self, "Keyboard Shortcuts", shortcuts_text)

    def apply_styles(self):
        """Apply comprehensive styling for Phase 2.1"""
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
                font-weight: bold;
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
        """Handle comprehensive keyboard events"""
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
            elif event.key() == Qt.Key.Key_G:
                self.create_new_goal()
        elif event.modifiers() == (Qt.KeyboardModifier.ControlModifier | Qt.KeyboardModifier.ShiftModifier):
            if event.key() == Qt.Key.Key_E:
                self.end_current_session()
        else:
            super().keyPressEvent(event)

    def closeEvent(self, event):
        """Handle application close with comprehensive cleanup"""
        logger.info("Application closing - performing cleanup...")
        
        # End current session
        if self.current_session_id:
            logger.info("Ending session before closing...")
            self.session_timer.end_session()
        
        # Save current page position
        if self.current_pdf_id:
            self.save_current_page()
            
        # Stop timers
        self.page_save_timer.stop()
        self.cleanup_timer.stop()
        
        # Clean up temporary file
        self._cleanup_current_temp_file()
        
        # Clean up all temp files
        try:
            self.db_manager.cleanup_temp_files()
        except Exception as e:
            logger.warning(f"Error cleaning temp files: {e}")
        
        # Close database connection
        try:
            self.db_manager.disconnect()
        except Exception as e:
            logger.warning(f"Error disconnecting database: {e}")
        
        # Close PDF document
        if self.pdf_viewer.pdf_document:
            try:
                self.pdf_viewer.pdf_document.close()
            except Exception as e:
                logger.warning(f"Error closing PDF document: {e}")
            
        self.status_bar.showMessage("StudySprint Phase 2.1 shutting down...", 1000)
        logger.info("✅ Application cleanup completed")
        event.accept()