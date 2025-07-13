import os
import tempfile
import logging
from PyQt6.QtWidgets import (QMainWindow, QWidget, QHBoxLayout, QVBoxLayout, 
                            QSplitter, QLabel, QPushButton, QFileDialog, 
                            QMessageBox, QStatusBar, QMenuBar, QApplication,
                            QInputDialog)
from PyQt6.QtCore import Qt, pyqtSignal, QTimer
from PyQt6.QtGui import QAction, QFont, QDragEnterEvent, QDropEvent, QKeySequence

from database.db_manager import DatabaseManager
from ui.pdf_viewer import PDFViewer
from ui.topic_manager import TopicManager

logger = logging.getLogger(__name__)

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.db_manager = DatabaseManager()
        self.current_pdf_id = None
        self.current_temp_file = None
        self.temp_files_created = []
        
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
        """Set up the main user interface"""
        self.setWindowTitle("StudySprint - Professional PDF Study Manager")
        self.setMinimumSize(1200, 800)
        self.resize(1400, 900)
        
        # Create central widget and main layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # Create main splitter
        self.main_splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # Left sidebar for topics
        self.topic_manager = TopicManager(self.db_manager)
        self.topic_manager.setMaximumWidth(400)
        self.topic_manager.setMinimumWidth(250)
        
        # Right side for PDF viewer
        self.pdf_viewer = PDFViewer()
        
        # Add widgets to splitter
        self.main_splitter.addWidget(self.topic_manager)
        self.main_splitter.addWidget(self.pdf_viewer)
        
        # Set splitter proportions (25% sidebar, 75% viewer)
        self.main_splitter.setSizes([300, 1100])
        
        # Create main layout
        layout = QHBoxLayout()
        layout.addWidget(self.main_splitter)
        layout.setContentsMargins(5, 5, 5, 5)
        
        central_widget.setLayout(layout)
        
        # Status bar
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        
        # Status bar widgets
        self.current_file_label = QLabel("No PDF loaded")
        self.status_bar.addWidget(self.current_file_label)
        
        self.page_info_label = QLabel("")
        self.status_bar.addPermanentWidget(self.page_info_label)
        
        self.storage_info_label = QLabel("Database Storage")
        self.status_bar.addPermanentWidget(self.storage_info_label)
        
        self.db_status_label = QLabel("Database: Connected")
        self.status_bar.addPermanentWidget(self.db_status_label)
        
        self.status_bar.showMessage("Ready - PDFs are stored securely in database")
        
    def setup_menu(self):
        """Set up the application menu"""
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
        
        about_action = QAction('&About StudySprint', self)
        about_action.triggered.connect(self.show_about)
        help_menu.addAction(about_action)
        
    def setup_connections(self):
        """Set up signal connections"""
        print("Setting up signal connections...")
        
        # Connect the PDF selection signal
        self.topic_manager.pdf_selected.connect(self.load_pdf_from_database)
        print("Connected pdf_selected signal")
        
        # Connect page change signal
        self.pdf_viewer.page_changed.connect(self.on_page_changed)
        print("Connected page_changed signal")
        
    def start_background_tasks(self):
        """Start background timers"""
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
        """Load PDF from database and display in viewer"""
        print(f"\n=== LOADING PDF FROM DATABASE ===")
        print(f"PDF ID: {pdf_id}")
        
        try:
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
                    
                    # Restore reading position
                    self.restore_reading_position(pdf_id)
                    
                    self.status_bar.showMessage(f"Opened {pdf_info['title']} from database", 3000)
                    print(f"PDF loaded successfully")
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
            
    def restore_reading_position(self, pdf_id):
        """Restore the last reading position for a PDF"""
        try:
            print(f"Restoring position for PDF {pdf_id}")
            pdf_info = self.db_manager.get_pdf_by_id(pdf_id)
            
            if pdf_info and pdf_info['current_page'] > 1:
                print(f"Restoring to page {pdf_info['current_page']}")
                self.pdf_viewer.set_page(pdf_info['current_page'])
                self.status_bar.showMessage(f"Resumed at page {pdf_info['current_page']}", 2000)
            else:
                print(f"No saved position or starting from page 1")
                
        except Exception as e:
            print(f"Error restoring reading position: {e}")
            
    def save_current_page(self):
        """Save current page position to database"""
        if not self.current_pdf_id or not self.pdf_viewer.pdf_document:
            return
            
        try:
            current_page = self.pdf_viewer.get_current_page()
            print(f"Saving page {current_page} for PDF {self.current_pdf_id}")
            self.db_manager.update_pdf_page(self.current_pdf_id, current_page)
        except Exception as e:
            print(f"Error saving page position: {e}")
            
    def on_page_changed(self, page_num):
        """Handle page changes"""
        if self.pdf_viewer.total_pages > 0:
            self.page_info_label.setText(f"Page {page_num} of {self.pdf_viewer.total_pages}")
            progress = (page_num / self.pdf_viewer.total_pages) * 100
            self.status_bar.showMessage(f"Progress: {progress:.1f}%", 1000)
            
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
        """Export the currently viewed PDF"""
        if not self.current_pdf_id:
            QMessageBox.information(self, "No PDF", "No PDF is currently loaded")
            return
            
        try:
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
        """Show about dialog"""
        QMessageBox.about(
            self, "About StudySprint Enhanced",
            "<h3>StudySprint v1.0.0 Enhanced</h3>"
            "<p>A powerful PDF study management application with database storage.</p>"
            "<p><b>Enhanced Features:</b></p>"
            "<ul>"
            "<li>✅ Complete PDF storage in database</li>"
            "<li>✅ Data integrity verification</li>"
            "<li>✅ Duplicate detection</li>"
            "<li>✅ Automatic reading position saving</li>"
            "<li>✅ Full PDF viewing with zoom and navigation</li>"
            "<li>✅ Database statistics and management</li>"
            "</ul>"
            "<p><b>How to use:</b></p>"
            "<ol>"
            "<li>Create a topic (+ Topic button)</li>"
            "<li>Select the topic</li>"
            "<li>Add PDFs (+ PDF button) - they'll be stored in database</li>"
            "<li>Double-click any PDF to view it</li>"
            "<li>Use Database → Statistics to see storage info</li>"
            "</ol>"
            "<p><b>Storage:</b> All PDFs are stored securely in PostgreSQL with SHA-256 verification.</p>"
            "<p>Built with PyQt6 and PostgreSQL</p>"
        )
        
    def apply_styles(self):
        """Apply consistent styling"""
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
        """)
        
    def keyPressEvent(self, event):
        """Handle keyboard events"""
        if event.key() == Qt.Key.Key_Left:
            self.pdf_viewer.previous_page()
        elif event.key() == Qt.Key.Key_Right:
            self.pdf_viewer.next_page()
        elif event.key() == Qt.Key.Key_Escape:
            self.topic_manager.topic_tree.clearSelection()
        else:
            super().keyPressEvent(event)
            
    def closeEvent(self, event):
        """Handle application close"""
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
            
        self.status_bar.showMessage("Shutting down...", 1000)
        event.accept()