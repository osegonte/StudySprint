from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QTreeWidget, QTreeWidgetItem, 
                            QPushButton, QInputDialog, QMessageBox, QHBoxLayout,
                            QFileDialog, QLabel, QFrame, QProgressDialog, QApplication)
from PyQt6.QtCore import Qt, pyqtSignal, QThread, pyqtSlot
from PyQt6.QtGui import QFont
import os
import fitz

class PDFImportThread(QThread):
    """Thread for importing PDFs without blocking UI"""
    progress_update = pyqtSignal(int, str)  # progress, status
    finished_import = pyqtSignal(bool, str)  # success, message
    
    def __init__(self, db_manager, file_paths, topic_id):
        super().__init__()
        self.db_manager = db_manager
        self.file_paths = file_paths
        self.topic_id = topic_id
        
    def run(self):
        success_count = 0
        total_files = len(self.file_paths)
        
        for i, file_path in enumerate(self.file_paths):
            try:
                # Update progress
                filename = os.path.basename(file_path)
                self.progress_update.emit(i, f"Processing: {filename}")
                
                # Validate file
                if not os.path.exists(file_path):
                    continue
                    
                if not file_path.lower().endswith('.pdf'):
                    continue
                
                # Get PDF info
                title = os.path.basename(file_path)
                
                # Count pages using PyMuPDF
                total_pages = 0
                try:
                    pdf_doc = fitz.open(file_path)
                    total_pages = len(pdf_doc)
                    pdf_doc.close()
                except Exception as e:
                    print(f"Warning: Could not read PDF pages for {filename}: {e}")
                
                # Add to database (this will read and store the file)
                pdf_id = self.db_manager.add_pdf(title, file_path, self.topic_id, total_pages)
                
                if pdf_id:
                    success_count += 1
                    
            except Exception as e:
                print(f"Error importing {filename}: {e}")
                
        # Final progress update
        self.progress_update.emit(total_files, f"Completed: {success_count}/{total_files}")
        
        if success_count > 0:
            self.finished_import.emit(True, f"Successfully imported {success_count} PDF(s)")
        else:
            self.finished_import.emit(False, "No PDFs were imported")

class TopicManager(QWidget):
    pdf_selected = pyqtSignal(int)  # Only PDF ID now, since we get data from DB
    
    def __init__(self, db_manager):
        super().__init__()
        self.db_manager = db_manager
        self.setup_ui()
        self.apply_styles()
        
        # Clean up old temp files on startup
        try:
            self.db_manager.cleanup_temp_files()
        except Exception as e:
            print(f"Error cleaning temp files: {e}")
        
    def setup_ui(self):
        layout = QVBoxLayout()
        layout.setContentsMargins(10, 10, 10, 10)
        
        # Header with stats
        self.header = QLabel("📚 StudySprint Library")
        self.header.setFont(QFont("Arial", 16, QFont.Weight.Bold))
        
        self.stats_label = QLabel("Loading...")
        self.stats_label.setFont(QFont("Arial", 10))
        
        # Button layout
        btn_layout = QHBoxLayout()
        
        self.add_topic_btn = QPushButton("+ Topic")
        self.add_topic_btn.clicked.connect(self.add_topic)
        
        self.add_pdf_btn = QPushButton("+ PDF")
        self.add_pdf_btn.clicked.connect(self.add_pdf)
        self.add_pdf_btn.setEnabled(False)
        
        self.stats_btn = QPushButton("📊 Stats")
        self.stats_btn.clicked.connect(self.show_stats)
        
        btn_layout.addWidget(self.add_topic_btn)
        btn_layout.addWidget(self.add_pdf_btn)
        btn_layout.addWidget(self.stats_btn)
        
        # Topic tree
        self.topic_tree = QTreeWidget()
        self.topic_tree.setHeaderLabel("Topics & PDFs")
        self.topic_tree.itemDoubleClicked.connect(self.on_item_double_clicked)
        self.topic_tree.itemSelectionChanged.connect(self.on_selection_changed)
        
        # Status label
        self.status_label = QLabel("Ready - Create a topic to get started")
        
        layout.addWidget(self.header)
        layout.addWidget(self.stats_label)
        layout.addLayout(btn_layout)
        layout.addWidget(self.topic_tree)
        layout.addWidget(self.status_label)
        
        self.setLayout(layout)
        self.setMinimumWidth(300)
        
    def apply_styles(self):
        """Apply proper styling for visibility"""
        self.setStyleSheet("""
            QWidget {
                background-color: #ffffff;
                color: #000000;
            }
            QPushButton {
                background-color: #007acc;
                color: white;
                border: 1px solid #005a9e;
                padding: 10px 16px;
                border-radius: 6px;
                font-weight: bold;
                font-size: 13px;
                min-width: 80px;
            }
            QPushButton:hover {
                background-color: #005a9e;
            }
            QPushButton:disabled {
                background-color: #cccccc;
                color: #666666;
                border: 1px solid #999999;
            }
            QLabel {
                color: #000000;
                padding: 5px;
            }
            QTreeWidget {
                border: 1px solid #cccccc;
                border-radius: 4px;
                background-color: white;
                color: #000000;
                font-size: 13px;
                selection-background-color: #007acc;
                selection-color: white;
            }
            QTreeWidget::item {
                padding: 8px;
                border-bottom: 1px solid #eeeeee;
            }
            QTreeWidget::item:selected {
                background-color: #007acc;
                color: white;
            }
            QTreeWidget::item:hover {
                background-color: #e6f3ff;
                color: #000000;
            }
        """)
        
    def update_stats(self):
        """Update the statistics display"""
        try:
            stats = self.db_manager.get_database_stats()
            if stats:
                total_size_mb = stats['total_size'] / (1024 * 1024)
                self.stats_label.setText(
                    f"📊 {stats['total_pdfs']} PDFs • {total_size_mb:.1f} MB stored"
                )
            else:
                self.stats_label.setText("📊 No statistics available")
        except Exception as e:
            self.stats_label.setText("📊 Error loading stats")
            print(f"Error updating stats: {e}")
        
    def refresh_topics(self):
        print("=== REFRESHING TOPICS (DATABASE STORAGE) ===")
        self.topic_tree.clear()
        
        try:
            topics = self.db_manager.get_all_topics()
            print(f"Found {len(topics)} topics")
            
            for topic in topics:
                print(f"\nProcessing topic: {topic['name']} (ID: {topic['id']})")
                topic_item = QTreeWidgetItem([f"📁 {topic['name']}"])
                topic_item.setData(0, Qt.ItemDataRole.UserRole, ('topic', topic['id']))
                
                # Add PDFs to topic
                pdfs = self.db_manager.get_pdfs_by_topic(topic['id'])
                print(f"Topic '{topic['name']}' has {len(pdfs)} PDFs")
                
                for pdf in pdfs:
                    print(f"  Adding PDF: {pdf['title']} (ID: {pdf['id']})")
                    
                    pdf_title = pdf['title']
                    if len(pdf_title) > 30:
                        pdf_title = pdf_title[:27] + "..."
                    
                    # Check data integrity
                    size_match = pdf['file_size'] == pdf['actual_size']
                    status_icon = "📄" if size_match else "⚠️"
                    
                    pdf_item = QTreeWidgetItem([f"{status_icon} {pdf_title}"])
                    
                    # Store PDF ID only - we'll get data from database when needed
                    pdf_data = ('pdf', pdf['id'])
                    pdf_item.setData(0, Qt.ItemDataRole.UserRole, pdf_data)
                    
                    print(f"    Stored data: {pdf_data}")
                    print(f"    Data integrity: {'✅ OK' if size_match else '❌ CORRUPTED'}")
                    
                    # Add tooltip with full info
                    size_mb = pdf['file_size'] / (1024 * 1024)
                    tooltip = f"Title: {pdf['title']}\nFile: {pdf['file_name']}\nSize: {size_mb:.1f} MB\nPages: {pdf.get('total_pages', 'Unknown')}"
                    if pdf.get('current_page', 1) > 1:
                        tooltip += f"\nLast read: Page {pdf['current_page']}"
                    if not size_match:
                        tooltip += "\n⚠️ DATA INTEGRITY ISSUE"
                    tooltip += f"\nHash: {pdf['content_hash'][:8]}..."
                    pdf_item.setToolTip(0, tooltip)
                    
                    topic_item.addChild(pdf_item)
                    
                # Expand topic if it has PDFs
                if pdfs:
                    topic_item.setExpanded(True)
                    
                self.topic_tree.addTopLevelItem(topic_item)
                
            self.status_label.setText(f"Loaded {len(topics)} topics")
            self.update_stats()
            print("=== REFRESH COMPLETE ===\n")
            
        except Exception as e:
            print(f"ERROR in refresh_topics: {e}")
            import traceback
            traceback.print_exc()
            QMessageBox.critical(self, "Database Error", f"Failed to load topics: {str(e)}")
            self.status_label.setText("Error loading topics")
            
    def add_topic(self):
        """Add a new topic"""
        name, ok = QInputDialog.getText(self, "Add Topic", "Enter topic name:")
        if ok and name.strip():
            try:
                print(f"Creating topic: {name}")
                topic_id = self.db_manager.create_topic(name.strip())
                self.refresh_topics()
                self.status_label.setText(f"Created topic: {name}")
                print(f"Topic created with ID: {topic_id}")
            except Exception as e:
                print(f"Error creating topic: {e}")
                QMessageBox.critical(self, "Database Error", f"Failed to create topic: {str(e)}")
                
    def add_pdf(self):
        """Add PDF to selected topic"""
        current_item = self.topic_tree.currentItem()
        if not current_item:
            QMessageBox.warning(self, "No Selection", "Please select a topic first")
            return
            
        topic_id = self.get_topic_id_from_item(current_item)
        if not topic_id:
            QMessageBox.warning(self, "Invalid Selection", "Please select a topic")
            return
            
        print(f"Adding PDF to topic ID: {topic_id}")
        
        # File dialog
        file_paths, _ = QFileDialog.getOpenFileNames(
            self, "Select PDF Files", "", "PDF Files (*.pdf);;All Files (*)"
        )
        
        if not file_paths:
            return
            
        # Show progress dialog for large imports
        if len(file_paths) > 1:
            self.import_pdfs_with_progress(file_paths, topic_id)
        else:
            # Single file import
            if self.add_single_pdf(file_paths[0], topic_id):
                self.refresh_topics()
                self.status_label.setText("PDF added successfully")
            
    def import_pdfs_with_progress(self, file_paths, topic_id):
        """Import multiple PDFs with progress dialog"""
        # Create progress dialog
        self.progress_dialog = QProgressDialog("Importing PDFs...", "Cancel", 0, len(file_paths), self)
        self.progress_dialog.setWindowModality(Qt.WindowModality.WindowModal)
        self.progress_dialog.setAutoClose(False)
        self.progress_dialog.setAutoReset(False)
        
        # Create and start import thread
        self.import_thread = PDFImportThread(self.db_manager, file_paths, topic_id)
        self.import_thread.progress_update.connect(self.on_import_progress)
        self.import_thread.finished_import.connect(self.on_import_finished)
        
        # Handle cancel button
        self.progress_dialog.canceled.connect(self.import_thread.terminate)
        
        self.import_thread.start()
        self.progress_dialog.show()
        
    @pyqtSlot(int, str)
    def on_import_progress(self, value, status):
        """Handle import progress updates"""
        self.progress_dialog.setValue(value)
        self.progress_dialog.setLabelText(status)
        QApplication.processEvents()
        
    @pyqtSlot(bool, str)
    def on_import_finished(self, success, message):
        """Handle import completion"""
        self.progress_dialog.close()
        
        if success:
            QMessageBox.information(self, "Import Complete", message)
            self.refresh_topics()
        else:
            QMessageBox.warning(self, "Import Failed", message)
            
        self.status_label.setText(message)
        
    def add_single_pdf(self, file_path, topic_id):
        """Add a single PDF to a topic"""
        try:
            print(f"Adding PDF: {file_path}")
            
            # Validate file
            if not os.path.exists(file_path):
                QMessageBox.warning(self, "File Not Found", f"File not found: {file_path}")
                return False
                
            if not file_path.lower().endswith('.pdf'):
                QMessageBox.warning(self, "Invalid File", "Please select a PDF file")
                return False
            
            # Get PDF info
            title = os.path.basename(file_path)
            
            # Count pages using PyMuPDF
            total_pages = 0
            try:
                pdf_doc = fitz.open(file_path)
                total_pages = len(pdf_doc)
                pdf_doc.close()
                print(f"PDF has {total_pages} pages")
            except Exception as e:
                print(f"Warning: Could not read PDF pages: {e}")
                QMessageBox.warning(self, "PDF Error", f"Could not read PDF: {e}\nAdding anyway...")
                
            # Add to database (this will read and store the entire file)
            pdf_id = self.db_manager.add_pdf(title, file_path, topic_id, total_pages)
            
            if pdf_id:
                print(f"PDF added to database with ID: {pdf_id}")
                return True
            else:
                print(f"Failed to add PDF to database")
                return False
            
        except Exception as e:
            print(f"Error adding PDF: {e}")
            import traceback
            traceback.print_exc()
            QMessageBox.critical(self, "Error Adding PDF", 
                               f"Failed to add {os.path.basename(file_path)}:\n{str(e)}")
            return False
            
    def get_topic_id_from_item(self, item):
        """Get topic ID from tree item"""
        if not item:
            return None
            
        item_data = item.data(0, Qt.ItemDataRole.UserRole)
        if not item_data:
            return None
            
        if item_data[0] == 'topic':
            return item_data[1]
        elif item_data[0] == 'pdf' and item.parent():
            parent_data = item.parent().data(0, Qt.ItemDataRole.UserRole)
            return parent_data[1] if parent_data else None
        return None
        
    def on_selection_changed(self):
        """Handle selection changes"""
        current_item = self.topic_tree.currentItem()
        has_selection = bool(current_item)
        self.add_pdf_btn.setEnabled(has_selection)
        
        if has_selection:
            item_data = current_item.data(0, Qt.ItemDataRole.UserRole)
            if item_data and item_data[0] == 'topic':
                self.status_label.setText(f"Selected topic: {current_item.text(0)[2:]}")  # Remove emoji
            elif item_data and item_data[0] == 'pdf':
                pdf_id = item_data[1]
                try:
                    pdf_info = self.db_manager.get_pdf_by_id(pdf_id)
                    if pdf_info:
                        size_mb = pdf_info['file_size'] / (1024 * 1024)
                        self.status_label.setText(f"Selected PDF: {pdf_info['title']} ({size_mb:.1f} MB)")
                    else:
                        self.status_label.setText(f"Selected PDF: ID {pdf_id} (error loading info)")
                except Exception as e:
                    self.status_label.setText(f"Selected PDF: ID {pdf_id} (database error)")
                
    def on_item_double_clicked(self, item):
        """Handle double-click on items"""
        print("\n=== DOUBLE CLICK EVENT (DATABASE STORAGE) ===")
        print(f"Item clicked: {item.text(0)}")
        
        item_data = item.data(0, Qt.ItemDataRole.UserRole)
        print(f"Item data: {item_data}")
        
        if item_data and len(item_data) >= 2 and item_data[0] == 'pdf':
            pdf_id = item_data[1]
            print(f"PDF ID: {pdf_id}")
            
            # Check if PDF exists in database
            try:
                pdf_info = self.db_manager.get_pdf_by_id(pdf_id)
                if pdf_info:
                    print(f"PDF found in database: {pdf_info['title']}")
                    print(f"EMITTING SIGNAL: pdf_selected({pdf_id})")
                    self.pdf_selected.emit(pdf_id)
                    self.status_label.setText(f"Opening: {pdf_info['title']}")
                else:
                    print(f"PDF NOT FOUND in database: {pdf_id}")
                    QMessageBox.warning(self, "PDF Not Found", 
                                      f"PDF with ID {pdf_id} not found in database.")
            except Exception as e:
                print(f"Database error accessing PDF {pdf_id}: {e}")
                QMessageBox.critical(self, "Database Error", 
                                   f"Error accessing PDF: {str(e)}")
        else:
            print(f"Not a PDF item or invalid data: {item_data}")
        
        print("=== END DOUBLE CLICK EVENT ===\n")
        
    def show_stats(self):
        """Show database statistics"""
        try:
            stats = self.db_manager.get_database_stats()
            if not stats:
                QMessageBox.warning(self, "Statistics", "Could not load statistics")
                return
                
            total_size_mb = stats['total_size'] / (1024 * 1024)
            avg_size_mb = stats['avg_size'] / (1024 * 1024) if stats['avg_size'] else 0
            max_size_mb = stats['max_size'] / (1024 * 1024) if stats['max_size'] else 0
            
            stats_text = f"""
            <h3>📊 StudySprint Database Statistics</h3>
            
            <h4>Overall Storage:</h4>
            <ul>
            <li><b>Total PDFs:</b> {stats['total_pdfs']}</li>
            <li><b>Total Size:</b> {total_size_mb:.1f} MB</li>
            <li><b>Average Size:</b> {avg_size_mb:.1f} MB</li>
            <li><b>Largest PDF:</b> {max_size_mb:.1f} MB</li>
            </ul>
            
            <h4>By Topic:</h4>
            <ul>
            """
            
            for topic in stats['topics']:
                topic_size_mb = (topic['topic_size'] or 0) / (1024 * 1024)
                stats_text += f"<li><b>{topic['topic_name']}:</b> {topic['pdf_count']} PDFs, {topic_size_mb:.1f} MB</li>"
            
            stats_text += """
            </ul>
            
            <p><i>All PDFs are stored securely in the database with integrity verification.</i></p>
            """
            
            QMessageBox.information(self, "Database Statistics", stats_text)
            
        except Exception as e:
            QMessageBox.critical(self, "Statistics Error", f"Error loading statistics: {str(e)}")
            
    def export_pdf(self, pdf_id):
        """Export a PDF from database to file system"""
        try:
            pdf_data = self.db_manager.get_pdf_data(pdf_id)
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
                
        except Exception as e:
            QMessageBox.critical(self, "Export Error", f"Failed to export PDF: {str(e)}")