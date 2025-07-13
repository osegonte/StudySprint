from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QTreeWidget, QTreeWidgetItem, 
                            QPushButton, QInputDialog, QMessageBox, QHBoxLayout,
                            QFileDialog, QLabel, QFrame, QProgressDialog, QApplication)
from PyQt6.QtCore import Qt, pyqtSignal, pyqtSlot, QThread, pyqtSlot
from PyQt6.QtGui import QFont, QBrush, QColor
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
    exercise_pdf_selected = pyqtSignal(int)  # Signal for exercise PDF selection    
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
        
        # Button layout - Two rows for better spacing
        btn_container = QVBoxLayout()
        
        # First row: Primary actions
        btn_row1 = QHBoxLayout()
        btn_row1.setSpacing(10)  # Add spacing between buttons
        
        self.add_topic_btn = QPushButton("📁 + Topic")
        self.add_topic_btn.clicked.connect(self.add_topic)
        self.add_topic_btn.setMinimumHeight(35)
        
        self.add_pdf_btn = QPushButton("📄 + PDF")
        self.add_pdf_btn.clicked.connect(self.add_pdf)
        self.add_pdf_btn.setEnabled(False)
        self.add_pdf_btn.setMinimumHeight(35)
        
        self.stats_btn = QPushButton("📊 Stats")
        self.stats_btn.clicked.connect(self.show_stats)
        self.stats_btn.setMinimumHeight(35)
        
        btn_row1.addWidget(self.add_topic_btn)
        btn_row1.addWidget(self.add_pdf_btn)
        btn_row1.addWidget(self.stats_btn)
        
        # Second row: Delete actions
        btn_row2 = QHBoxLayout()
        btn_row2.setSpacing(10)  # Add spacing between buttons
        
        self.delete_topic_btn = QPushButton("🗑️ Delete Topic")
        self.delete_topic_btn.clicked.connect(self.delete_topic)
        self.delete_topic_btn.setEnabled(False)
        self.delete_topic_btn.setMinimumHeight(35)
        
        self.delete_pdf_btn = QPushButton("🗑️ Delete PDF")
        self.delete_pdf_btn.clicked.connect(self.delete_pdf)
        self.delete_pdf_btn.setEnabled(False)
        self.delete_pdf_btn.setMinimumHeight(35)
        
        # Add stretch to center the delete buttons
        btn_row2.addWidget(self.delete_topic_btn)
        btn_row2.addWidget(self.delete_pdf_btn)
        btn_row2.addStretch()  # Push buttons to the left
        
        # Add rows to container
        btn_container.addLayout(btn_row1)
        btn_container.addLayout(btn_row2)
        btn_container.setSpacing(8)  # Space between rows
        
        # Topic tree
        self.topic_tree = QTreeWidget()
        self.topic_tree.setHeaderLabel("Topics & PDFs")
        self.topic_tree.itemDoubleClicked.connect(self.on_item_double_clicked)
        self.topic_tree.itemSelectionChanged.connect(self.on_selection_changed)
        
        # Add context menu
        self.topic_tree.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.topic_tree.customContextMenuRequested.connect(self.show_context_menu)
        
        # Status label
        self.status_label = QLabel("Ready - Create a topic to get started")
        
        layout.addWidget(self.header)
        layout.addWidget(self.stats_label)
        layout.addLayout(btn_container)
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
            padding: 8px 16px;
            border-radius: 6px;
            font-weight: bold;
            font-size: 13px;
            min-width: 100px;
            min-height: 35px;
            margin: 2px;
        }
        QPushButton:hover {
            background-color: #005a9e;
            transform: translateY(-1px);
        }
        QPushButton:pressed {
            background-color: #004080;
            transform: translateY(0px);
        }
        QPushButton:disabled {
            background-color: #cccccc;
            color: #666666;
            border: 1px solid #999999;
        }
        QPushButton[text*="Delete"] {
            background-color: #dc3545;
            border: 1px solid #c82333;
        }
        QPushButton[text*="Delete"]:hover {
            background-color: #c82333;
        }
        QPushButton[text*="Delete"]:pressed {
            background-color: #bd2130;
        }
        QPushButton[text*="Stats"] {
            background-color: #28a745;
            border: 1px solid #1e7e34;
        }
        QPushButton[text*="Stats"]:hover {
            background-color: #1e7e34;
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
                
            # Enable/disable delete buttons based on selection
            if item_data and item_data[0] == 'topic':
                self.delete_topic_btn.setEnabled(True)
                self.delete_pdf_btn.setEnabled(False)
            elif item_data and item_data[0] == 'pdf':
                self.delete_topic_btn.setEnabled(False)
                self.delete_pdf_btn.setEnabled(True)
            else:
                self.delete_topic_btn.setEnabled(False)
                self.delete_pdf_btn.setEnabled(False)
        else:
            self.delete_topic_btn.setEnabled(False)
            self.delete_pdf_btn.setEnabled(False)
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
    def show_context_menu(self, position):
        """Show context menu for right-click actions"""
        item = self.topic_tree.itemAt(position)
        if not item:
            return
        
        from PyQt6.QtWidgets import QMenu
        menu = QMenu(self)
        
        item_data = item.data(0, Qt.ItemDataRole.UserRole)
        if item_data:
            if item_data[0] == 'topic':
                # Topic context menu
                add_pdf_action = menu.addAction("📄 Add PDF to Topic")
                add_pdf_action.triggered.connect(self.add_pdf)
                
                menu.addSeparator()
                
                rename_action = menu.addAction("✏️ Rename Topic")
                rename_action.triggered.connect(self.rename_topic)
                
                delete_action = menu.addAction("🗑️ Delete Topic")
                delete_action.triggered.connect(self.delete_topic)
                
            elif item_data[0] == 'pdf':
                # PDF context menu
                view_action = menu.addAction("👁️ View PDF")
                view_action.triggered.connect(lambda: self.on_item_double_clicked(item))
                
                export_action = menu.addAction("📤 Export PDF")
                export_action.triggered.connect(lambda: self.export_pdf(item_data[1]))
                
                menu.addSeparator()
                
                delete_action = menu.addAction("🗑️ Delete PDF")
                delete_action.triggered.connect(self.delete_pdf)
        
        menu.exec(self.topic_tree.mapToGlobal(position))

    def delete_topic(self):
        """Delete the selected topic"""
        current_item = self.topic_tree.currentItem()
        if not current_item:
            return
        
        item_data = current_item.data(0, Qt.ItemDataRole.UserRole)
        if not item_data or item_data[0] != 'topic':
            QMessageBox.warning(self, "Invalid Selection", "Please select a topic to delete")
            return
        
        topic_id = item_data[1]
        topic_name = current_item.text(0)[2:]  # Remove emoji
        
        # Check if topic has PDFs
        try:
            pdfs = self.db_manager.get_pdfs_by_topic(topic_id)
            pdf_count = len(pdfs)
            
            if pdf_count > 0:
                reply = QMessageBox.question(
                    self, "Delete Topic",
                    f"Topic '{topic_name}' contains {pdf_count} PDF(s).\n\n"
                    f"Deleting this topic will permanently delete all PDFs in it.\n"
                    f"This action cannot be undone.\n\n"
                    f"Are you sure you want to delete this topic?",
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                    QMessageBox.StandardButton.No
                )
            else:
                reply = QMessageBox.question(
                    self, "Delete Topic",
                    f"Are you sure you want to delete topic '{topic_name}'?",
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                    QMessageBox.StandardButton.No
                )
            
            if reply == QMessageBox.StandardButton.Yes:
                try:
                    self.db_manager.delete_topic(topic_id)
                    self.refresh_topics()
                    self.status_label.setText(f"Deleted topic: {topic_name}")
                    print(f"Topic '{topic_name}' deleted successfully")
                    
                except Exception as e:
                    print(f"Error deleting topic: {e}")
                    QMessageBox.critical(self, "Delete Error", f"Failed to delete topic: {str(e)}")
                    
        except Exception as e:
            print(f"Error checking topic contents: {e}")
            QMessageBox.critical(self, "Error", f"Error checking topic contents: {str(e)}")

    def delete_pdf(self):
        """Delete the selected PDF"""
        current_item = self.topic_tree.currentItem()
        if not current_item:
            return
        
        item_data = current_item.data(0, Qt.ItemDataRole.UserRole)
        if not item_data or item_data[0] != 'pdf':
            QMessageBox.warning(self, "Invalid Selection", "Please select a PDF to delete")
            return
        
        pdf_id = item_data[1]
        
        try:
            # Get PDF info for confirmation
            pdf_info = self.db_manager.get_pdf_by_id(pdf_id)
            if not pdf_info:
                QMessageBox.warning(self, "PDF Not Found", "The selected PDF was not found in the database")
                return
            
            pdf_title = pdf_info['title']
            size_mb = pdf_info['file_size'] / (1024 * 1024)
            
            reply = QMessageBox.question(
                self, "Delete PDF",
                f"Are you sure you want to delete PDF:\n\n"
                f"'{pdf_title}'\n"
                f"Size: {size_mb:.1f} MB\n\n"
                f"This action cannot be undone.",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No
            )
            
            if reply == QMessageBox.StandardButton.Yes:
                try:
                    self.db_manager.delete_pdf(pdf_id)
                    self.refresh_topics()
                    self.status_label.setText(f"Deleted PDF: {pdf_title}")
                    print(f"PDF '{pdf_title}' deleted successfully")
                    
                except Exception as e:
                    print(f"Error deleting PDF: {e}")
                    QMessageBox.critical(self, "Delete Error", f"Failed to delete PDF: {str(e)}")
                    
        except Exception as e:
            print(f"Error getting PDF info: {e}")
            QMessageBox.critical(self, "Error", f"Error getting PDF information: {str(e)}")

    def rename_topic(self):
        """Rename the selected topic"""
        current_item = self.topic_tree.currentItem()
        if not current_item:
            return
        
        item_data = current_item.data(0, Qt.ItemDataRole.UserRole)
        if not item_data or item_data[0] != 'topic':
            return
        
        topic_id = item_data[1]
        current_name = current_item.text(0)[2:]  # Remove emoji
        
        from PyQt6.QtWidgets import QInputDialog
        new_name, ok = QInputDialog.getText(
            self, "Rename Topic", 
            "Enter new topic name:", 
            text=current_name
        )
        
        if ok and new_name.strip() and new_name.strip() != current_name:
            try:
                self.db_manager.rename_topic(topic_id, new_name.strip())
                self.refresh_topics()
                self.status_label.setText(f"Renamed topic to: {new_name}")
                print(f"Topic renamed from '{current_name}' to '{new_name}'")
                
            except Exception as e:
                print(f"Error renaming topic: {e}")
                QMessageBox.critical(self, "Rename Error", f"Failed to rename topic: {str(e)}")

    def get_current_topic_id(self):
        """Get the currently selected topic ID"""
        current_item = self.topic_tree.currentItem()
        if current_item:
            return self.get_topic_id_from_item(current_item)
        return None

    def refresh_topics(self):
        print("=== REFRESHING TOPICS (WITH EXERCISES) ===")
        self.topic_tree.clear()
        
        try:
            topics = self.db_manager.get_all_topics()
            print(f"Found {len(topics)} topics")
            
            for topic in topics:
                print(f"\nProcessing topic: {topic['name']} (ID: {topic['id']})")
                
                # Calculate topic progress including exercises
                topic_pdfs = self.db_manager.get_pdfs_by_topic(topic['id'])
                total_pages = sum(pdf.get('total_pages', 0) for pdf in topic_pdfs)
                read_pages = sum(pdf.get('current_page', 1) - 1 for pdf in topic_pdfs)
                progress_percent = (read_pages / total_pages * 100) if total_pages > 0 else 0
                
                # Count total exercise PDFs
                total_exercises = 0
                for pdf in topic_pdfs:
                    exercises = self.db_manager.get_exercise_pdfs_for_parent(pdf['id'])
                    total_exercises += len(exercises)
                
                # Create topic item with progress and exercise count
                exercise_text = f" ({total_exercises} exercises)" if total_exercises > 0 else ""
                topic_display = f"📁 {topic['name']} ({len(topic_pdfs)} PDFs{exercise_text}, {progress_percent:.1f}%)"
                topic_item = QTreeWidgetItem([topic_display])
                topic_item.setData(0, Qt.ItemDataRole.UserRole, ('topic', topic['id']))
                
                # Enhanced topic tooltip
                topic_tooltip = f"Topic: {topic['name']}\n"
                topic_tooltip += f"PDFs: {len(topic_pdfs)}\n"
                topic_tooltip += f"Exercise PDFs: {total_exercises}\n"
                topic_tooltip += f"Total Pages: {total_pages}\n"
                topic_tooltip += f"Pages Read: {read_pages}\n"
                topic_tooltip += f"Progress: {progress_percent:.1f}%"
                topic_item.setToolTip(0, topic_tooltip)
                
                print(f"Topic '{topic['name']}' has {len(topic_pdfs)} PDFs and {total_exercises} exercises")
                
                for pdf in topic_pdfs:
                    print(f"  Adding PDF: {pdf['title']} (ID: {pdf['id']})")
                    
                    # Calculate PDF progress
                    current_page = pdf.get('current_page', 1)
                    total_pdf_pages = pdf.get('total_pages', 1)
                    pdf_progress = ((current_page - 1) / total_pdf_pages * 100) if total_pdf_pages > 0 else 0
                    
                    # Get exercise PDFs for this parent
                    exercises = self.db_manager.get_exercise_pdfs_for_parent(pdf['id'])
                    exercise_count = len(exercises)
                    
                    # Choose status icon based on progress
                    if pdf_progress == 0:
                        status_icon = "📄"  # Not started
                    elif pdf_progress >= 100:
                        status_icon = "✅"  # Completed
                    elif pdf_progress >= 50:
                        status_icon = "📖"  # More than halfway
                    else:
                        status_icon = "📑"  # In progress
                    
                    # Check data integrity
                    size_match = pdf['file_size'] == pdf['actual_size']
                    if not size_match:
                        status_icon = "⚠️"  # Data integrity issue
                    
                    pdf_title = pdf['title']
                    if len(pdf_title) > 22:
                        pdf_title = pdf_title[:19] + "..."
                    
                    # Add exercise count to display
                    exercise_text = f" (+{exercise_count})" if exercise_count > 0 else ""
                    pdf_display = f"{status_icon} {pdf_title}{exercise_text} ({pdf_progress:.0f}%)"
                    pdf_item = QTreeWidgetItem([pdf_display])
                    
                    # Store PDF ID
                    pdf_data = ('pdf', pdf['id'])
                    pdf_item.setData(0, Qt.ItemDataRole.UserRole, pdf_data)
                    
                    # Enhanced tooltip with exercise info
                    size_mb = pdf['file_size'] / (1024 * 1024)
                    tooltip = f"Title: {pdf['title']}\n"
                    tooltip += f"File: {pdf['file_name']}\n"
                    tooltip += f"Size: {size_mb:.1f} MB\n"
                    tooltip += f"Pages: {total_pdf_pages}\n"
                    tooltip += f"Current Page: {current_page}\n"
                    tooltip += f"Progress: {pdf_progress:.1f}%\n"
                    tooltip += f"Exercise PDFs: {exercise_count}\n"
                    if current_page > 1:
                        tooltip += f"📖 Last read: Page {current_page}\n"
                    if pdf_progress >= 100:
                        tooltip += "🎉 Completed!\n"
                    elif pdf_progress > 0:
                        remaining_pages = total_pdf_pages - (current_page - 1)
                        tooltip += f"📚 {remaining_pages} pages remaining\n"
                    if not size_match:
                        tooltip += "⚠️ DATA INTEGRITY ISSUE\n"
                    tooltip += f"Hash: {pdf['content_hash'][:8]}..."
                    pdf_item.setToolTip(0, tooltip)
                    
                    # Color coding based on progress
                    if pdf_progress >= 100:
                        pdf_item.setBackground(0, QBrush(QColor(230, 255, 230)))
                    elif pdf_progress >= 50:
                        pdf_item.setBackground(0, QBrush(QColor(230, 240, 255)))
                    elif pdf_progress > 0:
                        pdf_item.setBackground(0, QBrush(QColor(255, 255, 230)))
                    
                    # Add exercise PDFs as children
                    for exercise in exercises:
                        exercise_title = exercise['title']
                        if len(exercise_title) > 25:
                            exercise_title = exercise_title[:22] + "..."
                        
                        # Exercise type icons
                        type_icons = {
                            'exercises': '🏋️',
                            'solutions': '✅',
                            'practice': '📝',
                            'homework': '📚',
                            'general': '📄'
                        }
                        exercise_icon = type_icons.get(exercise['exercise_type'], '📄')
                        
                        exercise_display = f"  {exercise_icon} {exercise_title} ({exercise['exercise_type']})"
                        exercise_item = QTreeWidgetItem([exercise_display])
                        exercise_item.setData(0, Qt.ItemDataRole.UserRole, ('exercise', exercise['id']))
                        
                        # Exercise tooltip
                        exercise_size_mb = exercise['file_size'] / (1024 * 1024)
                        exercise_tooltip = f"Exercise: {exercise['title']}\n"
                        exercise_tooltip += f"Type: {exercise['exercise_type']}\n"
                        exercise_tooltip += f"File: {exercise['file_name']}\n"
                        exercise_tooltip += f"Size: {exercise_size_mb:.1f} MB\n"
                        exercise_tooltip += f"Pages: {exercise['total_pages']}\n"
                        if exercise.get('description'):
                            exercise_tooltip += f"Description: {exercise['description']}\n"
                        exercise_tooltip += f"Parent: {pdf['title']}"
                        exercise_item.setToolTip(0, exercise_tooltip)
                        
                        # Light background for exercises
                        exercise_item.setBackground(0, QBrush(QColor(250, 250, 255)))
                        
                        pdf_item.addChild(exercise_item)
                        print(f"    Added exercise: {exercise['title']} ({exercise['exercise_type']})")
                    
                    topic_item.addChild(pdf_item)
                    
                # Expand topic if it has PDFs
                if topic_pdfs:
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

    def add_exercise_pdf(self):
        """Add an exercise PDF to the selected main PDF"""
        current_item = self.topic_tree.currentItem()
        if not current_item:
            QMessageBox.warning(self, "No Selection", "Please select a PDF to add exercises to")
            return
        
        item_data = current_item.data(0, Qt.ItemDataRole.UserRole)
        if not item_data or item_data[0] != 'pdf':
            QMessageBox.warning(self, "Invalid Selection", "Please select a main PDF to add exercises to")
            return
        
        parent_pdf_id = item_data[1]
        
        # Get parent PDF info
        try:
            parent_info = self.db_manager.get_pdf_by_id(parent_pdf_id)
            if not parent_info:
                QMessageBox.warning(self, "Error", "Could not find parent PDF information")
                return
        except Exception as e:
            QMessageBox.critical(self, "Database Error", f"Error accessing parent PDF: {str(e)}")
            return
        
        # Exercise PDF upload dialog
        from PyQt6.QtWidgets import QDialog, QVBoxLayout, QHBoxLayout, QComboBox, QTextEdit, QLineEdit
        
        dialog = QDialog(self)
        dialog.setWindowTitle(f"Add Exercise PDF to: {parent_info['title']}")
        dialog.setMinimumSize(500, 400)
        
        layout = QVBoxLayout()
        
        # Title input
        layout.addWidget(QLabel("Exercise Title:"))
        title_input = QLineEdit()
        title_input.setPlaceholderText("e.g., Chapter 5 Exercises, Problem Set 3, Solutions Manual")
        layout.addWidget(title_input)
        
        # Exercise type selection
        layout.addWidget(QLabel("Exercise Type:"))
        type_combo = QComboBox()
        type_combo.addItems([
            ("exercises", "🏋️ Exercises"),
            ("solutions", "✅ Solutions"),
            ("practice", "📝 Practice Problems"),
            ("homework", "📚 Homework"),
            ("general", "📄 General")
        ])
        type_combo.setCurrentIndex(0)
        layout.addWidget(type_combo)
        
        # Description input
        layout.addWidget(QLabel("Description (optional):"))
        desc_input = QTextEdit()
        desc_input.setMaximumHeight(80)
        desc_input.setPlaceholderText("Brief description of the exercise content...")
        layout.addWidget(desc_input)
        
        # File selection
        layout.addWidget(QLabel("Select Exercise PDF File:"))
        file_btn = QPushButton("📁 Choose Exercise PDF File...")
        selected_file = {"path": None}
        
        def select_file():
            file_path, _ = QFileDialog.getOpenFileName(
                dialog, "Select Exercise PDF", "", "PDF Files (*.pdf)"
            )
            if file_path:
                selected_file["path"] = file_path
                file_btn.setText(f"📄 {os.path.basename(file_path)}")
        
        file_btn.clicked.connect(select_file)
        layout.addWidget(file_btn)
        
        # Buttons
        btn_layout = QHBoxLayout()
        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(dialog.reject)
        
        add_btn = QPushButton("Add Exercise PDF")
        add_btn.clicked.connect(dialog.accept)
        add_btn.setStyleSheet("background-color: #28a745; color: white; font-weight: bold;")
        
        btn_layout.addWidget(cancel_btn)
        btn_layout.addWidget(add_btn)
        layout.addLayout(btn_layout)
        
        dialog.setLayout(layout)
        
        if dialog.exec() == QDialog.DialogCode.Accepted:
            if not selected_file["path"]:
                QMessageBox.warning(self, "No File", "Please select an exercise PDF file")
                return
            
            if not title_input.text().strip():
                QMessageBox.warning(self, "No Title", "Please enter a title for the exercise PDF")
                return
            
            try:
                # Add exercise PDF to database
                exercise_type = type_combo.currentData() or type_combo.currentText().split()[1].lower()
                exercise_id = self.db_manager.add_exercise_pdf(
                    parent_pdf_id=parent_pdf_id,
                    title=title_input.text().strip(),
                    file_path=selected_file["path"],
                    exercise_type=exercise_type,
                    description=desc_input.toPlainText().strip()
                )
                
                if exercise_id:
                    self.refresh_topics()
                    self.status_label.setText(f"Added exercise PDF: {title_input.text().strip()}")
                    QMessageBox.information(self, "Success", 
                                          f"Exercise PDF '{title_input.text().strip()}' added successfully!")
                else:
                    QMessageBox.warning(self, "Duplicate", "This exercise PDF already exists (duplicate content)")
                    
            except Exception as e:
                print(f"Error adding exercise PDF: {e}")
                QMessageBox.critical(self, "Error", f"Failed to add exercise PDF: {str(e)}")

    def on_item_double_clicked(self, item):
        """Handle double-click on items including exercise PDFs"""
        print("\n=== DOUBLE CLICK EVENT (WITH EXERCISES) ===")
        print(f"Item clicked: {item.text(0)}")
        
        item_data = item.data(0, Qt.ItemDataRole.UserRole)
        print(f"Item data: {item_data}")
        
        if item_data and len(item_data) >= 2:
            if item_data[0] == 'pdf':
                # Main PDF double-click
                pdf_id = item_data[1]
                print(f"Main PDF ID: {pdf_id}")
                
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
                                       
            elif item_data[0] == 'exercise':
                # Exercise PDF double-click
                exercise_id = item_data[1]
                print(f"Exercise PDF ID: {exercise_id}")
                
                try:
                    exercise_info = self.db_manager.get_exercise_pdf_by_id(exercise_id)
                    if exercise_info:
                        print(f"Exercise PDF found: {exercise_info['title']}")
                        # Emit exercise PDF signal (we'll need to add this to main window)
                        self.exercise_pdf_selected.emit(exercise_id)
                        self.status_label.setText(f"Opening exercise: {exercise_info['title']}")
                    else:
                        print(f"Exercise PDF NOT FOUND: {exercise_id}")
                        QMessageBox.warning(self, "Exercise PDF Not Found", 
                                          f"Exercise PDF with ID {exercise_id} not found in database.")
                except Exception as e:
                    print(f"Database error accessing exercise PDF {exercise_id}: {e}")
                    QMessageBox.critical(self, "Database Error", 
                                       f"Error accessing exercise PDF: {str(e)}")
        else:
            print(f"Not a PDF/exercise item or invalid data: {item_data}")
        
        print("=== END DOUBLE CLICK EVENT ===\n")

    def show_context_menu(self, position):
        """Enhanced context menu with exercise PDF options"""
        item = self.topic_tree.itemAt(position)
        if not item:
            return
        
        from PyQt6.QtWidgets import QMenu
        menu = QMenu(self)
        
        item_data = item.data(0, Qt.ItemDataRole.UserRole)
        if item_data:
            if item_data[0] == 'topic':
                # Topic context menu
                add_pdf_action = menu.addAction("📄 Add PDF to Topic")
                add_pdf_action.triggered.connect(self.add_pdf)
                
                menu.addSeparator()
                
                rename_action = menu.addAction("✏️ Rename Topic")
                rename_action.triggered.connect(self.rename_topic)
                
                delete_action = menu.addAction("🗑️ Delete Topic")
                delete_action.triggered.connect(self.delete_topic)
                
            elif item_data[0] == 'pdf':
                # Main PDF context menu
                view_action = menu.addAction("👁️ View PDF")
                view_action.triggered.connect(lambda: self.on_item_double_clicked(item))
                
                # NEW: Add exercise PDF option
                add_exercise_action = menu.addAction("🏋️ Add Exercise PDF")
                add_exercise_action.triggered.connect(self.add_exercise_pdf)
                
                export_action = menu.addAction("📤 Export PDF")
                export_action.triggered.connect(lambda: self.export_pdf(item_data[1]))
                
                menu.addSeparator()
                
                delete_action = menu.addAction("🗑️ Delete PDF")
                delete_action.triggered.connect(self.delete_pdf)
                
            elif item_data[0] == 'exercise':
                # Exercise PDF context menu
                view_action = menu.addAction("👁️ View Exercise PDF")
                view_action.triggered.connect(lambda: self.on_item_double_clicked(item))
                
                export_action = menu.addAction("📤 Export Exercise PDF")
                export_action.triggered.connect(lambda: self.export_exercise_pdf(item_data[1]))
                
                menu.addSeparator()
                
                delete_action = menu.addAction("🗑️ Delete Exercise PDF")
                delete_action.triggered.connect(lambda: self.delete_exercise_pdf_by_id(item_data[1]))
        
        menu.exec(self.topic_tree.mapToGlobal(position))

    def on_selection_changed(self):
        """Enhanced selection handling for exercise PDFs"""
        current_item = self.topic_tree.currentItem()
        has_selection = bool(current_item)
        
        # Reset all buttons
        self.add_pdf_btn.setEnabled(False)
        self.delete_topic_btn.setEnabled(False)
        self.delete_pdf_btn.setEnabled(False)
        
        if has_selection:
            item_data = current_item.data(0, Qt.ItemDataRole.UserRole)
            if item_data:
                if item_data[0] == 'topic':
                    self.add_pdf_btn.setEnabled(True)
                    self.delete_topic_btn.setEnabled(True)
                    topic_name = current_item.text(0)[2:]  # Remove emoji
                    self.status_label.setText(f"Selected topic: {topic_name}")
                    
                elif item_data[0] == 'pdf':
                    self.delete_pdf_btn.setEnabled(True)
                    pdf_id = item_data[1]
                    try:
                        pdf_info = self.db_manager.get_pdf_by_id(pdf_id)
                        if pdf_info:
                            size_mb = pdf_info['file_size'] / (1024 * 1024)
                            # Count exercises
                            exercises = self.db_manager.get_exercise_pdfs_for_parent(pdf_id)
                            exercise_text = f" (+{len(exercises)} exercises)" if exercises else ""
                            self.status_label.setText(f"Selected PDF: {pdf_info['title']} ({size_mb:.1f} MB){exercise_text}")
                        else:
                            self.status_label.setText(f"Selected PDF: ID {pdf_id}")
                    except:
                        self.status_label.setText(f"Selected PDF: ID {pdf_id}")
                        
                elif item_data[0] == 'exercise':
                    exercise_id = item_data[1]
                    try:
                        exercise_info = self.db_manager.get_exercise_pdf_by_id(exercise_id)
                        if exercise_info:
                            size_mb = exercise_info['file_size'] / (1024 * 1024)
                            self.status_label.setText(f"Selected exercise: {exercise_info['title']} ({exercise_info['exercise_type']}, {size_mb:.1f} MB)")
                        else:
                            self.status_label.setText(f"Selected exercise: ID {exercise_id}")
                    except:
                        self.status_label.setText(f"Selected exercise: ID {exercise_id}")
            else:
                self.status_label.setText("Ready - Select a topic or PDF")
        else:
            self.status_label.setText("Ready - Select a topic or PDF")

    def export_exercise_pdf(self, exercise_id):
        """Export an exercise PDF from database to file system"""
        try:
            exercise_data = self.db_manager.get_exercise_pdf_data(exercise_id)
            if not exercise_data:
                QMessageBox.warning(self, "Export Error", "Could not retrieve exercise PDF data")
                return
                
            # Ask user where to save
            suggested_name = exercise_data['file_name']
            file_path, _ = QFileDialog.getSaveFileName(
                self, "Export Exercise PDF", suggested_name, "PDF Files (*.pdf)"
            )
            
            if file_path:
                with open(file_path, 'wb') as f:
                    f.write(exercise_data['data'])
                    
                QMessageBox.information(self, "Export Complete", 
                                      f"Exercise PDF exported successfully to:\n{file_path}")
                
        except Exception as e:
            QMessageBox.critical(self, "Export Error", f"Failed to export exercise PDF: {str(e)}")

    def delete_exercise_pdf_by_id(self, exercise_id):
        """Delete an exercise PDF by ID"""
        try:
            exercise_info = self.db_manager.get_exercise_pdf_by_id(exercise_id)
            if not exercise_info:
                QMessageBox.warning(self, "Exercise PDF Not Found", "The selected exercise PDF was not found")
                return
            
            exercise_title = exercise_info['title']
            exercise_type = exercise_info['exercise_type']
            size_mb = exercise_info['file_size'] / (1024 * 1024)
            
            reply = QMessageBox.question(
                self, "Delete Exercise PDF",
                f"Are you sure you want to delete exercise PDF:\n\n"
                f"'{exercise_title}'\n"
                f"Type: {exercise_type}\n"
                f"Size: {size_mb:.1f} MB\n\n"
                f"This action cannot be undone.",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No
            )
            
            if reply == QMessageBox.StandardButton.Yes:
                try:
                    self.db_manager.delete_exercise_pdf(exercise_id)
                    self.refresh_topics()
                    self.status_label.setText(f"Deleted exercise PDF: {exercise_title}")
                    print(f"Exercise PDF '{exercise_title}' deleted successfully")
                    
                except Exception as e:
                    print(f"Error deleting exercise PDF: {e}")
                    QMessageBox.critical(self, "Delete Error", f"Failed to delete exercise PDF: {str(e)}")
                    
        except Exception as e:
            print(f"Error getting exercise PDF info: {e}")
            QMessageBox.critical(self, "Error", f"Error getting exercise PDF information: {str(e)}")
