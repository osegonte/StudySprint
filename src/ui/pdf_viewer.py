import fitz  # PyMuPDF
import os
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QScrollArea, 
                            QLabel, QPushButton, QSpinBox, QMessageBox, QFrame,
                            QSizePolicy, QProgressBar)
from PyQt6.QtCore import Qt, pyqtSignal, QThread, pyqtSlot
from PyQt6.QtGui import QPixmap, QImage, QPainter, QFont

class PDFRenderThread(QThread):
    page_rendered = pyqtSignal(int, QPixmap)
    
    def __init__(self, pdf_document, page_num, zoom=1.0):
        super().__init__()
        self.pdf_document = pdf_document
        self.page_num = page_num
        self.zoom = zoom
        
    def run(self):
        try:
            page = self.pdf_document[self.page_num]
            mat = fitz.Matrix(self.zoom, self.zoom)
            pix = page.get_pixmap(matrix=mat)
            
            img_data = pix.tobytes("ppm")
            qimg = QImage.fromData(img_data)
            pixmap = QPixmap.fromImage(qimg)
            
            self.page_rendered.emit(self.page_num, pixmap)
        except Exception as e:
            print(f"Error rendering page {self.page_num}: {e}")

class PDFViewer(QWidget):
    page_changed = pyqtSignal(int)
    
    def __init__(self):
        super().__init__()
        self.pdf_document = None
        self.current_page = 0
        self.total_pages = 0
        self.zoom_level = 1.0
        self.pdf_id = None
        self.setup_ui()
        
        # Phase 2: Add interaction tracking capability
        self.session_timer = None
        self.apply_styles()
        
    def set_session_timer(self, session_timer):
        """Set session timer for interaction tracking"""
        self.session_timer = session_timer
        
    def setup_ui(self):
        layout = QVBoxLayout()
        layout.setContentsMargins(5, 5, 5, 5)
        
        # Control bar
        control_layout = QHBoxLayout()
        
        self.prev_btn = QPushButton("◀ Previous")
        self.prev_btn.clicked.connect(self.previous_page)
        self.prev_btn.setEnabled(False)
        
        self.next_btn = QPushButton("Next ▶")
        self.next_btn.clicked.connect(self.next_page)
        self.next_btn.setEnabled(False)
        
        self.page_spinbox = QSpinBox()
        self.page_spinbox.setMinimum(1)
        self.page_spinbox.valueChanged.connect(self.goto_page)
        self.page_spinbox.setEnabled(False)
        
        self.page_label = QLabel("of 0")
        
        self.zoom_out_btn = QPushButton("Zoom -")
        self.zoom_out_btn.clicked.connect(self.zoom_out)
        self.zoom_out_btn.setEnabled(False)
        
        self.zoom_in_btn = QPushButton("Zoom +")
        self.zoom_in_btn.clicked.connect(self.zoom_in)
        self.zoom_in_btn.setEnabled(False)
        
        self.zoom_label = QLabel("100%")
        
        control_layout.addWidget(self.prev_btn)
        control_layout.addWidget(self.next_btn)
        control_layout.addWidget(QLabel("Page:"))
        control_layout.addWidget(self.page_spinbox)
        control_layout.addWidget(self.page_label)
        control_layout.addStretch()
        control_layout.addWidget(self.zoom_out_btn)
        control_layout.addWidget(self.zoom_label)
        control_layout.addWidget(self.zoom_in_btn)
        
        # PDF display area
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        self.pdf_label = QLabel("Select a PDF from the sidebar to view")
        self.pdf_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.pdf_label.setMinimumSize(400, 300)
        
        self.scroll_area.setWidget(self.pdf_label)
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        
        layout.addLayout(control_layout)
        layout.addWidget(self.progress_bar)
        layout.addWidget(self.scroll_area)
        
        self.setLayout(layout)
        
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
                border-radius: 4px;
                font-weight: bold;
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
                font-size: 14px;
            }
            QSpinBox {
                color: #000000;
                background-color: white;
                border: 1px solid #cccccc;
                padding: 4px;
                min-width: 60px;
            }
            QScrollArea {
                border: 1px solid #cccccc;
                background-color: #f5f5f5;
            }
            QProgressBar {
                border: 1px solid #cccccc;
                border-radius: 4px;
                text-align: center;
                color: #000000;
            }
            QProgressBar::chunk {
                background-color: #007acc;
                border-radius: 3px;
            }
        """)
        
        # Style the placeholder label specifically
        self.pdf_label.setStyleSheet("""
            QLabel {
                color: #666666;
                font-size: 16px;
                padding: 50px;
                border: 2px dashed #cccccc;
                border-radius: 10px;
                background-color: #f9f9f9;
            }
        """)
        
    def load_pdf(self, file_path, pdf_id=None, is_exercise=False):
        try:
            print(f"Loading PDF: {file_path}")
            
            if not os.path.exists(file_path):
                QMessageBox.warning(self, "File Not Found", f"PDF file not found: {file_path}")
                return False
                
            self.progress_bar.setVisible(True)
            self.progress_bar.setValue(0)
            
            if self.pdf_document:
                self.pdf_document.close()
                
            self.pdf_document = fitz.open(file_path)
            self.total_pages = len(self.pdf_document)
            self.pdf_id = pdf_id
            self.is_exercise = is_exercise
            
            # Update title based on context
            if is_exercise:
                print(f"Loading exercise PDF: {file_path}")
            else:
                print(f"Loading main PDF: {file_path}")
            self.current_page = 0
            
            print(f"PDF loaded successfully: {self.total_pages} pages")
            
            self.page_spinbox.setMaximum(self.total_pages)
            self.page_spinbox.setValue(1)
            self.page_spinbox.setEnabled(True)
            self.page_label.setText(f"of {self.total_pages}")
            
            self.prev_btn.setEnabled(True)
            self.next_btn.setEnabled(True)
            self.zoom_in_btn.setEnabled(True)
            self.zoom_out_btn.setEnabled(True)
            
            self.progress_bar.setValue(50)
            self.render_current_page()
            
            self.progress_bar.setValue(100)
            self.progress_bar.setVisible(False)
            
            return True
            
        except Exception as e:
            print(f"Error loading PDF: {e}")
            QMessageBox.critical(self, "Error Loading PDF", f"Failed to load PDF: {str(e)}")
            self.progress_bar.setVisible(False)
            return False
            
    def render_current_page(self):
        if not self.pdf_document:
            return
            
        print(f"Rendering page {self.current_page + 1}")
        self.render_thread = PDFRenderThread(self.pdf_document, self.current_page, self.zoom_level)
        self.render_thread.page_rendered.connect(self.on_page_rendered)
        self.render_thread.start()
        
    @pyqtSlot(int, QPixmap)
    def on_page_rendered(self, page_num, pixmap):
        if page_num == self.current_page:
            print(f"Page {page_num + 1} rendered successfully")
            
            # Clear the placeholder styling and show the PDF
            self.pdf_label.setStyleSheet("")
            self.pdf_label.setPixmap(pixmap)
            self.pdf_label.resize(pixmap.size())
            
            self.prev_btn.setEnabled(self.current_page > 0)
            self.next_btn.setEnabled(self.current_page < self.total_pages - 1)
            
            self.page_changed.emit(self.current_page + 1)
            
    def previous_page(self):
        if self.current_page > 0:
            self.current_page -= 1
            self.page_spinbox.setValue(self.current_page + 1)
            self.render_current_page()
            
            # Record interaction for session tracking
            if self.session_timer:
                self.session_timer.record_interaction()
            
    def next_page(self):
        if self.current_page < self.total_pages - 1:
            self.current_page += 1
            self.page_spinbox.setValue(self.current_page + 1)
            self.render_current_page()
            
            # Record interaction for session tracking
            if self.session_timer:
                self.session_timer.record_interaction()
            
    def goto_page(self, page_num):
        new_page = page_num - 1
        if 0 <= new_page < self.total_pages and new_page != self.current_page:
            self.current_page = new_page
            self.render_current_page()
            
            # Record interaction for session tracking
            if self.session_timer:
                self.session_timer.record_interaction()
            
    def zoom_in(self):
        if self.zoom_level < 3.0:
            self.zoom_level += 0.25
            self.zoom_label.setText(f"{int(self.zoom_level * 100)}%")
            if self.pdf_document:
                self.render_current_page()
            
            # Record interaction for session tracking
            if self.session_timer:
                self.session_timer.record_interaction()
            
    def zoom_out(self):
        if self.zoom_level > 0.5:
            self.zoom_level -= 0.25
            self.zoom_label.setText(f"{int(self.zoom_level * 100)}%")
            if self.pdf_document:
                self.render_current_page()
            
            # Record interaction for session tracking
            if self.session_timer:
                self.session_timer.record_interaction()
            
    def get_current_page(self):
        return self.current_page + 1 if self.pdf_document else 0
        
    def set_page(self, page_num):
        if self.pdf_document and 1 <= page_num <= self.total_pages:
            self.current_page = page_num - 1
            self.page_spinbox.setValue(page_num)
            self.render_current_page()
            
    def closeEvent(self, event):
        if self.pdf_document:
            self.pdf_document.close()
        event.accept()
    def mousePressEvent(self, event):
        """Handle mouse press events with interaction tracking"""
        if self.session_timer:
            self.session_timer.record_interaction()
        super().mousePressEvent(event)

    def wheelEvent(self, event):
        """Handle mouse wheel events with interaction tracking"""
        if self.session_timer:
            self.session_timer.record_interaction()
        super().wheelEvent(event)

    def keyPressEvent(self, event):
        """Handle key press events with interaction tracking"""
        if self.session_timer:
            self.session_timer.record_interaction()
        super().keyPressEvent(event)

    def mouseMoveEvent(self, event):
        """Handle mouse move events with interaction tracking"""
        if self.session_timer:
            self.session_timer.record_interaction()
        super().mouseMoveEvent(event)