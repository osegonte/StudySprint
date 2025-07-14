# src/ui/pdf_viewer.py - Enhanced with Complete Timer Integration
import fitz  # PyMuPDF
import os
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QScrollArea, 
                            QLabel, QPushButton, QSpinBox, QMessageBox, QFrame,
                            QSizePolicy, QProgressBar, QToolBar, QSlider, QCheckBox,
                            QApplication)
from PyQt6.QtCore import Qt, pyqtSignal, QThread, pyqtSlot, QTimer, QElapsedTimer
from PyQt6.QtGui import QPixmap, QImage, QPainter, QFont, QKeySequence, QShortcut

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
    """Enhanced PDF viewer with comprehensive timer integration and interaction tracking"""
    
    page_changed = pyqtSignal(int)
    interaction_detected = pyqtSignal(str, dict)  # interaction_type, metadata
    focus_changed = pyqtSignal(bool)  # has_focus
    
    def __init__(self):
        super().__init__()
        self.pdf_document = None
        self.current_page = 0
        self.total_pages = 0
        self.zoom_level = 1.0
        self.pdf_id = None
        self.is_exercise = False
        
        # Timer integration
        self.session_timer = None
        self.interaction_timer = QTimer()
        self.interaction_timer.timeout.connect(self._record_periodic_interaction)
        self.interaction_timer.start(30000)  # Record activity every 30 seconds
        
        # Interaction tracking
        self.last_interaction_time = QElapsedTimer()
        self.interaction_count = 0
        self.scroll_interactions = 0
        self.click_interactions = 0
        self.keyboard_interactions = 0
        
        # Focus and window state monitoring
        self.focus_timer = QTimer()
        self.focus_timer.timeout.connect(self._check_focus_state)
        self.focus_timer.start(1000)  # Check focus every second
        self.was_focused = False
        
        # Auto-save timer for current page
        self.auto_save_timer = QTimer()
        self.auto_save_timer.timeout.connect(self._auto_save_position)
        self.auto_save_timer.start(10000)  # Auto-save every 10 seconds
        
        self.setup_ui()
        self.setup_shortcuts()
        self.apply_styles()
        
        # Enable interaction tracking
        self.setFocusPolicy(Qt.FocusPolicy.StrongFocus)
        self.setMouseTracking(True)
        
    def setup_ui(self):
        """Set up enhanced UI with timer-aware controls"""
        layout = QVBoxLayout()
        layout.setContentsMargins(5, 5, 5, 5)
        
        # Enhanced toolbar with timer info
        self.toolbar = QToolBar()
        self.toolbar.setMovable(False)
        
        # Navigation controls
        self.prev_btn = QPushButton("◀ Previous")
        self.prev_btn.clicked.connect(self.previous_page)
        self.prev_btn.setEnabled(False)
        self.prev_btn.setToolTip("Previous page (Left Arrow)")
        
        self.next_btn = QPushButton("Next ▶")
        self.next_btn.clicked.connect(self.next_page)
        self.next_btn.setEnabled(False)
        self.next_btn.setToolTip("Next page (Right Arrow)")
        
        self.page_spinbox = QSpinBox()
        self.page_spinbox.setMinimum(1)
        self.page_spinbox.valueChanged.connect(self.goto_page)
        self.page_spinbox.setEnabled(False)
        self.page_spinbox.setToolTip("Go to specific page")
        
        self.page_label = QLabel("of 0")
        
        # Zoom controls
        self.zoom_out_btn = QPushButton("🔍-")
        self.zoom_out_btn.clicked.connect(self.zoom_out)
        self.zoom_out_btn.setEnabled(False)
        self.zoom_out_btn.setToolTip("Zoom out (Ctrl+-)")
        
        self.zoom_slider = QSlider(Qt.Orientation.Horizontal)
        self.zoom_slider.setMinimum(50)  # 50%
        self.zoom_slider.setMaximum(300)  # 300%
        self.zoom_slider.setValue(100)
        self.zoom_slider.setMaximumWidth(100)
        self.zoom_slider.valueChanged.connect(self.set_zoom_from_slider)
        self.zoom_slider.setEnabled(False)
        self.zoom_slider.setToolTip("Zoom level")
        
        self.zoom_in_btn = QPushButton("🔍+")
        self.zoom_in_btn.clicked.connect(self.zoom_in)
        self.zoom_in_btn.setEnabled(False)
        self.zoom_in_btn.setToolTip("Zoom in (Ctrl++)")
        
        self.zoom_label = QLabel("100%")
        
        # Timer-aware features
        self.reading_mode_cb = QCheckBox("📖 Focus Mode")
        self.reading_mode_cb.stateChanged.connect(self.toggle_reading_mode)
        self.reading_mode_cb.setToolTip("Hide UI elements for distraction-free reading")
        
        self.track_interactions_cb = QCheckBox("📊 Track Interactions")
        self.track_interactions_cb.setChecked(True)
        self.track_interactions_cb.stateChanged.connect(self.toggle_interaction_tracking)
        self.track_interactions_cb.setToolTip("Track reading interactions for session analytics")
        
        # Add controls to toolbar
        self.toolbar.addWidget(self.prev_btn)
        self.toolbar.addWidget(self.next_btn)
        self.toolbar.addSeparator()
        self.toolbar.addWidget(QLabel("Page:"))
        self.toolbar.addWidget(self.page_spinbox)
        self.toolbar.addWidget(self.page_label)
        self.toolbar.addSeparator()
        self.toolbar.addWidget(self.zoom_out_btn)
        self.toolbar.addWidget(self.zoom_slider)
        self.toolbar.addWidget(self.zoom_in_btn)
        self.toolbar.addWidget(self.zoom_label)
        self.toolbar.addSeparator()
        self.toolbar.addWidget(self.reading_mode_cb)
        self.toolbar.addWidget(self.track_interactions_cb)
        
        # PDF display area with enhanced scroll tracking
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.scroll_area.verticalScrollBar().valueChanged.connect(self._on_scroll)
        self.scroll_area.horizontalScrollBar().valueChanged.connect(self._on_scroll)
        
        self.pdf_label = QLabel("Select a PDF from the sidebar to view")
        self.pdf_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.pdf_label.setMinimumSize(400, 300)
        
        self.scroll_area.setWidget(self.pdf_label)
        
        # Enhanced progress bar with time estimation
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setFormat("Loading... %p%")
        
        # Status bar with interaction info
        self.status_layout = QHBoxLayout()
        self.interaction_label = QLabel("Interactions: 0")
        self.focus_label = QLabel("📍 Focused")
        self.reading_time_label = QLabel("Reading time: 00:00")
        
        self.status_layout.addWidget(self.interaction_label)
        self.status_layout.addStretch()
        self.status_layout.addWidget(self.reading_time_label)
        self.status_layout.addWidget(self.focus_label)
        
        layout.addWidget(self.toolbar)
        layout.addWidget(self.progress_bar)
        layout.addWidget(self.scroll_area)
        layout.addLayout(self.status_layout)
        
        self.setLayout(layout)
        
    def setup_shortcuts(self):
        """Set up keyboard shortcuts for enhanced navigation"""
        # Page navigation
        QShortcut(QKeySequence(Qt.Key.Key_Left), self, self.previous_page)
        QShortcut(QKeySequence(Qt.Key.Key_Right), self, self.next_page)
        QShortcut(QKeySequence(Qt.Key.Key_Home), self, self.go_to_first_page)
        QShortcut(QKeySequence(Qt.Key.Key_End), self, self.go_to_last_page)
        QShortcut(QKeySequence(Qt.Key.Key_PageUp), self, self.previous_page)
        QShortcut(QKeySequence(Qt.Key.Key_PageDown), self, self.next_page)
        
        # Zoom shortcuts
        QShortcut(QKeySequence("Ctrl++"), self, self.zoom_in)
        QShortcut(QKeySequence("Ctrl+-"), self, self.zoom_out)
        QShortcut(QKeySequence("Ctrl+0"), self, self.reset_zoom)
        
        # Focus mode
        QShortcut(QKeySequence("F11"), self, self.toggle_reading_mode)
        
        # Quick page jumps
        for i in range(1, 10):
            QShortcut(QKeySequence(f"Ctrl+{i}"), self, lambda checked=False, page=i: self.quick_jump_percent(page * 10))
        
    def apply_styles(self):
        """Apply enhanced styling with focus indicators"""
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
                min-height: 30px;
            }
            QPushButton:hover {
                background-color: #005a9e;
                transform: translateY(-1px);
            }
            QPushButton:pressed {
                background-color: #004080;
            }
            QPushButton:disabled {
                background-color: #cccccc;
                color: #666666;
                border: 1px solid #999999;
            }
            QLabel {
                color: #000000;
                font-size: 14px;
                padding: 2px;
            }
            QSpinBox {
                color: #000000;
                background-color: white;
                border: 1px solid #cccccc;
                padding: 4px;
                min-width: 60px;
                border-radius: 3px;
            }
            QScrollArea {
                border: 2px solid #cccccc;
                border-radius: 4px;
                background-color: #f5f5f5;
            }
            QScrollArea:focus {
                border-color: #007acc;
                background-color: #f9f9f9;
            }
            QProgressBar {
                border: 1px solid #cccccc;
                border-radius: 4px;
                text-align: center;
                color: #000000;
                font-weight: bold;
            }
            QProgressBar::chunk {
                background-color: #007acc;
                border-radius: 3px;
            }
            QToolBar {
                border: none;
                background-color: #f8f9fa;
                padding: 5px;
                spacing: 5px;
            }
            QSlider::groove:horizontal {
                border: 1px solid #999999;
                height: 8px;
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #B1B1B1, stop:1 #c4c4c4);
                margin: 2px 0;
                border-radius: 4px;
            }
            QSlider::handle:horizontal {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #b4b4b4, stop:1 #8f8f8f);
                border: 1px solid #5c5c5c;
                width: 18px;
                margin: -2px 0;
                border-radius: 9px;
            }
            QCheckBox {
                color: #333333;
                font-weight: bold;
            }
            QCheckBox::indicator:checked {
                background-color: #007acc;
                border: 1px solid #005a9e;
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
        
    def set_session_timer(self, session_timer):
        """Set session timer for comprehensive interaction tracking"""
        self.session_timer = session_timer
        
    def load_pdf(self, file_path, pdf_id=None, is_exercise=False):
        """Load PDF with enhanced timer integration"""
        try:
            print(f"📖 Loading PDF: {file_path}")
            
            if not os.path.exists(file_path):
                QMessageBox.warning(self, "File Not Found", f"PDF file not found: {file_path}")
                return False
                
            self.progress_bar.setVisible(True)
            self.progress_bar.setValue(0)
            self.progress_bar.setFormat("Initializing PDF...")
            
            # Close previous document
            if self.pdf_document:
                self.pdf_document.close()
                
            # Reset interaction tracking
            self.interaction_count = 0
            self.scroll_interactions = 0
            self.click_interactions = 0
            self.keyboard_interactions = 0
            self.last_interaction_time.start()
            
            self.pdf_document = fitz.open(file_path)
            self.total_pages = len(self.pdf_document)
            self.pdf_id = pdf_id
            self.is_exercise = is_exercise
            
            print(f"✅ PDF loaded: {self.total_pages} pages, Exercise: {is_exercise}")
            
            self.current_page = 0
            
            # Update UI controls
            self.page_spinbox.setMaximum(self.total_pages)
            self.page_spinbox.setValue(1)
            self.page_spinbox.setEnabled(True)
            self.page_label.setText(f"of {self.total_pages}")
            
            # Enable controls
            self.prev_btn.setEnabled(True)
            self.next_btn.setEnabled(True)
            self.zoom_in_btn.setEnabled(True)
            self.zoom_out_btn.setEnabled(True)
            self.zoom_slider.setEnabled(True)
            
            self.progress_bar.setValue(50)
            self.progress_bar.setFormat("Rendering first page...")
            
            # Render first page
            self.render_current_page()
            
            self.progress_bar.setValue(100)
            self.progress_bar.setFormat("PDF loaded successfully!")
            
            # Hide progress bar after delay
            QTimer.singleShot(2000, lambda: self.progress_bar.setVisible(False))
            
            # Record initial load interaction
            self._record_interaction("pdf_loaded", {
                'pdf_id': pdf_id,
                'is_exercise': is_exercise,
                'total_pages': self.total_pages,
                'file_size': os.path.getsize(file_path)
            })
            
            return True
            
        except Exception as e:
            print(f"❌ Error loading PDF: {e}")
            QMessageBox.critical(self, "Error Loading PDF", f"Failed to load PDF: {str(e)}")
            self.progress_bar.setVisible(False)
            return False
            
    def render_current_page(self):
        """Render current page with interaction tracking"""
        if not self.pdf_document:
            return
            
        print(f"🎨 Rendering page {self.current_page + 1}")
        
        # Record page render as interaction
        self._record_interaction("page_render", {
            'page': self.current_page + 1,
            'zoom_level': self.zoom_level
        })
        
        self.render_thread = PDFRenderThread(self.pdf_document, self.current_page, self.zoom_level)
        self.render_thread.page_rendered.connect(self.on_page_rendered)
        self.render_thread.start()
        
    @pyqtSlot(int, QPixmap)
    def on_page_rendered(self, page_num, pixmap):
        """Handle page rendered with enhanced feedback"""
        if page_num == self.current_page:
            print(f"✅ Page {page_num + 1} rendered successfully")
            
            # Clear placeholder styling and show PDF
            self.pdf_label.setStyleSheet("")
            self.pdf_label.setPixmap(pixmap)
            self.pdf_label.resize(pixmap.size())
            
            # Update navigation buttons
            self.prev_btn.setEnabled(self.current_page > 0)
            self.next_btn.setEnabled(self.current_page < self.total_pages - 1)
            
            # Emit page change signal
            self.page_changed.emit(self.current_page + 1)
            
            # Update interaction status
            self._update_interaction_display()
            
    def previous_page(self):
        """Navigate to previous page with interaction tracking"""
        if self.current_page > 0:
            old_page = self.current_page
            self.current_page -= 1
            self.page_spinbox.setValue(self.current_page + 1)
            
            self._record_interaction("page_navigation", {
                'direction': 'previous',
                'from_page': old_page + 1,
                'to_page': self.current_page + 1,
                'method': 'button'
            })
            
            self.render_current_page()
            
    def next_page(self):
        """Navigate to next page with interaction tracking"""
        if self.current_page < self.total_pages - 1:
            old_page = self.current_page
            self.current_page += 1
            self.page_spinbox.setValue(self.current_page + 1)
            
            self._record_interaction("page_navigation", {
                'direction': 'next',
                'from_page': old_page + 1,
                'to_page': self.current_page + 1,
                'method': 'button'
            })
            
            self.render_current_page()
            
    def goto_page(self, page_num):
        """Go to specific page with interaction tracking"""
        new_page = page_num - 1
        if 0 <= new_page < self.total_pages and new_page != self.current_page:
            old_page = self.current_page
            self.current_page = new_page
            
            self._record_interaction("page_navigation", {
                'direction': 'direct',
                'from_page': old_page + 1,
                'to_page': self.current_page + 1,
                'method': 'spinbox'
            })
            
            self.render_current_page()
            
    def go_to_first_page(self):
        """Go to first page"""
        if self.current_page != 0:
            old_page = self.current_page
            self.current_page = 0
            self.page_spinbox.setValue(1)
            
            self._record_interaction("page_navigation", {
                'direction': 'first',
                'from_page': old_page + 1,
                'to_page': 1,
                'method': 'shortcut'
            })
            
            self.render_current_page()
            
    def go_to_last_page(self):
        """Go to last page"""
        if self.current_page != self.total_pages - 1:
            old_page = self.current_page
            self.current_page = self.total_pages - 1
            self.page_spinbox.setValue(self.total_pages)
            
            self._record_interaction("page_navigation", {
                'direction': 'last',
                'from_page': old_page + 1,
                'to_page': self.total_pages,
                'method': 'shortcut'
            })
            
            self.render_current_page()
            
    def quick_jump_percent(self, percent):
        """Jump to percentage of document"""
        if self.total_pages > 0:
            target_page = min(self.total_pages - 1, int((percent / 100) * self.total_pages))
            if target_page != self.current_page:
                old_page = self.current_page
                self.current_page = target_page
                self.page_spinbox.setValue(target_page + 1)
                
                self._record_interaction("page_navigation", {
                    'direction': 'percent_jump',
                    'from_page': old_page + 1,
                    'to_page': target_page + 1,
                    'target_percent': percent,
                    'method': 'shortcut'
                })
                
                self.render_current_page()
            
    def zoom_in(self):
        """Zoom in with interaction tracking"""
        if self.zoom_level < 3.0:
            old_zoom = self.zoom_level
            self.zoom_level += 0.25
            self.zoom_slider.setValue(int(self.zoom_level * 100))
            self.zoom_label.setText(f"{int(self.zoom_level * 100)}%")
            
            self._record_interaction("zoom_change", {
                'direction': 'in',
                'from_zoom': old_zoom,
                'to_zoom': self.zoom_level,
                'method': 'button'
            })
            
            if self.pdf_document:
                self.render_current_page()
            
    def zoom_out(self):
        """Zoom out with interaction tracking"""
        if self.zoom_level > 0.5:
            old_zoom = self.zoom_level
            self.zoom_level -= 0.25
            self.zoom_slider.setValue(int(self.zoom_level * 100))
            self.zoom_label.setText(f"{int(self.zoom_level * 100)}%")
            
            self._record_interaction("zoom_change", {
                'direction': 'out',
                'from_zoom': old_zoom,
                'to_zoom': self.zoom_level,
                'method': 'button'
            })
            
            if self.pdf_document:
                self.render_current_page()
                
    def reset_zoom(self):
        """Reset zoom to 100%"""
        if self.zoom_level != 1.0:
            old_zoom = self.zoom_level
            self.zoom_level = 1.0
            self.zoom_slider.setValue(100)
            self.zoom_label.setText("100%")
            
            self._record_interaction("zoom_change", {
                'direction': 'reset',
                'from_zoom': old_zoom,
                'to_zoom': 1.0,
                'method': 'shortcut'
            })
            
            if self.pdf_document:
                self.render_current_page()
                
    def set_zoom_from_slider(self, value):
        """Set zoom from slider value"""
        new_zoom = value / 100.0
        if abs(new_zoom - self.zoom_level) > 0.01:  # Avoid micro-adjustments
            old_zoom = self.zoom_level
            self.zoom_level = new_zoom
            self.zoom_label.setText(f"{value}%")
            
            self._record_interaction("zoom_change", {
                'direction': 'slider',
                'from_zoom': old_zoom,
                'to_zoom': new_zoom,
                'method': 'slider'
            })
            
            if self.pdf_document:
                self.render_current_page()
                
    def toggle_reading_mode(self):
        """Toggle focus/reading mode"""
        is_focus_mode = self.reading_mode_cb.isChecked()
        
        # Hide/show UI elements
        self.toolbar.setVisible(not is_focus_mode)
        
        # Update scroll area styling for focus mode
        if is_focus_mode:
            self.scroll_area.setStyleSheet("""
                QScrollArea {
                    border: none;
                    background-color: #000000;
                }
            """)
        else:
            self.scroll_area.setStyleSheet("")
            
        self._record_interaction("mode_change", {
            'mode': 'focus' if is_focus_mode else 'normal',
            'hidden_ui': is_focus_mode
        })
        
    def toggle_interaction_tracking(self):
        """Toggle interaction tracking"""
        is_tracking = self.track_interactions_cb.isChecked()
        
        if is_tracking:
            self.interaction_timer.start()
        else:
            self.interaction_timer.stop()
            
        self._record_interaction("tracking_toggle", {
            'tracking_enabled': is_tracking
        })
        
    def get_current_page(self):
        """Get current page number (1-indexed)"""
        return self.current_page + 1 if self.pdf_document else 0
        
    def set_page(self, page_num):
        """Set current page programmatically"""
        if self.pdf_document and 1 <= page_num <= self.total_pages:
            old_page = self.current_page
            self.current_page = page_num - 1
            self.page_spinbox.setValue(page_num)
            
            self._record_interaction("page_navigation", {
                'direction': 'programmatic',
                'from_page': old_page + 1,
                'to_page': page_num,
                'method': 'api'
            })
            
            self.render_current_page()
            
    # Interaction tracking methods
    def _record_interaction(self, interaction_type, metadata=None):
        """Record detailed user interaction"""
        if not self.track_interactions_cb.isChecked():
            return
            
        self.interaction_count += 1
        
        # Track specific interaction types
        if interaction_type in ['scroll_vertical', 'scroll_horizontal']:
            self.scroll_interactions += 1
        elif interaction_type in ['mouse_click', 'mouse_press']:
            self.click_interactions += 1
        elif interaction_type in ['key_press', 'shortcut']:
            self.keyboard_interactions += 1
            
        # Record with session timer
        if self.session_timer:
            self.session_timer.record_interaction(interaction_type)
            
        # Emit interaction signal
        interaction_data = {
            'type': interaction_type,
            'timestamp': self.last_interaction_time.elapsed(),
            'page': self.current_page + 1,
            'zoom': self.zoom_level,
            'total_interactions': self.interaction_count,
            'metadata': metadata or {}
        }
        
        self.interaction_detected.emit(interaction_type, interaction_data)
        
        # Update display
        self._update_interaction_display()
        
    def _record_periodic_interaction(self):
        """Record periodic heartbeat interaction"""
        if self.pdf_document and self.hasFocus():
            self._record_interaction("heartbeat", {
                'reading_time': self.last_interaction_time.elapsed() // 1000
            })
            
    def _on_scroll(self, value):
        """Handle scroll events"""
        sender = self.sender()
        if sender == self.scroll_area.verticalScrollBar():
            self._record_interaction("scroll_vertical", {'scroll_value': value})
        else:
            self._record_interaction("scroll_horizontal", {'scroll_value': value})
            
    def _check_focus_state(self):
        """Monitor focus state for session tracking"""
        has_focus = self.hasFocus() or self.scroll_area.hasFocus()
        
        if has_focus != self.was_focused:
            self.was_focused = has_focus
            self.focus_changed.emit(has_focus)
            
            if has_focus:
                self.focus_label.setText("📍 Focused")
                self.focus_label.setStyleSheet("color: #28a745; font-weight: bold;")
            else:
                self.focus_label.setText("📍 Not Focused")
                self.focus_label.setStyleSheet("color: #dc3545; font-weight: bold;")
                
            self._record_interaction("focus_change", {'has_focus': has_focus})
            
    def _auto_save_position(self):
        """Auto-save current reading position"""
        if self.pdf_document and self.session_timer:
            # This would typically save to database via session timer
            pass
            
    def _update_interaction_display(self):
        """Update interaction display in status bar"""
        self.interaction_label.setText(f"Interactions: {self.interaction_count}")
        
        # Update reading time if session timer is available
        if self.session_timer:
            stats = self.session_timer.get_current_stats()
            if stats:
                active_time = stats.get('active_time_seconds', 0)
                minutes = active_time // 60
                seconds = active_time % 60
                self.reading_time_label.setText(f"Reading time: {minutes:02d}:{seconds:02d}")
        
    # Enhanced event handlers with interaction tracking
    def mousePressEvent(self, event):
        """Handle mouse press with interaction tracking"""
        self._record_interaction("mouse_press", {
            'button': event.button().name,
            'position': (event.position().x(), event.position().y())
        })
        super().mousePressEvent(event)

    def mouseReleaseEvent(self, event):
        """Handle mouse release with interaction tracking"""
        self._record_interaction("mouse_click", {
            'button': event.button().name,
            'position': (event.position().x(), event.position().y())
        })
        super().mouseReleaseEvent(event)

    def wheelEvent(self, event):
        """Handle wheel events with interaction tracking"""
        delta = event.angleDelta().y()
        self._record_interaction("mouse_wheel", {
            'delta': delta,
            'direction': 'up' if delta > 0 else 'down'
        })
        super().wheelEvent(event)

    def keyPressEvent(self, event):
        """Handle key press with interaction tracking"""
        self._record_interaction("key_press", {
            'key': event.key(),
            'text': event.text(),
            'modifiers': event.modifiers().name
        })
        super().keyPressEvent(event)

    def mouseMoveEvent(self, event):
        """Handle mouse move with periodic interaction tracking"""
        # Only record mouse move every 100ms to avoid spam
        if not hasattr(self, '_last_mouse_move') or self._last_mouse_move.elapsed() > 100:
            self._record_interaction("mouse_move", {
                'position': (event.position().x(), event.position().y())
            })
            self._last_mouse_move = QElapsedTimer()
            self._last_mouse_move.start()
        super().mouseMoveEvent(event)

    def focusInEvent(self, event):
        """Handle focus in event"""
        self._record_interaction("focus_in", {})
        super().focusInEvent(event)

    def focusOutEvent(self, event):
        """Handle focus out event"""
        self._record_interaction("focus_out", {})
        super().focusOutEvent(event)
        
    def closeEvent(self, event):
        """Handle close event with cleanup"""
        # Stop all timers
        self.interaction_timer.stop()
        self.focus_timer.stop()
        self.auto_save_timer.stop()
        
        # Record final interaction
        self._record_interaction("pdf_closed", {
            'total_interactions': self.interaction_count,
            'session_duration': self.last_interaction_time.elapsed() // 1000
        })
        
        # Close PDF document
        if self.pdf_document:
            self.pdf_document.close()
            
        event.accept()