# src/ui/timer_widget.py - Optimized Version
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                            QPushButton, QProgressBar, QFrame, QGridLayout,
                            QGroupBox, QTextEdit, QTabWidget)
from PyQt6.QtCore import Qt, QTimer, pyqtSlot
from PyQt6.QtGui import QFont
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class TimerWidget(QWidget):
    """Optimized timer widget with essential features"""
    
    def __init__(self):
        super().__init__()
        self.session_timer = None
        self.reading_intelligence = None
        self.current_session_stats = None
        self.current_pdf_info = None
        
        # UI Update timer
        self.ui_timer = QTimer()
        self.ui_timer.timeout.connect(self.update_displays)
        self.ui_timer.start(1000)
        
        self.setup_ui()
        self.apply_styles()
        
    def setup_ui(self):
        """Set up optimized timer UI"""
        layout = QVBoxLayout()
        layout.setContentsMargins(10, 10, 10, 10)
        
        # Header
        header = QLabel("⏱️ Study Timer")
        header.setFont(QFont("Arial", 16, QFont.Weight.Bold))
        header.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(header)
        
        # Main time display
        self.main_time_display = QLabel("00:00:00")
        self.main_time_display.setFont(QFont("Arial", 24, QFont.Weight.Bold))
        self.main_time_display.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.main_time_display.setStyleSheet("""
            QLabel {
                background-color: #1a1a1a;
                color: #00ff00;
                border: 2px solid #333333;
                border-radius: 10px;
                padding: 15px;
                font-family: 'Courier New', monospace;
            }
        """)
        layout.addWidget(self.main_time_display)
        
        # Status indicator
        self.status_indicator = QLabel("⚫ No Session")
        self.status_indicator.setFont(QFont("Arial", 12, QFont.Weight.Bold))
        self.status_indicator.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.status_indicator)
        
        # Quick stats
        stats_group = QGroupBox("📊 Session Stats")
        stats_layout = QGridLayout()
        
        self.active_time_label = QLabel("Active: 00:00:00")
        self.pages_label = QLabel("Pages: 0")
        self.speed_label = QLabel("Speed: 0.0 PPM")
        self.efficiency_label = QLabel("Efficiency: 0%")
        
        stats_layout.addWidget(self.active_time_label, 0, 0)
        stats_layout.addWidget(self.pages_label, 0, 1)
        stats_layout.addWidget(self.speed_label, 1, 0)
        stats_layout.addWidget(self.efficiency_label, 1, 1)
        
        stats_group.setLayout(stats_layout)
        layout.addWidget(stats_group)
        
        # Progress
        progress_group = QGroupBox("📖 Reading Progress")
        progress_layout = QVBoxLayout()
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_text = QLabel("No document loaded")
        self.progress_text.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        progress_layout.addWidget(self.progress_bar)
        progress_layout.addWidget(self.progress_text)
        progress_group.setLayout(progress_layout)
        layout.addWidget(progress_group)
        
        # Time estimation
        estimation_group = QGroupBox("⏳ Time Estimation")
        estimation_layout = QGridLayout()
        
        self.time_remaining_label = QLabel("Calculating...")
        self.finish_time_label = QLabel("Finish: -")
        self.sessions_needed_label = QLabel("Sessions: -")
        self.confidence_label = QLabel("Confidence: -")
        
        estimation_layout.addWidget(QLabel("Remaining:"), 0, 0)
        estimation_layout.addWidget(self.time_remaining_label, 0, 1)
        estimation_layout.addWidget(QLabel("Est. Finish:"), 1, 0)
        estimation_layout.addWidget(self.finish_time_label, 1, 1)
        estimation_layout.addWidget(QLabel("Sessions:"), 2, 0)
        estimation_layout.addWidget(self.sessions_needed_label, 2, 1)
        estimation_layout.addWidget(QLabel("Confidence:"), 3, 0)
        estimation_layout.addWidget(self.confidence_label, 3, 1)
        
        estimation_group.setLayout(estimation_layout)
        layout.addWidget(estimation_group)
        
        # Controls
        controls_layout = QHBoxLayout()
        
        self.pause_resume_btn = QPushButton("⏸️ Pause")
        self.pause_resume_btn.setEnabled(False)
        self.pause_resume_btn.clicked.connect(self.toggle_pause_resume)
        
        self.end_session_btn = QPushButton("⏹️ End Session")
        self.end_session_btn.setEnabled(False)
        self.end_session_btn.clicked.connect(self.end_current_session)
        
        controls_layout.addWidget(self.pause_resume_btn)
        controls_layout.addWidget(self.end_session_btn)
        
        layout.addLayout(controls_layout)
        layout.addStretch()
        self.setLayout(layout)
    
    def apply_styles(self):
        """Apply optimized styling"""
        self.setStyleSheet("""
            QGroupBox {
                font-weight: bold;
                border: 2px solid #cccccc;
                border-radius: 8px;
                margin-top: 10px;
                padding-top: 5px;
                background-color: #fafafa;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 8px 0 8px;
                background-color: white;
                border: 1px solid #cccccc;
                border-radius: 4px;
            }
            QPushButton {
                background-color: #007acc;
                color: white;
                border: 1px solid #005a9e;
                padding: 8px 16px;
                border-radius: 6px;
                font-weight: bold;
                min-height: 30px;
            }
            QPushButton:hover { background-color: #005a9e; }
            QPushButton:disabled { background-color: #cccccc; color: #666666; }
            QPushButton[text*="Pause"] { background-color: #ffa500; }
            QPushButton[text*="Resume"] { background-color: #28a745; }
            QPushButton[text*="End"] { background-color: #dc3545; }
            QProgressBar {
                border: 1px solid #cccccc;
                border-radius: 4px;
                text-align: center;
                height: 20px;
            }
            QProgressBar::chunk { background-color: #007acc; border-radius: 3px; }
        """)
    
    def set_session_timer(self, session_timer):
        """Set session timer with signal connections"""
        self.session_timer = session_timer
        if session_timer:
            session_timer.session_started.connect(self.on_session_started)
            session_timer.session_ended.connect(self.on_session_ended)
            session_timer.session_paused.connect(self.on_session_paused)
            session_timer.session_resumed.connect(self.on_session_resumed)
            session_timer.idle_detected.connect(self.on_idle_state_changed)
            session_timer.stats_updated.connect(self.on_stats_updated)
    
    def set_reading_intelligence(self, reading_intelligence):
        """Set reading intelligence instance"""
        self.reading_intelligence = reading_intelligence
    
    def set_current_pdf_info(self, pdf_info, is_exercise=False):
        """Set current PDF information"""
        self.current_pdf_info = pdf_info
        if pdf_info:
            current_page = pdf_info.get('current_page', 1)
            total_pages = pdf_info.get('total_pages', 1)
            self.update_progress_display(current_page, total_pages)
            
            pdf_type = "Exercise" if is_exercise else "Main PDF"
            title = pdf_info.get('title', 'Unknown')
            self.progress_text.setText(f"{pdf_type}: {title}")
            
            self.update_time_estimation(current_page, total_pages)
        else:
            self.reset_progress_display()
    
    def update_progress_display(self, current_page, total_pages):
        """Update progress display"""
        if total_pages > 0:
            progress_percent = ((current_page - 1) / total_pages) * 100
            pages_remaining = total_pages - current_page + 1
            
            self.progress_bar.setMaximum(total_pages)
            self.progress_bar.setValue(current_page - 1)
            self.progress_bar.setVisible(True)
            
            progress_text = f"Page {current_page} of {total_pages} ({progress_percent:.1f}%)"
            if pages_remaining > 0:
                progress_text += f" • {pages_remaining} pages remaining"
            
            self.progress_text.setText(progress_text)
        else:
            self.reset_progress_display()
    
    def reset_progress_display(self):
        """Reset progress display"""
        self.progress_bar.setVisible(False)
        self.progress_text.setText("No document loaded")
    
    def update_time_estimation(self, current_page, total_pages):
        """Update time estimation"""
        if not self.reading_intelligence or not self.current_pdf_info:
            self.reset_time_estimation()
            return
        
        try:
            pdf_id = self.current_pdf_info.get('id') if not self.current_pdf_info.get('is_exercise') else None
            exercise_pdf_id = self.current_pdf_info.get('id') if self.current_pdf_info.get('is_exercise') else None
            
            estimation = self.reading_intelligence.estimate_finish_time(
                pdf_id=pdf_id,
                exercise_pdf_id=exercise_pdf_id,
                current_page=current_page,
                total_pages=total_pages
            )
            
            if estimation:
                remaining_minutes = estimation.get('estimated_minutes', 0)
                if remaining_minutes > 0:
                    if remaining_minutes >= 60:
                        hours = int(remaining_minutes // 60)
                        minutes = int(remaining_minutes % 60)
                        self.time_remaining_label.setText(f"{hours}h {minutes}m")
                    else:
                        self.time_remaining_label.setText(f"{int(remaining_minutes)}m")
                    
                    finish_time = datetime.now() + timedelta(minutes=remaining_minutes)
                    self.finish_time_label.setText(finish_time.strftime("%H:%M"))
                else:
                    self.time_remaining_label.setText("Complete!")
                    self.finish_time_label.setText("Finished")
                
                sessions = estimation.get('sessions_needed', 1)
                self.sessions_needed_label.setText(f"~{sessions}")
                
                confidence = estimation.get('confidence', 'low')
                color = "#28a745" if confidence == 'high' else "#ffc107" if confidence == 'medium' else "#dc3545"
                self.confidence_label.setText(f"<span style='color: {color}'>{confidence.title()}</span>")
            else:
                self.reset_time_estimation()
                
        except Exception as e:
            logger.error(f"Error updating time estimation: {e}")
            self.reset_time_estimation()
    
    def reset_time_estimation(self):
        """Reset time estimation"""
        self.time_remaining_label.setText("Calculating...")
        self.finish_time_label.setText("-")
        self.sessions_needed_label.setText("-")
        self.confidence_label.setText("-")
    
    def format_duration(self, seconds):
        """Format duration to HH:MM:SS"""
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    
    def update_displays(self):
        """Update all displays"""
        if self.current_session_stats:
            total_time = self.current_session_stats.get('total_time_seconds', 0)
            active_time = self.current_session_stats.get('active_time_seconds', 0)
            pages_visited = self.current_session_stats.get('pages_visited', 0)
            reading_speed = self.current_session_stats.get('reading_speed_ppm', 0)
            
            # Update displays
            self.main_time_display.setText(self.format_duration(total_time))
            self.active_time_label.setText(f"Active: {self.format_duration(active_time)}")
            self.pages_label.setText(f"Pages: {pages_visited}")
            self.speed_label.setText(f"Speed: {reading_speed:.1f} PPM")
            
            efficiency = (active_time / total_time * 100) if total_time > 0 else 0
            self.efficiency_label.setText(f"Efficiency: {efficiency:.0f}%")
    
    def toggle_pause_resume(self):
        """Toggle pause/resume"""
        if self.session_timer:
            if self.current_session_stats and self.current_session_stats.get('is_idle', False):
                self.session_timer.resume_session()
            else:
                self.session_timer.pause_session(manual=True)
    
    def end_current_session(self):
        """End current session"""
        if self.session_timer:
            self.session_timer.end_session()
    
    # Signal handlers
    @pyqtSlot(int)
    def on_session_started(self, session_id):
        """Handle session started"""
        self.status_indicator.setText("🟢 Session Active")
        self.pause_resume_btn.setEnabled(True)
        self.end_session_btn.setEnabled(True)
        self.pause_resume_btn.setText("⏸️ Pause")
    
    @pyqtSlot(int, dict)
    def on_session_ended(self, session_id, stats):
        """Handle session ended"""
        self.status_indicator.setText("⚫ No Session")
        self.pause_resume_btn.setEnabled(False)
        self.end_session_btn.setEnabled(False)
        self.main_time_display.setText("00:00:00")
        self.active_time_label.setText("Active: 00:00:00")
        self.pages_label.setText("Pages: 0")
        self.speed_label.setText("Speed: 0.0 PPM")
        self.efficiency_label.setText("Efficiency: 0%")
        self.current_session_stats = None
    
    @pyqtSlot(int, bool)
    def on_session_paused(self, session_id, is_manual):
        """Handle session paused"""
        if is_manual:
            self.status_indicator.setText("⏸️ Manually Paused")
        else:
            self.status_indicator.setText("😴 Auto-Paused (Idle)")
        self.pause_resume_btn.setText("▶️ Resume")
    
    @pyqtSlot(int)
    def on_session_resumed(self, session_id):
        """Handle session resumed"""
        self.status_indicator.setText("🟢 Session Active")
        self.pause_resume_btn.setText("⏸️ Pause")
    
    @pyqtSlot(bool)
    def on_idle_state_changed(self, is_idle):
        """Handle idle state changes"""
        if is_idle:
            self.status_indicator.setText("🟡 Idle Detected")
            self.pause_resume_btn.setText("▶️ Resume")
        else:
            self.status_indicator.setText("🟢 Session Active")
            self.pause_resume_btn.setText("⏸️ Pause")
    
    @pyqtSlot(dict)
    def on_stats_updated(self, stats):
        """Handle stats updates"""
        self.current_session_stats = stats
        
        # Update page progress if available
        if self.current_pdf_info:
            current_page = stats.get('current_page', 1)
            total_pages = self.current_pdf_info.get('total_pages', 1)
            self.update_progress_display(current_page, total_pages)
            self.update_time_estimation(current_page, total_pages)


class StudyDashboardWidget(QWidget):
    """Optimized dashboard for study analytics"""
    
    def __init__(self, db_manager):
        super().__init__()
        self.db_manager = db_manager
        self.reading_intelligence = None
        
        # Update timer
        self.update_timer = QTimer()
        self.update_timer.timeout.connect(self.refresh_stats)
        self.update_timer.start(60000)  # Update every minute
        
        self.setup_ui()
        self.apply_styles()
    
    def setup_ui(self):
        """Set up dashboard UI"""
        layout = QVBoxLayout()
        layout.setContentsMargins(10, 10, 10, 10)
        
        # Header
        header = QLabel("📊 Study Dashboard")
        header.setFont(QFont("Arial", 16, QFont.Weight.Bold))
        header.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(header)
        
        # Overview Stats
        overview_group = QGroupBox("📈 Overview")
        overview_layout = QGridLayout()
        
        self.total_study_time_label = QLabel("Total Time: Loading...")
        self.total_sessions_label = QLabel("Sessions: Loading...")
        self.total_pages_label = QLabel("Pages: Loading...")
        self.avg_session_label = QLabel("Avg Session: Loading...")
        
        overview_layout.addWidget(self.total_study_time_label, 0, 0)
        overview_layout.addWidget(self.total_sessions_label, 0, 1)
        overview_layout.addWidget(self.total_pages_label, 1, 0)
        overview_layout.addWidget(self.avg_session_label, 1, 1)
        
        overview_group.setLayout(overview_layout)
        layout.addWidget(overview_group)
        
        # This Week Stats
        week_group = QGroupBox("📅 This Week")
        week_layout = QGridLayout()
        
        self.week_sessions_label = QLabel("Sessions: Loading...")
        self.week_time_label = QLabel("Time: Loading...")
        self.week_pages_label = QLabel("Pages: Loading...")
        self.week_avg_label = QLabel("Daily Avg: Loading...")
        
        week_layout.addWidget(self.week_sessions_label, 0, 0)
        week_layout.addWidget(self.week_time_label, 0, 1)
        week_layout.addWidget(self.week_pages_label, 1, 0)
        week_layout.addWidget(self.week_avg_label, 1, 1)
        
        week_group.setLayout(week_layout)
        layout.addWidget(week_group)
        
        # Reading Speed
        speed_group = QGroupBox("⚡ Reading Performance")
        speed_layout = QGridLayout()
        
        self.overall_speed_label = QLabel("Speed: Loading...")
        self.efficiency_label = QLabel("Efficiency: Loading...")
        
        speed_layout.addWidget(self.overall_speed_label, 0, 0)
        speed_layout.addWidget(self.efficiency_label, 0, 1)
        
        speed_group.setLayout(speed_layout)
        layout.addWidget(speed_group)
        
        # Refresh button
        refresh_btn = QPushButton("🔄 Refresh")
        refresh_btn.clicked.connect(self.refresh_stats)
        layout.addWidget(refresh_btn)
        
        layout.addStretch()
        self.setLayout(layout)
    
    def apply_styles(self):
        """Apply styling"""
        self.setStyleSheet("""
            QGroupBox {
                font-weight: bold;
                border: 2px solid #cccccc;
                border-radius: 8px;
                margin-top: 10px;
                padding-top: 10px;
                background-color: #fafafa;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 8px 0 8px;
                background-color: white;
                border: 1px solid #cccccc;
                border-radius: 4px;
            }
            QLabel {
                padding: 4px;
                background-color: white;
                border: 1px solid #eeeeee;
                border-radius: 4px;
                margin: 2px;
            }
            QPushButton {
                background-color: #28a745;
                color: white;
                border: 1px solid #1e7e34;
                padding: 10px 20px;
                border-radius: 6px;
                font-weight: bold;
                min-height: 35px;
            }
            QPushButton:hover { background-color: #1e7e34; }
        """)
    
    def set_reading_intelligence(self, reading_intelligence):
        """Set reading intelligence"""
        self.reading_intelligence = reading_intelligence
        self.refresh_stats()
    
    def refresh_stats(self):
        """Refresh all statistics"""
        try:
            self.update_overview_stats()
            self.update_week_stats()
            self.update_speed_stats()
        except Exception as e:
            logger.error(f"Error refreshing stats: {e}")
    
    def update_overview_stats(self):
        """Update overview statistics"""
        try:
            if self.reading_intelligence:
                metrics = self.reading_intelligence.get_reading_speed(user_wide=True)
                
                if metrics:
                    total_time = float(metrics.get('total_time_spent_seconds', 0))
                    total_pages = metrics.get('total_pages_read', 0) or 0
                    
                    self.total_study_time_label.setText(f"Total Time: {self.format_duration(int(total_time))}")
                    self.total_pages_label.setText(f"Pages: {total_pages:,}")
                else:
                    self.total_study_time_label.setText("Total Time: No data")
                    self.total_pages_label.setText("Pages: No data")
            
            self.total_sessions_label.setText("Sessions: Computing...")
            self.avg_session_label.setText("Avg Session: Computing...")
            
        except Exception as e:
            logger.error(f"Error updating overview: {e}")
    
    def update_week_stats(self):
        """Update weekly statistics"""
        try:
            if self.reading_intelligence:
                history = self.reading_intelligence.get_session_history(days=7)
                
                if history:
                    week_sessions = len(history)
                    week_time = sum(float(s.get('total_time_seconds', 0)) for s in history)
                    week_pages = sum(s.get('pages_visited', 0) or 0 for s in history)
                    daily_avg = week_time / 7
                    
                    self.week_sessions_label.setText(f"Sessions: {week_sessions}")
                    self.week_time_label.setText(f"Time: {self.format_duration(int(week_time))}")
                    self.week_pages_label.setText(f"Pages: {week_pages}")
                    self.week_avg_label.setText(f"Daily Avg: {self.format_duration(int(daily_avg))}")
                else:
                    self.week_sessions_label.setText("Sessions: 0")
                    self.week_time_label.setText("Time: 00:00:00")
                    self.week_pages_label.setText("Pages: 0")
                    self.week_avg_label.setText("Daily Avg: 00:00:00")
            
        except Exception as e:
            logger.error(f"Error updating week stats: {e}")
    
    def update_speed_stats(self):
        """Update speed statistics"""
        try:
            if self.reading_intelligence:
                metrics = self.reading_intelligence.get_reading_speed(user_wide=True)
                
                if metrics:
                    speed = float(metrics.get('pages_per_minute', 0))
                    self.overall_speed_label.setText(f"Speed: {speed:.2f} pages/min")
                    self.efficiency_label.setText("Efficiency: 85%")  # Placeholder
                else:
                    self.overall_speed_label.setText("Speed: No data")
                    self.efficiency_label.setText("Efficiency: No data")
            
        except Exception as e:
            logger.error(f"Error updating speed stats: {e}")
    
    def format_duration(self, seconds):
        """Format duration to readable format"""
        if seconds < 60:
            return f"{seconds}s"
        elif seconds < 3600:
            minutes = seconds // 60
            return f"{minutes}m"
        else:
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            return f"{hours}h {minutes}m"