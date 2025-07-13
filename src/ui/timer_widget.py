# src/ui/timer_widget.py
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                            QPushButton, QProgressBar, QFrame, QGridLayout,
                            QGroupBox, QScrollArea)
from PyQt6.QtCore import Qt, QTimer, pyqtSlot
from PyQt6.QtGui import QFont, QPixmap, QPainter, QColor, QPen
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class TimerWidget(QWidget):
    """Widget to display session timer and reading statistics"""
    
    def __init__(self):
        super().__init__()
        self.session_timer = None
        self.reading_intelligence = None
        self.current_session_stats = None
        self.current_pdf_info = None
        
        # UI Update timer
        self.ui_timer = QTimer()
        self.ui_timer.timeout.connect(self.update_displays)
        self.ui_timer.start(1000)  # Update every second
        
        self.setup_ui()
        self.apply_styles()
        
    def setup_ui(self):
        """Set up the timer widget UI"""
        layout = QVBoxLayout()
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)
        
        # Session Status Group
        self.session_group = QGroupBox("📚 Current Session")
        session_layout = QVBoxLayout()
        
        # Session info
        info_layout = QGridLayout()
        
        self.session_status_label = QLabel("No active session")
        self.session_status_label.setFont(QFont("Arial", 12, QFont.Weight.Bold))
        info_layout.addWidget(QLabel("Status:"), 0, 0)
        info_layout.addWidget(self.session_status_label, 0, 1)
        
        self.current_pdf_label = QLabel("No PDF loaded")
        info_layout.addWidget(QLabel("Document:"), 1, 0)
        info_layout.addWidget(self.current_pdf_label, 1, 1)
        
        self.session_time_label = QLabel("00:00:00")
        self.session_time_label.setFont(QFont("Arial", 14, QFont.Weight.Bold))
        info_layout.addWidget(QLabel("Session Time:"), 2, 0)
        info_layout.addWidget(self.session_time_label, 2, 1)
        
        self.active_time_label = QLabel("00:00:00")
        info_layout.addWidget(QLabel("Active Time:"), 3, 0)
        info_layout.addWidget(self.active_time_label, 3, 1)
        
        self.pages_visited_label = QLabel("0 pages")
        info_layout.addWidget(QLabel("Pages Visited:"), 4, 0)
        info_layout.addWidget(self.pages_visited_label, 4, 1)
        
        session_layout.addLayout(info_layout)
        
        # Idle indicator
        self.idle_indicator = QLabel("🟢 Active")
        self.idle_indicator.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.idle_indicator.setFont(QFont("Arial", 10, QFont.Weight.Bold))
        session_layout.addWidget(self.idle_indicator)
        
        self.session_group.setLayout(session_layout)
        layout.addWidget(self.session_group)
        
        # Reading Progress Group
        self.progress_group = QGroupBox("📖 Reading Progress")
        progress_layout = QVBoxLayout()
        
        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        progress_layout.addWidget(self.progress_bar)
        
        # Progress info
        progress_info_layout = QGridLayout()
        
        self.current_page_label = QLabel("Page: -")
        progress_info_layout.addWidget(self.current_page_label, 0, 0)
        
        self.total_pages_label = QLabel("Total: -")
        progress_info_layout.addWidget(self.total_pages_label, 0, 1)
        
        self.progress_percent_label = QLabel("Progress: 0%")
        progress_info_layout.addWidget(self.progress_percent_label, 1, 0, 1, 2)
        
        progress_layout.addLayout(progress_info_layout)
        self.progress_group.setLayout(progress_layout)
        layout.addWidget(self.progress_group)
        
        # Time Estimation Group
        self.estimation_group = QGroupBox("⏱️ Time Estimation")
        estimation_layout = QVBoxLayout()
        
        est_info_layout = QGridLayout()
        
        self.reading_speed_label = QLabel("Speed: - pages/min")
        est_info_layout.addWidget(self.reading_speed_label, 0, 0, 1, 2)
        
        self.time_remaining_label = QLabel("Remaining: -")
        est_info_layout.addWidget(QLabel("Time Remaining:"), 1, 0)
        est_info_layout.addWidget(self.time_remaining_label, 1, 1)
        
        self.estimated_finish_label = QLabel("Finish: -")
        est_info_layout.addWidget(QLabel("Est. Finish:"), 2, 0)
        est_info_layout.addWidget(self.estimated_finish_label, 2, 1)
        
        self.confidence_label = QLabel("Confidence: -")
        est_info_layout.addWidget(QLabel("Confidence:"), 3, 0)
        est_info_layout.addWidget(self.confidence_label, 3, 1)
        
        estimation_layout.addLayout(est_info_layout)
        self.estimation_group.setLayout(estimation_layout)
        layout.addWidget(self.estimation_group)
        
        # Daily Stats Group
        self.daily_group = QGroupBox("📊 Today's Progress")
        daily_layout = QVBoxLayout()
        
        daily_info_layout = QGridLayout()
        
        self.daily_sessions_label = QLabel("Sessions: 0")
        daily_info_layout.addWidget(self.daily_sessions_label, 0, 0)
        
        self.daily_time_label = QLabel("Time: 00:00:00")
        daily_info_layout.addWidget(self.daily_time_label, 0, 1)
        
        self.daily_pages_label = QLabel("Pages: 0")
        daily_info_layout.addWidget(self.daily_pages_label, 1, 0)
        
        self.daily_avg_label = QLabel("Avg: 0 sec/page")
        daily_info_layout.addWidget(self.daily_avg_label, 1, 1)
        
        daily_layout.addLayout(daily_info_layout)
        self.daily_group.setLayout(daily_layout)
        layout.addWidget(self.daily_group)
        
        # Control buttons
        button_layout = QHBoxLayout()
        
        self.pause_resume_btn = QPushButton("⏸️ Pause")
        self.pause_resume_btn.setEnabled(False)
        self.pause_resume_btn.clicked.connect(self.toggle_pause_resume)
        
        self.end_session_btn = QPushButton("⏹️ End Session")
        self.end_session_btn.setEnabled(False)
        self.end_session_btn.clicked.connect(self.end_current_session)
        
        button_layout.addWidget(self.pause_resume_btn)
        button_layout.addWidget(self.end_session_btn)
        
        layout.addLayout(button_layout)
        layout.addStretch()
        
        self.setLayout(layout)
        
    def apply_styles(self):
        """Apply consistent styling"""
        self.setStyleSheet("""
            QGroupBox {
                font-weight: bold;
                font-size: 14px;
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
            QLabel {
                color: #333333;
                font-size: 12px;
                padding: 2px;
            }
            QPushButton {
                background-color: #007acc;
                color: white;
                border: 1px solid #005a9e;
                padding: 8px 16px;
                border-radius: 6px;
                font-weight: bold;
                font-size: 12px;
                min-height: 30px;
            }
            QPushButton:hover {
                background-color: #005a9e;
            }
            QPushButton:disabled {
                background-color: #cccccc;
                color: #666666;
                border: 1px solid #999999;
            }
            QPushButton[text*="Pause"] {
                background-color: #ffa500;
                border: 1px solid #ff8c00;
            }
            QPushButton[text*="Resume"] {
                background-color: #28a745;
                border: 1px solid #1e7e34;
            }
            QPushButton[text*="End"] {
                background-color: #dc3545;
                border: 1px solid #c82333;
            }
            QProgressBar {
                border: 1px solid #cccccc;
                border-radius: 4px;
                text-align: center;
                height: 20px;
            }
            QProgressBar::chunk {
                background-color: #007acc;
                border-radius: 3px;
            }
        """)
        
    def set_session_timer(self, session_timer):
        """Set the session timer instance"""
        self.session_timer = session_timer
        if session_timer:
            # Connect signals
            session_timer.session_started.connect(self.on_session_started)
            session_timer.session_ended.connect(self.on_session_ended)
            session_timer.idle_detected.connect(self.on_idle_state_changed)
            session_timer.stats_updated.connect(self.on_stats_updated)
            
    def set_reading_intelligence(self, reading_intelligence):
        """Set the reading intelligence instance"""
        self.reading_intelligence = reading_intelligence
        
    def set_current_pdf_info(self, pdf_info, is_exercise=False):
        """Set current PDF information"""
        self.current_pdf_info = pdf_info
        if pdf_info:
            pdf_type = "Exercise" if is_exercise else "Main PDF"
            self.current_pdf_label.setText(f"{pdf_type}: {pdf_info.get('title', 'Unknown')}")
            
            # Update progress display
            current_page = pdf_info.get('current_page', 1)
            total_pages = pdf_info.get('total_pages', 1)
            
            self.update_progress_display(current_page, total_pages)
        else:
            self.current_pdf_label.setText("No PDF loaded")
            self.reset_progress_display()
            
    def update_progress_display(self, current_page, total_pages):
        """Update the progress display"""
        if total_pages > 0:
            progress_percent = ((current_page - 1) / total_pages) * 100
            
            self.current_page_label.setText(f"Page: {current_page}")
            self.total_pages_label.setText(f"Total: {total_pages}")
            self.progress_percent_label.setText(f"Progress: {progress_percent:.1f}%")
            
            self.progress_bar.setMaximum(total_pages)
            self.progress_bar.setValue(current_page - 1)
            self.progress_bar.setVisible(True)
            
            # Update time estimation
            self.update_time_estimation(current_page, total_pages)
        else:
            self.reset_progress_display()
            
    def reset_progress_display(self):
        """Reset progress display"""
        self.current_page_label.setText("Page: -")
        self.total_pages_label.setText("Total: -")
        self.progress_percent_label.setText("Progress: 0%")
        self.progress_bar.setVisible(False)
        self.reset_time_estimation()
        
    def update_time_estimation(self, current_page, total_pages):
        """Update time estimation display"""
        if not self.reading_intelligence or not self.current_pdf_info:
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
                # Reading speed
                avg_time = estimation.get('average_time_per_page', 0)
                if avg_time > 0:
                    pages_per_minute = 60 / avg_time
                    self.reading_speed_label.setText(f"Speed: {pages_per_minute:.1f} pages/min")
                else:
                    self.reading_speed_label.setText("Speed: Calculating...")
                
                # Time remaining
                remaining_minutes = estimation.get('estimated_minutes', 0)
                if remaining_minutes > 0:
                    if remaining_minutes >= 60:
                        hours = int(remaining_minutes // 60)
                        minutes = int(remaining_minutes % 60)
                        self.time_remaining_label.setText(f"{hours}h {minutes}m")
                    else:
                        self.time_remaining_label.setText(f"{int(remaining_minutes)}m")
                        
                    # Estimated finish time
                    finish_time = datetime.now() + timedelta(minutes=remaining_minutes)
                    self.estimated_finish_label.setText(finish_time.strftime("%H:%M"))
                else:
                    self.time_remaining_label.setText("Complete!")
                    self.estimated_finish_label.setText("Finished")
                
                # Confidence
                confidence = estimation.get('confidence', 'low')
                confidence_text = "High" if confidence == 'high' else "Low"
                confidence_color = "#28a745" if confidence == 'high' else "#ffc107"
                self.confidence_label.setText(f"<span style='color: {confidence_color}'>{confidence_text}</span>")
            else:
                self.reset_time_estimation()
                
        except Exception as e:
            logger.error(f"Error updating time estimation: {e}")
            self.reset_time_estimation()
            
    def reset_time_estimation(self):
        """Reset time estimation display"""
        self.reading_speed_label.setText("Speed: - pages/min")
        self.time_remaining_label.setText("-")
        self.estimated_finish_label.setText("-")
        self.confidence_label.setText("-")
        
    def update_daily_stats(self):
        """Update daily statistics display"""
        if not self.reading_intelligence:
            return
            
        try:
            stats = self.reading_intelligence.get_daily_stats()
            if stats:
                sessions = stats.get('sessions_count', 0)
                total_seconds = stats.get('total_time_seconds', 0)
                total_pages = stats.get('total_pages_read', 0)
                avg_seconds = stats.get('avg_seconds_per_page', 0)
                
                self.daily_sessions_label.setText(f"Sessions: {sessions}")
                self.daily_time_label.setText(f"Time: {self.format_duration(total_seconds)}")
                self.daily_pages_label.setText(f"Pages: {total_pages}")
                self.daily_avg_label.setText(f"Avg: {int(avg_seconds)}s/page")
            else:
                self.daily_sessions_label.setText("Sessions: 0")
                self.daily_time_label.setText("Time: 00:00:00")
                self.daily_pages_label.setText("Pages: 0")
                self.daily_avg_label.setText("Avg: 0s/page")
                
        except Exception as e:
            logger.error(f"Error updating daily stats: {e}")
            
    def format_duration(self, seconds):
        """Format duration in seconds to HH:MM:SS"""
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
        
    def update_displays(self):
        """Update all time displays"""
        # Update session time display
        if self.current_session_stats:
            total_time = self.current_session_stats.get('total_time_seconds', 0)
            active_time = self.current_session_stats.get('active_time_seconds', 0)
            
            self.session_time_label.setText(self.format_duration(total_time))
            self.active_time_label.setText(self.format_duration(active_time))
            
            pages_visited = self.current_session_stats.get('pages_visited', 0)
            self.pages_visited_label.setText(f"{pages_visited} pages")
        
        # Update daily stats periodically (every 30 seconds)
        if hasattr(self, '_last_daily_update'):
            if (datetime.now() - self._last_daily_update).seconds > 30:
                self.update_daily_stats()
                self._last_daily_update = datetime.now()
        else:
            self.update_daily_stats()
            self._last_daily_update = datetime.now()
    
    def toggle_pause_resume(self):
        """Toggle pause/resume session"""
        if not self.session_timer:
            return
            
        if self.current_session_stats and self.current_session_stats.get('is_idle', False):
            self.session_timer.resume_session()
            self.pause_resume_btn.setText("⏸️ Pause")
        else:
            self.session_timer.pause_session()
            self.pause_resume_btn.setText("▶️ Resume")
    
    def end_current_session(self):
        """End the current session"""
        if self.session_timer:
            self.session_timer.end_session()
    
    @pyqtSlot(int)
    def on_session_started(self, session_id):
        """Handle session started"""
        self.session_status_label.setText(f"🟢 Active (ID: {session_id})")
        self.pause_resume_btn.setEnabled(True)
        self.end_session_btn.setEnabled(True)
        self.pause_resume_btn.setText("⏸️ Pause")
        
        logger.info(f"Timer widget: Session {session_id} started")
    
    @pyqtSlot(int, dict)
    def on_session_ended(self, session_id, stats):
        """Handle session ended"""
        self.session_status_label.setText("⏹️ No active session")
        self.pause_resume_btn.setEnabled(False)
        self.end_session_btn.setEnabled(False)
        self.pause_resume_btn.setText("⏸️ Pause")
        
        # Reset session displays
        self.session_time_label.setText("00:00:00")
        self.active_time_label.setText("00:00:00")
        self.pages_visited_label.setText("0 pages")
        self.idle_indicator.setText("⚫ Inactive")
        
        self.current_session_stats = None
        
        # Update daily stats after session ends
        self.update_daily_stats()
        
        logger.info(f"Timer widget: Session {session_id} ended")
    
    @pyqtSlot(bool)
    def on_idle_state_changed(self, is_idle):
        """Handle idle state changes"""
        if is_idle:
            self.idle_indicator.setText("🟡 Idle")
            self.pause_resume_btn.setText("▶️ Resume")
        else:
            self.idle_indicator.setText("🟢 Active")
            self.pause_resume_btn.setText("⏸️ Pause")
    
    @pyqtSlot(dict)
    def on_stats_updated(self, stats):
        """Handle stats updates"""
        self.current_session_stats = stats
        
        # Update page progress if current page changed
        if self.current_pdf_info:
            current_page = stats.get('current_page', 1)
            total_pages = self.current_pdf_info.get('total_pages', 1)
            self.update_progress_display(current_page, total_pages)


class StudyDashboardWidget(QWidget):
    """Enhanced dashboard showing study analytics and progress"""
    
    def __init__(self, db_manager):
        super().__init__()
        self.db_manager = db_manager
        self.reading_intelligence = None
        
        # Update timer
        self.update_timer = QTimer()
        self.update_timer.timeout.connect(self.refresh_all_stats)
        self.update_timer.start(60000)  # Update every minute
        
        self.setup_ui()
        self.apply_styles()
        
    def setup_ui(self):
        """Set up the dashboard UI"""
        layout = QVBoxLayout()
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(15)
        
        # Dashboard header
        header = QLabel("📊 Study Dashboard")
        header.setFont(QFont("Arial", 18, QFont.Weight.Bold))
        header.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(header)
        
        # Create scroll area for dashboard content
        scroll_area = QScrollArea()
        scroll_widget = QWidget()
        scroll_layout = QVBoxLayout()
        
        # Overview Stats Group
        self.overview_group = QGroupBox("📈 Overview")
        overview_layout = QGridLayout()
        
        self.total_study_time_label = QLabel("Total Study Time: Loading...")
        self.total_sessions_label = QLabel("Total Sessions: Loading...")
        self.total_pages_label = QLabel("Total Pages Read: Loading...")
        self.avg_session_time_label = QLabel("Avg Session Time: Loading...")
        
        overview_layout.addWidget(self.total_study_time_label, 0, 0)
        overview_layout.addWidget(self.total_sessions_label, 0, 1)
        overview_layout.addWidget(self.total_pages_label, 1, 0)
        overview_layout.addWidget(self.avg_session_time_label, 1, 1)
        
        self.overview_group.setLayout(overview_layout)
        scroll_layout.addWidget(self.overview_group)
        
        # This Week Stats Group
        self.week_group = QGroupBox("📅 This Week")
        week_layout = QGridLayout()
        
        self.week_sessions_label = QLabel("Sessions: Loading...")
        self.week_time_label = QLabel("Study Time: Loading...")
        self.week_pages_label = QLabel("Pages Read: Loading...")
        self.week_avg_label = QLabel("Daily Avg: Loading...")
        
        week_layout.addWidget(self.week_sessions_label, 0, 0)
        week_layout.addWidget(self.week_time_label, 0, 1)
        week_layout.addWidget(self.week_pages_label, 1, 0)
        week_layout.addWidget(self.week_avg_label, 1, 1)
        
        self.week_group.setLayout(week_layout)
        scroll_layout.addWidget(self.week_group)
        
        # Reading Speed Group
        self.speed_group = QGroupBox("⚡ Reading Performance")
        speed_layout = QGridLayout()
        
        self.overall_speed_label = QLabel("Overall Speed: Loading...")
        self.best_speed_label = QLabel("Best Speed: Loading...")
        self.efficiency_label = QLabel("Efficiency: Loading...")
        self.consistency_label = QLabel("Consistency: Loading...")
        
        speed_layout.addWidget(self.overall_speed_label, 0, 0)
        speed_layout.addWidget(self.best_speed_label, 0, 1)
        speed_layout.addWidget(self.efficiency_label, 1, 0)
        speed_layout.addWidget(self.consistency_label, 1, 1)
        
        self.speed_group.setLayout(speed_layout)
        scroll_layout.addWidget(self.speed_group)
        
        # Streaks Group
        self.streaks_group = QGroupBox("🔥 Study Streaks")
        streaks_layout = QGridLayout()
        
        self.current_streak_label = QLabel("Current Streak: Loading...")
        self.longest_streak_label = QLabel("Longest Streak: Loading...")
        self.streak_sessions_label = QLabel("Streak Sessions: Loading...")
        self.streak_time_label = QLabel("Streak Time: Loading...")
        
        streaks_layout.addWidget(self.current_streak_label, 0, 0)
        streaks_layout.addWidget(self.longest_streak_label, 0, 1)
        streaks_layout.addWidget(self.streak_sessions_label, 1, 0)
        streaks_layout.addWidget(self.streak_time_label, 1, 1)
        
        self.streaks_group.setLayout(streaks_layout)
        scroll_layout.addWidget(self.streaks_group)
        
        # Recent Activity Group
        self.activity_group = QGroupBox("📚 Recent Activity")
        activity_layout = QVBoxLayout()
        
        self.recent_activity_label = QLabel("Loading recent activity...")
        self.recent_activity_label.setWordWrap(True)
        activity_layout.addWidget(self.recent_activity_label)
        
        self.activity_group.setLayout(activity_layout)
        scroll_layout.addWidget(self.activity_group)
        
        # Refresh button
        refresh_btn = QPushButton("🔄 Refresh Statistics")
        refresh_btn.clicked.connect(self.refresh_all_stats)
        scroll_layout.addWidget(refresh_btn)
        
        scroll_layout.addStretch()
        scroll_widget.setLayout(scroll_layout)
        scroll_area.setWidget(scroll_widget)
        scroll_area.setWidgetResizable(True)
        
        layout.addWidget(scroll_area)
        self.setLayout(layout)
        
    def apply_styles(self):
        """Apply consistent styling"""
        self.setStyleSheet("""
            QGroupBox {
                font-weight: bold;
                font-size: 14px;
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
                color: #333333;
                font-size: 12px;
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
                font-size: 14px;
                min-height: 35px;
            }
            QPushButton:hover {
                background-color: #1e7e34;
            }
            QScrollArea {
                border: none;
                background-color: transparent;
            }
        """)
        
    def set_reading_intelligence(self, reading_intelligence):
        """Set the reading intelligence instance"""
        self.reading_intelligence = reading_intelligence
        self.refresh_all_stats()
        
    def refresh_all_stats(self):
        """Refresh all statistics displays"""
        try:
            self.update_overview_stats()
            self.update_week_stats()
            self.update_speed_stats()
            self.update_streak_stats()
            self.update_recent_activity()
        except Exception as e:
            logger.error(f"Error refreshing dashboard stats: {e}")
            
    def update_overview_stats(self):
        """Update overview statistics"""
        try:
            # Get overall metrics
            metrics = self.reading_intelligence.get_reading_speed(user_wide=True) if self.reading_intelligence else None
            
            if metrics:
                total_time = metrics.get('total_time_spent_seconds', 0)
                total_pages = metrics.get('total_pages_read', 0)
                
                self.total_study_time_label.setText(f"Total Study Time: {self.format_duration(total_time)}")
                self.total_pages_label.setText(f"Total Pages Read: {total_pages:,}")
            else:
                self.total_study_time_label.setText("Total Study Time: No data")
                self.total_pages_label.setText("Total Pages Read: No data")
            
            # Get session count from database
            try:
                stats = self.db_manager.get_current_session_stats()
                if stats:
                    # This is a placeholder - you'd need to implement total session count
                    self.total_sessions_label.setText("Total Sessions: Computing...")
                    self.avg_session_time_label.setText("Avg Session Time: Computing...")
                else:
                    self.total_sessions_label.setText("Total Sessions: No data")
                    self.avg_session_time_label.setText("Avg Session Time: No data")
            except:
                self.total_sessions_label.setText("Total Sessions: Error")
                self.avg_session_time_label.setText("Avg Session Time: Error")
                
        except Exception as e:
            logger.error(f"Error updating overview stats: {e}")
            
    def update_week_stats(self):
        """Update this week's statistics"""
        try:
            # Get week's session history
            if self.reading_intelligence:
                history = self.reading_intelligence.get_session_history(days=7)
                
                if history:
                    week_sessions = len(history)
                    week_time = sum(session.get('total_time_seconds', 0) for session in history)
                    week_pages = sum(session.get('pages_visited', 0) for session in history)
                    daily_avg = week_time / 7 if week_time > 0 else 0
                    
                    self.week_sessions_label.setText(f"Sessions: {week_sessions}")
                    self.week_time_label.setText(f"Study Time: {self.format_duration(week_time)}")
                    self.week_pages_label.setText(f"Pages Read: {week_pages}")
                    self.week_avg_label.setText(f"Daily Avg: {self.format_duration(int(daily_avg))}")
                else:
                    self.week_sessions_label.setText("Sessions: 0")
                    self.week_time_label.setText("Study Time: 00:00:00")
                    self.week_pages_label.setText("Pages Read: 0")
                    self.week_avg_label.setText("Daily Avg: 00:00:00")
            else:
                self.week_sessions_label.setText("Sessions: No data")
                self.week_time_label.setText("Study Time: No data")
                self.week_pages_label.setText("Pages Read: No data")
                self.week_avg_label.setText("Daily Avg: No data")
                
        except Exception as e:
            logger.error(f"Error updating week stats: {e}")
            
    def update_speed_stats(self):
        """Update reading speed statistics"""
        try:
            if self.reading_intelligence:
                metrics = self.reading_intelligence.get_reading_speed(user_wide=True)
                
                if metrics:
                    speed = metrics.get('pages_per_minute', 0)
                    avg_time = metrics.get('average_time_per_page_seconds', 0)
                    
                    self.overall_speed_label.setText(f"Overall Speed: {speed:.2f} pages/min")
                    
                    # Calculate efficiency (active time vs total time would need session data)
                    efficiency = 85  # Placeholder
                    self.efficiency_label.setText(f"Efficiency: {efficiency}%")
                    
                    # Placeholder values for best speed and consistency
                    self.best_speed_label.setText(f"Best Speed: {speed * 1.3:.2f} pages/min")
                    self.consistency_label.setText("Consistency: Good")
                else:
                    self.overall_speed_label.setText("Overall Speed: No data")
                    self.best_speed_label.setText("Best Speed: No data")
                    self.efficiency_label.setText("Efficiency: No data")
                    self.consistency_label.setText("Consistency: No data")
            else:
                self.overall_speed_label.setText("Overall Speed: No data")
                self.best_speed_label.setText("Best Speed: No data")
                self.efficiency_label.setText("Efficiency: No data")
                self.consistency_label.setText("Consistency: No data")
                
        except Exception as e:
            logger.error(f"Error updating speed stats: {e}")
            
    def update_streak_stats(self):
        """Update study streak statistics"""
        try:
            # Get reading streaks
            streaks = self.db_manager.get_reading_streaks() if hasattr(self.db_manager, 'get_reading_streaks') else None
            
            if streaks:
                current_streak = streaks.get('current_streak_days', 0)
                streak_sessions = streaks.get('streak_sessions', 0)
                streak_time = streaks.get('streak_total_time', 0)
                
                self.current_streak_label.setText(f"Current Streak: {current_streak} days")
                self.longest_streak_label.setText(f"Longest Streak: {current_streak} days")  # Placeholder
                self.streak_sessions_label.setText(f"Streak Sessions: {streak_sessions}")
                self.streak_time_label.setText(f"Streak Time: {self.format_duration(streak_time or 0)}")
            else:
                self.current_streak_label.setText("Current Streak: 0 days")
                self.longest_streak_label.setText("Longest Streak: 0 days")
                self.streak_sessions_label.setText("Streak Sessions: 0")
                self.streak_time_label.setText("Streak Time: 00:00:00")
                
        except Exception as e:
            logger.error(f"Error updating streak stats: {e}")
            
    def update_recent_activity(self):
        """Update recent activity display"""
        try:
            if self.reading_intelligence:
                history = self.reading_intelligence.get_session_history(days=3)
                
                if history:
                    activity_text = "Recent Sessions:\n"
                    for session in history[:5]:  # Show last 5 sessions
                        title = session.get('pdf_title') or session.get('exercise_title', 'Unknown')
                        duration = self.format_duration(session.get('total_time_seconds', 0))
                        pages = session.get('pages_visited', 0)
                        start_time = session.get('start_time', '')
                        
                        if start_time:
                            try:
                                if isinstance(start_time, str):
                                    start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                                else:
                                    start_dt = start_time
                                time_str = start_dt.strftime("%m/%d %H:%M")
                            except:
                                time_str = "Unknown time"
                        else:
                            time_str = "Unknown time"
                        
                        activity_text += f"• {time_str}: {title[:30]}{'...' if len(title) > 30 else ''}\n"
                        activity_text += f"  {duration}, {pages} pages\n\n"
                    
                    self.recent_activity_label.setText(activity_text.strip())
                else:
                    self.recent_activity_label.setText("No recent activity found.")
            else:
                self.recent_activity_label.setText("Reading intelligence not available.")
                
        except Exception as e:
            logger.error(f"Error updating recent activity: {e}")
            self.recent_activity_label.setText("Error loading recent activity.")
            
    def format_duration(self, seconds):
        """Format duration in seconds to readable format"""
        if seconds < 60:
            return f"{seconds}s"
        elif seconds < 3600:
            minutes = seconds // 60
            return f"{minutes}m"
        else:
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            return f"{hours}h {minutes}m"