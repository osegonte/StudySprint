# src/ui/timer_widget.py - Enhanced Version
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                            QPushButton, QProgressBar, QFrame, QGridLayout,
                            QGroupBox, QScrollArea, QTextEdit, QTabWidget,
                            QApplication, QSystemTrayIcon, QMenu)
from PyQt6.QtCore import Qt, QTimer, pyqtSlot, QPropertyAnimation, QRect
from PyQt6.QtGui import QFont, QPixmap, QPainter, QColor, QPen, QIcon
from datetime import datetime, timedelta
import logging
from decimal import Decimal

logger = logging.getLogger(__name__)

class FloatingTimerOverlay(QWidget):
    """Optional floating timer overlay for PDF view"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowFlags(Qt.WindowType.FramelessWindowHint | Qt.WindowType.WindowStaysOnTopHint)
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self.setFixedSize(200, 80)
        
        layout = QVBoxLayout()
        layout.setContentsMargins(10, 5, 10, 5)
        
        self.time_label = QLabel("00:00:00")
        self.time_label.setFont(QFont("Arial", 14, QFont.Weight.Bold))
        self.time_label.setStyleSheet("color: white; background-color: rgba(0, 0, 0, 180); padding: 5px; border-radius: 5px;")
        
        self.pages_label = QLabel("0 pages")
        self.pages_label.setFont(QFont("Arial", 10))
        self.pages_label.setStyleSheet("color: white; background-color: rgba(0, 0, 0, 120); padding: 3px; border-radius: 3px;")
        
        layout.addWidget(self.time_label)
        layout.addWidget(self.pages_label)
        self.setLayout(layout)
        
        # Make draggable
        self.dragging = False
        self.drag_position = None
        
    def update_display(self, time_str, pages_str):
        self.time_label.setText(time_str)
        self.pages_label.setText(pages_str)
        
    def mousePressEvent(self, event):
        if event.button() == Qt.MouseButton.LeftButton:
            self.dragging = True
            self.drag_position = event.globalPosition().toPoint() - self.frameGeometry().topLeft()
            
    def mouseMoveEvent(self, event):
        if self.dragging and self.drag_position:
            self.move(event.globalPosition().toPoint() - self.drag_position)
            
    def mouseReleaseEvent(self, event):
        self.dragging = False


class SessionStatsWidget(QWidget):
    """Detailed session statistics widget"""
    
    def __init__(self):
        super().__init__()
        self.setup_ui()
        
    def setup_ui(self):
        layout = QVBoxLayout()
        
        # Session overview
        overview_group = QGroupBox("📊 Session Overview")
        overview_layout = QGridLayout()
        
        self.session_id_label = QLabel("Session: -")
        self.start_time_label = QLabel("Started: -")
        self.document_label = QLabel("Document: -")
        self.status_label = QLabel("Status: Inactive")
        
        overview_layout.addWidget(QLabel("ID:"), 0, 0)
        overview_layout.addWidget(self.session_id_label, 0, 1)
        overview_layout.addWidget(QLabel("Started:"), 1, 0)
        overview_layout.addWidget(self.start_time_label, 1, 1)
        overview_layout.addWidget(QLabel("Document:"), 2, 0)
        overview_layout.addWidget(self.document_label, 2, 1)
        overview_layout.addWidget(QLabel("Status:"), 3, 0)
        overview_layout.addWidget(self.status_label, 3, 1)
        
        overview_group.setLayout(overview_layout)
        
        # Time breakdown
        time_group = QGroupBox("⏱️ Time Breakdown")
        time_layout = QGridLayout()
        
        self.total_time_label = QLabel("00:00:00")
        self.active_time_label = QLabel("00:00:00")
        self.idle_time_label = QLabel("00:00:00")
        self.efficiency_label = QLabel("0%")
        
        time_layout.addWidget(QLabel("Total Time:"), 0, 0)
        time_layout.addWidget(self.total_time_label, 0, 1)
        time_layout.addWidget(QLabel("Active Time:"), 1, 0)
        time_layout.addWidget(self.active_time_label, 1, 1)
        time_layout.addWidget(QLabel("Idle Time:"), 2, 0)
        time_layout.addWidget(self.idle_time_label, 2, 1)
        time_layout.addWidget(QLabel("Efficiency:"), 3, 0)
        time_layout.addWidget(self.efficiency_label, 3, 1)
        
        time_group.setLayout(time_layout)
        
        # Reading metrics
        reading_group = QGroupBox("📖 Reading Metrics")
        reading_layout = QGridLayout()
        
        self.pages_visited_label = QLabel("0")
        self.current_page_label = QLabel("-")
        self.reading_speed_label = QLabel("0.0 PPM")
        self.avg_page_time_label = QLabel("0s")
        
        reading_layout.addWidget(QLabel("Pages Visited:"), 0, 0)
        reading_layout.addWidget(self.pages_visited_label, 0, 1)
        reading_layout.addWidget(QLabel("Current Page:"), 1, 0)
        reading_layout.addWidget(self.current_page_label, 1, 1)
        reading_layout.addWidget(QLabel("Reading Speed:"), 2, 0)
        reading_layout.addWidget(self.reading_speed_label, 2, 1)
        reading_layout.addWidget(QLabel("Avg Time/Page:"), 3, 0)
        reading_layout.addWidget(self.avg_page_time_label, 3, 1)
        
        reading_group.setLayout(reading_layout)
        
        layout.addWidget(overview_group)
        layout.addWidget(time_group)
        layout.addWidget(reading_group)
        layout.addStretch()
        
        self.setLayout(layout)
        
    def update_stats(self, stats):
        if not stats:
            self.reset_display()
            return
            
        # Update overview
        self.session_id_label.setText(str(stats.get('session_id', '-')))
        
        start_time = stats.get('session_start_time')
        if start_time:
            try:
                dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                self.start_time_label.setText(dt.strftime("%H:%M:%S"))
            except:
                self.start_time_label.setText(start_time)
        else:
            self.start_time_label.setText("-")
            
        doc_type = "Exercise PDF" if stats.get('is_exercise') else "Main PDF"
        self.document_label.setText(doc_type)
        
        if stats.get('is_idle'):
            status = "🟡 Idle" if not stats.get('is_manually_paused') else "⏸️ Paused"
        else:
            status = "🟢 Active"
        self.status_label.setText(status)
        
        # Update time breakdown
        total_time = stats.get('total_time_seconds', 0)
        active_time = stats.get('active_time_seconds', 0)
        idle_time = stats.get('idle_time_seconds', 0)
        
        self.total_time_label.setText(self.format_duration(total_time))
        self.active_time_label.setText(self.format_duration(active_time))
        self.idle_time_label.setText(self.format_duration(idle_time))
        
        efficiency = (active_time / total_time * 100) if total_time > 0 else 0
        self.efficiency_label.setText(f"{efficiency:.1f}%")
        
        # Update reading metrics
        self.pages_visited_label.setText(str(stats.get('pages_visited', 0)))
        self.current_page_label.setText(str(stats.get('current_page', '-')))
        
        speed = stats.get('reading_speed_ppm', 0)
        self.reading_speed_label.setText(f"{speed:.1f} PPM")
        
        avg_time = stats.get('avg_time_per_page', 0)
        self.avg_page_time_label.setText(f"{avg_time:.0f}s")
        
    def reset_display(self):
        self.session_id_label.setText("-")
        self.start_time_label.setText("-")
        self.document_label.setText("-")
        self.status_label.setText("⚫ Inactive")
        self.total_time_label.setText("00:00:00")
        self.active_time_label.setText("00:00:00")
        self.idle_time_label.setText("00:00:00")
        self.efficiency_label.setText("0%")
        self.pages_visited_label.setText("0")
        self.current_page_label.setText("-")
        self.reading_speed_label.setText("0.0 PPM")
        self.avg_page_time_label.setText("0s")
        
    def format_duration(self, seconds):
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"


class TimerWidget(QWidget):
    """Enhanced timer widget with comprehensive session tracking"""
    
    def __init__(self):
        super().__init__()
        self.session_timer = None
        self.reading_intelligence = None
        self.current_session_stats = None
        self.current_pdf_info = None
        self.floating_overlay = None
        self.notification_sounds = True
        
        # UI Update timer
        self.ui_timer = QTimer()
        self.ui_timer.timeout.connect(self.update_displays)
        self.ui_timer.start(1000)  # Update every second
        
        # Notification timer for long idle periods
        self.idle_notification_timer = QTimer()
        self.idle_notification_timer.timeout.connect(self.show_idle_notification)
        
        self.setup_ui()
        self.apply_styles()
        
    def setup_ui(self):
        """Set up the enhanced timer widget UI"""
        layout = QVBoxLayout()
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)
        
        # Create tab widget for different views
        self.tab_widget = QTabWidget()
        
        # Main timer tab
        self.main_tab = self.create_main_timer_tab()
        self.tab_widget.addTab(self.main_tab, "⏱️ Timer")
        
        # Detailed stats tab
        self.stats_widget = SessionStatsWidget()
        self.tab_widget.addTab(self.stats_widget, "📊 Details")
        
        # Estimation tab
        self.estimation_tab = self.create_estimation_tab()
        self.tab_widget.addTab(self.estimation_tab, "🔮 Predictions")
        
        # Controls
        controls_layout = QHBoxLayout()
        
        self.pause_resume_btn = QPushButton("⏸️ Pause")
        self.pause_resume_btn.setEnabled(False)
        self.pause_resume_btn.clicked.connect(self.toggle_pause_resume)
        
        self.end_session_btn = QPushButton("⏹️ End Session")
        self.end_session_btn.setEnabled(False)
        self.end_session_btn.clicked.connect(self.end_current_session)
        
        self.toggle_overlay_btn = QPushButton("👁️ Overlay")
        self.toggle_overlay_btn.clicked.connect(self.toggle_floating_overlay)
        
        controls_layout.addWidget(self.pause_resume_btn)
        controls_layout.addWidget(self.end_session_btn)
        controls_layout.addWidget(self.toggle_overlay_btn)
        
        layout.addWidget(self.tab_widget)
        layout.addLayout(controls_layout)
        
        self.setLayout(layout)
        
    def create_main_timer_tab(self):
        """Create the main timer display tab"""
        widget = QWidget()
        layout = QVBoxLayout()
        
        # Large time display
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
        
        # Status indicator
        self.status_indicator = QLabel("⚫ No Session")
        self.status_indicator.setFont(QFont("Arial", 14, QFont.Weight.Bold))
        self.status_indicator.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        # Quick stats
        quick_stats_group = QGroupBox("📈 Quick Stats")
        quick_stats_layout = QGridLayout()
        
        self.quick_active_time = QLabel("Active: 00:00:00")
        self.quick_pages_count = QLabel("Pages: 0")
        self.quick_reading_speed = QLabel("Speed: 0.0 PPM")
        self.quick_efficiency = QLabel("Efficiency: 0%")
        
        quick_stats_layout.addWidget(self.quick_active_time, 0, 0)
        quick_stats_layout.addWidget(self.quick_pages_count, 0, 1)
        quick_stats_layout.addWidget(self.quick_reading_speed, 1, 0)
        quick_stats_layout.addWidget(self.quick_efficiency, 1, 1)
        
        quick_stats_group.setLayout(quick_stats_layout)
        
        # Progress display
        self.progress_group = QGroupBox("📖 Reading Progress")
        progress_layout = QVBoxLayout()
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        
        self.progress_text = QLabel("No document loaded")
        self.progress_text.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        progress_layout.addWidget(self.progress_bar)
        progress_layout.addWidget(self.progress_text)
        self.progress_group.setLayout(progress_layout)
        
        layout.addWidget(self.main_time_display)
        layout.addWidget(self.status_indicator)
        layout.addWidget(quick_stats_group)
        layout.addWidget(self.progress_group)
        layout.addStretch()
        
        widget.setLayout(layout)
        return widget
        
    def create_estimation_tab(self):
        """Create the time estimation and prediction tab"""
        widget = QWidget()
        layout = QVBoxLayout()
        
        # Time remaining
        remaining_group = QGroupBox("⏳ Time Remaining")
        remaining_layout = QGridLayout()
        
        self.time_remaining_label = QLabel("Calculating...")
        self.estimated_finish_label = QLabel("Finish: -")
        self.sessions_needed_label = QLabel("Sessions: -")
        self.confidence_label = QLabel("Confidence: -")
        
        remaining_layout.addWidget(QLabel("Time Remaining:"), 0, 0)
        remaining_layout.addWidget(self.time_remaining_label, 0, 1)
        remaining_layout.addWidget(QLabel("Est. Finish:"), 1, 0)
        remaining_layout.addWidget(self.estimated_finish_label, 1, 1)
        remaining_layout.addWidget(QLabel("Sessions Needed:"), 2, 0)
        remaining_layout.addWidget(self.sessions_needed_label, 2, 1)
        remaining_layout.addWidget(QLabel("Confidence:"), 3, 0)
        remaining_layout.addWidget(self.confidence_label, 3, 1)
        
        remaining_group.setLayout(remaining_layout)
        
        # Reading insights
        insights_group = QGroupBox("🧠 Reading Insights")
        insights_layout = QVBoxLayout()
        
        self.insights_text = QTextEdit()
        self.insights_text.setMaximumHeight(120)
        self.insights_text.setReadOnly(True)
        self.insights_text.setText("Start reading to see insights...")
        
        insights_layout.addWidget(self.insights_text)
        insights_group.setLayout(insights_layout)
        
        # Daily goals (if implemented)
        goals_group = QGroupBox("🎯 Today's Progress")
        goals_layout = QGridLayout()
        
        self.daily_time_progress = QProgressBar()
        self.daily_pages_label = QLabel("Pages today: 0")
        self.daily_sessions_label = QLabel("Sessions today: 0")
        self.streak_label = QLabel("Streak: 0 days")
        
        goals_layout.addWidget(QLabel("Daily Time Goal:"), 0, 0)
        goals_layout.addWidget(self.daily_time_progress, 0, 1)
        goals_layout.addWidget(self.daily_pages_label, 1, 0)
        goals_layout.addWidget(self.daily_sessions_label, 1, 1)
        goals_layout.addWidget(self.streak_label, 2, 0, 1, 2)
        
        goals_group.setLayout(goals_layout)
        
        layout.addWidget(remaining_group)
        layout.addWidget(insights_group)
        layout.addWidget(goals_group)
        layout.addStretch()
        
        widget.setLayout(layout)
        return widget
    
    def apply_styles(self):
        """Apply enhanced styling"""
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
        """)
        
    def set_session_timer(self, session_timer):
        """Set the session timer instance with enhanced signal connections"""
        self.session_timer = session_timer
        if session_timer:
            # Connect all signals
            session_timer.session_started.connect(self.on_session_started)
            session_timer.session_ended.connect(self.on_session_ended)
            session_timer.session_paused.connect(self.on_session_paused)
            session_timer.session_resumed.connect(self.on_session_resumed)
            session_timer.idle_detected.connect(self.on_idle_state_changed)
            session_timer.stats_updated.connect(self.on_stats_updated)
            session_timer.reading_speed_updated.connect(self.on_reading_speed_updated)
            session_timer.finish_time_estimated.connect(self.on_finish_time_estimated)
            
    def set_reading_intelligence(self, reading_intelligence):
        """Set the reading intelligence instance"""
        self.reading_intelligence = reading_intelligence
        
    def set_current_pdf_info(self, pdf_info, is_exercise=False):
        """Set current PDF information with enhanced tracking"""
        self.current_pdf_info = pdf_info
        if pdf_info:
            pdf_type = "Exercise" if is_exercise else "Main PDF"
            title = pdf_info.get('title', 'Unknown')
            
            # Update progress display
            current_page = pdf_info.get('current_page', 1)
            total_pages = pdf_info.get('total_pages', 1)
            
            self.update_progress_display(current_page, total_pages)
            self.progress_text.setText(f"{pdf_type}: {title}")
            
            # Update estimation display
            self.update_time_estimation(current_page, total_pages)
        else:
            self.reset_progress_display()
            
    def update_progress_display(self, current_page, total_pages):
        """Update the progress display with enhanced visualization"""
        if total_pages > 0:
            # Calculate progress
            progress_percent = ((current_page - 1) / total_pages) * 100
            pages_remaining = total_pages - current_page + 1
            
            # Update progress bar
            self.progress_bar.setMaximum(total_pages)
            self.progress_bar.setValue(current_page - 1)
            self.progress_bar.setVisible(True)
            
            # Update progress text with detailed info
            progress_text = f"Page {current_page} of {total_pages} ({progress_percent:.1f}%)"
            if pages_remaining > 0:
                progress_text += f" • {pages_remaining} pages remaining"
            else:
                progress_text += " • Complete! 🎉"
                
            self.progress_text.setText(progress_text)
            
            # Color-code progress bar based on completion
            if progress_percent >= 100:
                self.progress_bar.setStyleSheet("QProgressBar::chunk { background-color: #28a745; }")
            elif progress_percent >= 75:
                self.progress_bar.setStyleSheet("QProgressBar::chunk { background-color: #17a2b8; }")
            elif progress_percent >= 50:
                self.progress_bar.setStyleSheet("QProgressBar::chunk { background-color: #007acc; }")
            elif progress_percent >= 25:
                self.progress_bar.setStyleSheet("QProgressBar::chunk { background-color: #ffc107; }")
            else:
                self.progress_bar.setStyleSheet("QProgressBar::chunk { background-color: #fd7e14; }")
        else:
            self.reset_progress_display()
            
    def reset_progress_display(self):
        """Reset progress display"""
        self.progress_bar.setVisible(False)
        self.progress_text.setText("No document loaded")
        
    def update_time_estimation(self, current_page, total_pages):
        """Update time estimation display with enhanced predictions"""
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
                # Time remaining
                remaining_minutes = self.safe_float(estimation.get('estimated_minutes', 0))
                if remaining_minutes > 0:
                    if remaining_minutes >= 60:
                        hours = int(remaining_minutes // 60)
                        minutes = int(remaining_minutes % 60)
                        self.time_remaining_label.setText(f"{hours}h {minutes}m")
                    else:
                        self.time_remaining_label.setText(f"{int(remaining_minutes)}m")
                        
                    # Estimated finish time
                    try:
                        finish_time = datetime.now() + timedelta(minutes=remaining_minutes)
                        self.estimated_finish_label.setText(finish_time.strftime("%H:%M"))
                    except:
                        self.estimated_finish_label.setText("Error")
                else:
                    self.time_remaining_label.setText("Complete!")
                    self.estimated_finish_label.setText("Finished")
                
                # Sessions needed
                sessions = estimation.get('sessions_needed', 1)
                self.sessions_needed_label.setText(f"~{sessions} sessions")
                
                # Confidence with color coding
                confidence = estimation.get('confidence', 'low')
                strategy = estimation.get('strategy_used', 'Unknown')
                confidence_text = f"{confidence.title()}"
                
                if confidence == 'high':
                    color = "#28a745"
                elif confidence == 'medium':
                    color = "#ffc107"
                else:
                    color = "#dc3545"
                    
                self.confidence_label.setText(f"<span style='color: {color}'>{confidence_text}</span>")
                
                # Update insights
                self.update_reading_insights(estimation)
            else:
                self.reset_time_estimation()
                
        except Exception as e:
            logger.error(f"Error updating time estimation: {e}")
            self.reset_time_estimation()
            
    def update_reading_insights(self, estimation):
        """Update reading insights with intelligent analysis"""
        insights = []
        
        # Pace analysis
        pace_desc = estimation.get('reading_pace_description', 'moderate pace')
        insights.append(f"📊 You're reading at a {pace_desc}")
        
        # Time prediction accuracy
        confidence = estimation.get('confidence', 'low')
        if confidence == 'high':
            insights.append("🎯 Predictions are highly accurate based on your reading history")
        elif confidence == 'medium':
            insights.append("📈 Predictions are moderately accurate - more data will improve estimates")
        else:
            insights.append("🔍 Limited data available - estimates will improve as you read more")
        
        # Finish date prediction
        finish_info = estimation.get('finish_date_estimate')
        if finish_info:
            days_needed = finish_info.get('days_needed', 0)
            if days_needed <= 1:
                insights.append("🚀 You could finish this today at your current pace!")
            elif days_needed <= 7:
                insights.append(f"📅 At your current pace, you'll finish in {days_needed} days")
            else:
                insights.append(f"⏰ Estimated completion: {days_needed} days at current reading frequency")
        
        # Session optimization
        sessions_needed = estimation.get('sessions_needed', 1)
        if sessions_needed == 1:
            insights.append("⚡ You can finish this in your current session!")
        elif sessions_needed <= 3:
            insights.append(f"📚 Plan for {sessions_needed} more focused sessions to complete")
        else:
            insights.append(f"📖 Break this into {sessions_needed} manageable reading sessions")
        
        self.insights_text.setText("\n".join(insights))
        
    def reset_time_estimation(self):
        """Reset time estimation display"""
        self.time_remaining_label.setText("Calculating...")
        self.estimated_finish_label.setText("-")
        self.sessions_needed_label.setText("-")
        self.confidence_label.setText("-")
        self.insights_text.setText("Start reading to see insights...")
        
    def update_daily_stats(self):
        """Update daily statistics display"""
        if not self.reading_intelligence:
            return
            
        try:
            stats = self.reading_intelligence.get_daily_stats()
            if stats:
                # Daily time goal progress
                goal_progress = stats.get('daily_goal_progress', {})
                target_minutes = goal_progress.get('target_minutes', 60)
                actual_minutes = goal_progress.get('actual_minutes', 0)
                progress_percent = goal_progress.get('progress_percent', 0)
                
                self.daily_time_progress.setMaximum(100)
                self.daily_time_progress.setValue(int(progress_percent))
                self.daily_time_progress.setFormat(f"{actual_minutes:.0f}/{target_minutes} min ({progress_percent:.0f}%)")
                
                # Other daily stats
                sessions_count = stats.get('sessions_count', 0)
                total_pages = stats.get('total_pages_read', 0)
                
                self.daily_sessions_label.setText(f"Sessions today: {sessions_count}")
                self.daily_pages_label.setText(f"Pages today: {total_pages}")
                
            # Get streak info
            streak_data = self.reading_intelligence.get_streak_analytics()
            if streak_data:
                current_streak = streak_data.get('current_streak_days', 0)
                streak_quality = streak_data.get('streak_quality', 'none')
                self.streak_label.setText(f"Streak: {current_streak} days ({streak_quality})")
            else:
                self.streak_label.setText("Streak: 0 days")
                
        except Exception as e:
            logger.error(f"Error updating daily stats: {e}")
            
    def format_duration(self, seconds):
        """Format duration in seconds to HH:MM:SS"""
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
        
    def safe_float(self, value):
        """Safely convert Decimal/None values to float"""
        if value is None:
            return 0.0
        if isinstance(value, Decimal):
            return float(value)
        return float(value)
        
    def update_displays(self):
        """Update all time displays and floating overlay"""
        if self.current_session_stats:
            total_time = self.current_session_stats.get('total_time_seconds', 0)
            active_time = self.current_session_stats.get('active_time_seconds', 0)
            idle_time = self.current_session_stats.get('idle_time_seconds', 0)
            pages_visited = self.current_session_stats.get('pages_visited', 0)
            reading_speed = self.current_session_stats.get('reading_speed_ppm', 0)
            
            # Update main time display
            time_str = self.format_duration(total_time)
            self.main_time_display.setText(time_str)
            
            # Update quick stats
            self.quick_active_time.setText(f"Active: {self.format_duration(active_time)}")
            self.quick_pages_count.setText(f"Pages: {pages_visited}")
            self.quick_reading_speed.setText(f"Speed: {reading_speed:.1f} PPM")
            
            efficiency = (active_time / total_time * 100) if total_time > 0 else 0
            self.quick_efficiency.setText(f"Efficiency: {efficiency:.0f}%")
            
            # Update floating overlay if visible
            if self.floating_overlay and self.floating_overlay.isVisible():
                self.floating_overlay.update_display(
                    time_str,
                    f"{pages_visited} pages"
                )
        
        # Update daily stats periodically (every 30 seconds)
        if hasattr(self, '_last_daily_update'):
            if (datetime.now() - self._last_daily_update).seconds > 30:
                self.update_daily_stats()
                self._last_daily_update = datetime.now()
        else:
            self.update_daily_stats()
            self._last_daily_update = datetime.now()
    
    def toggle_pause_resume(self):
        """Toggle pause/resume session with enhanced feedback"""
        if not self.session_timer:
            return
            
        if self.current_session_stats and self.current_session_stats.get('is_idle', False):
            self.session_timer.resume_session()
        else:
            self.session_timer.pause_session(manual=True)
    
    def end_current_session(self):
        """End the current session with confirmation"""
        if self.session_timer and self.current_session_stats:
            # Could add confirmation dialog here
            self.session_timer.end_session()
    
    def toggle_floating_overlay(self):
        """Toggle the floating timer overlay"""
        if self.floating_overlay is None:
            self.floating_overlay = FloatingTimerOverlay()
            
        if self.floating_overlay.isVisible():
            self.floating_overlay.hide()
            self.toggle_overlay_btn.setText("👁️ Show Overlay")
        else:
            self.floating_overlay.show()
            # Position overlay in top-right corner
            screen = QApplication.primaryScreen().geometry()
            self.floating_overlay.move(screen.width() - 220, 20)
            self.toggle_overlay_btn.setText("👁️ Hide Overlay")
    
    def show_idle_notification(self):
        """Show notification for extended idle periods"""
        if self.current_session_stats and self.current_session_stats.get('is_idle'):
            # Could implement system tray notification here
            logger.info("📢 Extended idle period detected")
    
    # Signal handlers
    @pyqtSlot(int)
    def on_session_started(self, session_id):
        """Handle session started with enhanced feedback"""
        self.status_indicator.setText("🟢 Session Active")
        self.pause_resume_btn.setEnabled(True)
        self.end_session_btn.setEnabled(True)
        self.pause_resume_btn.setText("⏸️ Pause")
        
        # Start idle notification monitoring
        self.idle_notification_timer.start(300000)  # 5 minutes
        
        logger.info(f"✅ Timer widget: Session {session_id} started")
    
    @pyqtSlot(int, dict)
    def on_session_ended(self, session_id, stats):
        """Handle session ended with comprehensive cleanup"""
        self.status_indicator.setText("⚫ No Session")
        self.pause_resume_btn.setEnabled(False)
        self.end_session_btn.setEnabled(False)
        self.pause_resume_btn.setText("⏸️ Pause")
        
        # Stop timers
        self.idle_notification_timer.stop()
        
        # Reset displays
        self.main_time_display.setText("00:00:00")
        self.quick_active_time.setText("Active: 00:00:00")
        self.quick_pages_count.setText("Pages: 0")
        self.quick_reading_speed.setText("Speed: 0.0 PPM")
        self.quick_efficiency.setText("Efficiency: 0%")
        
        # Hide floating overlay
        if self.floating_overlay and self.floating_overlay.isVisible():
            self.floating_overlay.hide()
        
        # Reset session stats
        self.current_session_stats = None
        self.stats_widget.reset_display()
        
        # Update daily stats after session ends
        self.update_daily_stats()
        
        # Show session summary
        if stats:
            total_time = stats.get('total_time_seconds', 0)
            pages_visited = stats.get('pages_visited', 0)
            logger.info(f"📊 Session {session_id} ended: {self.format_duration(total_time)}, {pages_visited} pages")
    
    @pyqtSlot(int, bool)
    def on_session_paused(self, session_id, is_manual):
        """Handle session paused"""
        if is_manual:
            self.status_indicator.setText("⏸️ Manually Paused")
            self.pause_resume_btn.setText("▶️ Resume")
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
        """Handle idle state changes with visual feedback"""
        if is_idle:
            self.status_indicator.setText("🟡 Idle Detected")
            self.pause_resume_btn.setText("▶️ Resume")
            # Could add visual effects here (blinking, color change, etc.)
        else:
            self.status_indicator.setText("🟢 Session Active")
            self.pause_resume_btn.setText("⏸️ Pause")
    
    @pyqtSlot(dict)
    def on_stats_updated(self, stats):
        """Handle comprehensive stats updates"""
        self.current_session_stats = stats
        
        # Update detailed stats widget
        self.stats_widget.update_stats(stats)
        
        # Update page progress if available
        if self.current_pdf_info:
            current_page = stats.get('current_page', 1)
            total_pages = self.current_pdf_info.get('total_pages', 1)
            self.update_progress_display(current_page, total_pages)
            self.update_time_estimation(current_page, total_pages)
    
    @pyqtSlot(dict)
    def on_reading_speed_updated(self, speed_metrics):
        """Handle reading speed updates"""
        current_speed = speed_metrics.get('current_speed_ppm', 0)
        efficiency = speed_metrics.get('efficiency_percent', 0)
        
        logger.debug(f"📈 Reading speed updated: {current_speed:.2f} PPM, {efficiency:.1f}% efficiency")
    
    @pyqtSlot(dict)
    def on_finish_time_estimated(self, estimation):
        """Handle finish time estimation updates"""
        if self.current_pdf_info:
            current_page = self.current_session_stats.get('current_page', 1) if self.current_session_stats else 1
            total_pages = self.current_pdf_info.get('total_pages', 1)
            self.update_time_estimation(current_page, total_pages)

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
        
    def safe_float(self, value):
        """Safely convert Decimal/None values to float"""
        if value is None:
            return 0.0
        if isinstance(value, Decimal):
            return float(value)
        return float(value)
        
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
                total_time = self.safe_float(metrics.get('total_time_spent_seconds', 0))
                total_pages = metrics.get('total_pages_read', 0) or 0
                
                self.total_study_time_label.setText(f"Total Study Time: {self.format_duration(int(total_time))}")
                self.total_pages_label.setText(f"Total Pages Read: {total_pages:,}")
            else:
                self.total_study_time_label.setText("Total Study Time: No data")
                self.total_pages_label.setText("Total Pages Read: No data")
            
            # Get session count from database
            try:
                self.total_sessions_label.setText("Total Sessions: Computing...")
                self.avg_session_time_label.setText("Avg Session Time: Computing...")
            except:
                self.total_sessions_label.setText("Total Sessions: No data")
                self.avg_session_time_label.setText("Avg Session Time: No data")
                
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
                    week_time = sum(self.safe_float(session.get('total_time_seconds', 0)) for session in history)
                    week_pages = sum(session.get('pages_visited', 0) or 0 for session in history)
                    daily_avg = week_time / 7 if week_time > 0 else 0
                    
                    self.week_sessions_label.setText(f"Sessions: {week_sessions}")
                    self.week_time_label.setText(f"Study Time: {self.format_duration(int(week_time))}")
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
                    speed = self.safe_float(metrics.get('pages_per_minute', 0))
                    
                    self.overall_speed_label.setText(f"Overall Speed: {speed:.2f} pages/min")
                    
                    # Calculate efficiency (placeholder)
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
                current_streak = streaks.get('current_streak_days', 0) or 0
                streak_sessions = streaks.get('streak_sessions', 0) or 0
                streak_time = self.safe_float(streaks.get('streak_total_time', 0))
                
                self.current_streak_label.setText(f"Current Streak: {current_streak} days")
                self.longest_streak_label.setText(f"Longest Streak: {current_streak} days")  # Placeholder
                self.streak_sessions_label.setText(f"Streak Sessions: {streak_sessions}")
                self.streak_time_label.setText(f"Streak Time: {self.format_duration(int(streak_time))}")
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
                        duration = self.format_duration(int(self.safe_float(session.get('total_time_seconds', 0))))
                        pages = session.get('pages_visited', 0) or 0
                        start_time = session.get('start_time', '')
                        
                        if start_time:
                            try:
                                if isinstance(start_time, str):
                                    try:
                                        start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                                    except:
                                        start_dt = datetime.now()
                                elif hasattr(start_time, 'strftime'):
                                    start_dt = start_time
                                else:
                                    start_dt = datetime.now()
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
