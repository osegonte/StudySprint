# src/ui/goals_widget.py - Comprehensive Goals UI
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
                            QPushButton, QLabel, QComboBox, QSpinBox, QDateEdit,
                            QGroupBox, QScrollArea, QProgressBar, QFrame,
                            QDialog, QFormLayout, QRadioButton, QButtonGroup,
                            QTextEdit, QTabWidget, QMessageBox, QSizePolicy,
                            QApplication, QCheckBox)
from PyQt6.QtCore import Qt, QDate, QTimer, pyqtSignal, pyqtSlot
from PyQt6.QtGui import QFont, QPixmap, QPainter, QColor, QPen, QBrush, QIcon
from datetime import datetime, date, timedelta
import logging

from utils.goals_manager import GoalsManager, GoalType, GoalStatus

logger = logging.getLogger(__name__)

class CreateGoalDialog(QDialog):
    """Dialog for creating new study goals"""
    
    goal_created = pyqtSignal(dict)
    
    def __init__(self, db_manager, topics, parent=None):
        super().__init__(parent)
        self.db_manager = db_manager
        self.topics = topics
        self.goals_manager = GoalsManager(db_manager)
        
        self.setWindowTitle("Create Study Goal")
        self.setMinimumSize(500, 400)
        self.setup_ui()
        self.connect_signals()
    
    def setup_ui(self):
        layout = QVBoxLayout()
        
        # Header
        header = QLabel("🎯 Create New Study Goal")
        header.setFont(QFont("Arial", 16, QFont.Weight.Bold))
        header.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(header)
        
        # Form layout
        form_group = QGroupBox("Goal Details")
        form_layout = QFormLayout()
        
        # Topic selection
        self.topic_combo = QComboBox()
        self.topic_combo.addItem("Select a topic...", None)
        for topic in self.topics:
            self.topic_combo.addItem(f"📁 {topic['name']}", topic['id'])
        form_layout.addRow("📚 Topic:", self.topic_combo)
        
        # Goal type selection
        goal_type_group = QGroupBox("Goal Type")
        goal_type_layout = QVBoxLayout()
        
        self.goal_type_group = QButtonGroup()
        
        self.finish_by_date_radio = QRadioButton("📅 Finish topic by specific date")
        self.finish_by_date_radio.setChecked(True)
        self.goal_type_group.addButton(self.finish_by_date_radio, 0)
        goal_type_layout.addWidget(self.finish_by_date_radio)
        
        self.daily_time_radio = QRadioButton("⏰ Study for X minutes daily")
        self.goal_type_group.addButton(self.daily_time_radio, 1)
        goal_type_layout.addWidget(self.daily_time_radio)
        
        self.daily_pages_radio = QRadioButton("📄 Read X pages daily")
        self.goal_type_group.addButton(self.daily_pages_radio, 2)
        goal_type_layout.addWidget(self.daily_pages_radio)
        
        goal_type_group.setLayout(goal_type_layout)
        form_layout.addRow(goal_type_group)
        
        # Target value inputs (conditional)
        self.target_frame = QFrame()
        target_layout = QFormLayout()
        
        # Deadline selector (for finish_by_date)
        self.deadline_date = QDateEdit()
        self.deadline_date.setCalendarPopup(True)
        self.deadline_date.setDate(QDate.currentDate().addDays(30))  # Default 30 days
        self.deadline_date.setMinimumDate(QDate.currentDate().addDays(1))
        target_layout.addRow("📅 Deadline:", self.deadline_date)
        
        # Minutes selector (for daily_time)
        self.minutes_spin = QSpinBox()
        self.minutes_spin.setRange(5, 480)  # 5 minutes to 8 hours
        self.minutes_spin.setValue(30)
        self.minutes_spin.setSuffix(" minutes")
        target_layout.addRow("⏰ Daily Time:", self.minutes_spin)
        
        # Pages selector (for daily_pages)
        self.pages_spin = QSpinBox()
        self.pages_spin.setRange(1, 100)
        self.pages_spin.setValue(5)
        self.pages_spin.setSuffix(" pages")
        target_layout.addRow("📄 Daily Pages:", self.pages_spin)
        
        self.target_frame.setLayout(target_layout)
        form_layout.addRow(self.target_frame)
        
        # Goal preview
        self.preview_label = QLabel()
        self.preview_label.setWordWrap(True)
        self.preview_label.setStyleSheet("""
            QLabel {
                background-color: #f0f8ff;
                border: 2px solid #007acc;
                border-radius: 6px;
                padding: 10px;
                color: #003d66;
                font-weight: bold;
            }
        """)
        form_layout.addRow("📋 Preview:", self.preview_label)
        
        form_group.setLayout(form_layout)
        layout.addWidget(form_group)
        
        # Buttons
        button_layout = QHBoxLayout()
        
        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        
        self.create_btn = QPushButton("🎯 Create Goal")
        self.create_btn.clicked.connect(self.create_goal)
        self.create_btn.setStyleSheet("""
            QPushButton {
                background-color: #28a745;
                color: white;
                font-weight: bold;
                padding: 10px 20px;
                border-radius: 6px;
                border: none;
            }
            QPushButton:hover {
                background-color: #1e7e34;
            }
        """)
        
        button_layout.addStretch()
        button_layout.addWidget(cancel_btn)
        button_layout.addWidget(self.create_btn)
        
        layout.addLayout(button_layout)
        self.setLayout(layout)
        
        # Initial UI state
        self.update_ui_state()
        self.update_preview()
    
    def connect_signals(self):
        """Connect UI signals"""
        self.goal_type_group.buttonClicked.connect(self.update_ui_state)
        self.goal_type_group.buttonClicked.connect(self.update_preview)
        self.topic_combo.currentIndexChanged.connect(self.update_preview)
        self.deadline_date.dateChanged.connect(self.update_preview)
        self.minutes_spin.valueChanged.connect(self.update_preview)
        self.pages_spin.valueChanged.connect(self.update_preview)
    
    def update_ui_state(self):
        """Update UI based on selected goal type"""
        selected_id = self.goal_type_group.checkedId()
        
        # Show/hide appropriate inputs
        self.deadline_date.setVisible(selected_id == 0)  # finish_by_date
        self.minutes_spin.setVisible(selected_id == 1)   # daily_time
        self.pages_spin.setVisible(selected_id == 2)     # daily_pages
        
        # Update labels
        for i in range(self.target_frame.layout().rowCount()):
            item = self.target_frame.layout().itemAt(i, QFormLayout.ItemRole.LabelRole)
            if item and item.widget():
                label = item.widget()
                if selected_id == 0 and "📅" in label.text():
                    label.setVisible(True)
                elif selected_id == 1 and "⏰" in label.text():
                    label.setVisible(True)
                elif selected_id == 2 and "📄" in label.text():
                    label.setVisible(True)
                else:
                    label.setVisible(False)
    
    def update_preview(self):
        """Update goal preview text"""
        topic_index = self.topic_combo.currentIndex()
        if topic_index <= 0:
            self.preview_label.setText("Please select a topic to see goal preview")
            self.create_btn.setEnabled(False)
            return
        
        topic_name = self.topic_combo.currentText().replace("📁 ", "")
        selected_id = self.goal_type_group.checkedId()
        
        if selected_id == 0:  # finish_by_date
            qdate = self.deadline_date.date()
            deadline = date(qdate.year(), qdate.month(), qdate.day())
            days_until = (deadline - date.today()).days
            preview_text = f"🎯 Finish all PDFs in '{topic_name}' by {deadline.strftime('%B %d, %Y')} ({days_until} days from now)"
        elif selected_id == 1:  # daily_time
            minutes = self.minutes_spin.value()
            preview_text = f"⏰ Study '{topic_name}' for {minutes} minutes every day"
        else:  # daily_pages
            pages = self.pages_spin.value()
            preview_text = f"📄 Read {pages} pages from '{topic_name}' every day"
        
        self.preview_label.setText(preview_text)
        self.create_btn.setEnabled(True)
    
    def create_goal(self):
        """Create the goal"""
        try:
            topic_id = self.topic_combo.currentData()
            if not topic_id:
                QMessageBox.warning(self, "Validation Error", "Please select a topic")
                return
            
            selected_id = self.goal_type_group.checkedId()
            
            if selected_id == 0:  # finish_by_date
                goal_type = GoalType.FINISH_BY_DATE
                target_value = 0  # Placeholder value for deadline goals
                qdate = self.deadline_date.date()
                deadline = date(qdate.year(), qdate.month(), qdate.day())
            elif selected_id == 1:  # daily_time
                goal_type = GoalType.DAILY_TIME
                target_value = self.minutes_spin.value()
                deadline = None
            else:  # daily_pages
                goal_type = GoalType.DAILY_PAGES
                target_value = self.pages_spin.value()
                deadline = None
            
            # Create goal
            goal_id = self.goals_manager.create_goal(
                topic_id=topic_id,
                target_type=goal_type,
                target_value=target_value,
                deadline=deadline
            )
            
            if goal_id:
                QMessageBox.information(self, "Success", "Goal created successfully!")
                self.goal_created.emit({
                    'id': goal_id,
                    'topic_id': topic_id,
                    'target_type': goal_type.value,
                    'target_value': target_value,
                    'deadline': deadline
                })
                self.accept()
            else:
                QMessageBox.warning(self, "Error", "Failed to create goal. You may already have a similar goal for this topic.")
                
        except Exception as e:
            logger.error(f"Error creating goal: {e}")
            QMessageBox.critical(self, "Error", f"Failed to create goal: {str(e)}")

class GoalCard(QFrame):
    """Individual goal display card"""
    
    goal_clicked = pyqtSignal(int)  # goal_id
    goal_modified = pyqtSignal(int)  # goal_id
    
    def __init__(self, goal_data, daily_plan=None):
        super().__init__()
        self.goal_data = goal_data
        self.daily_plan = daily_plan
        self.setup_ui()
        self.setMouseTracking(True)
    
    def setup_ui(self):
        """Set up the goal card UI"""
        self.setFrameStyle(QFrame.Shape.Box)
        self.setLineWidth(2)
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        
        layout = QVBoxLayout()
        layout.setContentsMargins(15, 15, 15, 15)
        
        # Header
        header_layout = QHBoxLayout()
        
        # Goal type icon and title
        goal_type = self.goal_data['target_type']
        if goal_type == 'finish_by_date':
            icon = "📅"
            type_text = "Finish by Date"
        elif goal_type == 'daily_time':
            icon = "⏰"
            type_text = "Daily Time"
        else:
            icon = "📄"
            type_text = "Daily Pages"
        
        title_label = QLabel(f"{icon} {type_text}")
        title_label.setFont(QFont("Arial", 14, QFont.Weight.Bold))
        
        status_label = self._create_status_label()
        
        header_layout.addWidget(title_label)
        header_layout.addStretch()
        header_layout.addWidget(status_label)
        
        layout.addLayout(header_layout)
        
        # Topic name
        topic_label = QLabel(f"📚 {self.goal_data['topic_name']}")
        topic_label.setFont(QFont("Arial", 12))
        layout.addWidget(topic_label)
        
        # Progress bar
        progress_bar = QProgressBar()
        progress_percentage = self.goal_data.get('progress_percentage', 0)
        progress_bar.setValue(int(progress_percentage))
        progress_bar.setFormat(f"{progress_percentage:.1f}%")
        layout.addWidget(progress_bar)
        
        # Goal details
        details_layout = QVBoxLayout()
        
        if goal_type == 'finish_by_date':
            deadline = self.goal_data['deadline']
            if isinstance(deadline, str):
                deadline = datetime.fromisoformat(deadline).date()
            days_remaining = (deadline - date.today()).days
            
            details_layout.addWidget(QLabel(f"📅 Deadline: {deadline.strftime('%B %d, %Y')}"))
            details_layout.addWidget(QLabel(f"⏳ Days remaining: {days_remaining}"))
            
            if self.daily_plan:
                details_layout.addWidget(QLabel(f"📖 Pages needed daily: {self.daily_plan.adjusted_daily_target}"))
                
        elif goal_type == 'daily_time':
            details_layout.addWidget(QLabel(f"⏰ Target: {self.goal_data['target_value']} minutes/day"))
            total_time = self.goal_data.get('total_time_spent', 0)
            details_layout.addWidget(QLabel(f"📊 Total time: {total_time} minutes"))
            
        else:  # daily_pages
            details_layout.addWidget(QLabel(f"📄 Target: {self.goal_data['target_value']} pages/day"))
            total_pages = self.goal_data.get('total_pages_read', 0)
            details_layout.addWidget(QLabel(f"📊 Total pages: {total_pages}"))
        
        layout.addLayout(details_layout)
        
        # Daily plan message
        if self.daily_plan and self.daily_plan.message:
            message_label = QLabel(self.daily_plan.message)
            message_label.setWordWrap(True)
            message_label.setStyleSheet("""
                QLabel {
                    background-color: #f8f9fa;
                    border: 1px solid #dee2e6;
                    border-radius: 4px;
                    padding: 8px;
                    margin-top: 5px;
                }
            """)
            layout.addWidget(message_label)
        
        self.setLayout(layout)
        self._apply_status_styling()
    
    def _create_status_label(self):
        """Create status indicator label"""
        status = self.goal_data.get('status', 'on_track')
        
        status_icons = {
            'on_track': ('🟢', 'On Track'),
            'slightly_behind': ('🟡', 'Slightly Behind'),
            'behind': ('🟠', 'Behind'),
            'very_behind': ('🔴', 'Very Behind'),
            'ahead': ('🟦', 'Ahead'),
            'completed': ('✅', 'Completed')
        }
        
        icon, text = status_icons.get(status, ('⚪', 'Unknown'))
        label = QLabel(f"{icon} {text}")
        label.setFont(QFont("Arial", 10, QFont.Weight.Bold))
        
        return label
    
    def _apply_status_styling(self):
        """Apply styling based on goal status"""
        status = self.goal_data.get('status', 'on_track')
        
        style_map = {
            'on_track': "#28a745",
            'slightly_behind': "#ffc107", 
            'behind': "#fd7e14",
            'very_behind': "#dc3545",
            'ahead': "#17a2b8",
            'completed': "#6f42c1"
        }
        
        color = style_map.get(status, "#6c757d")
        
        self.setStyleSheet(f"""
            GoalCard {{
                border: 2px solid {color};
                border-radius: 8px;
                background-color: white;
            }}
            GoalCard:hover {{
                background-color: #f8f9fa;
                border-color: {color};
            }}
        """)
    
    def mousePressEvent(self, event):
        """Handle click events"""
        if event.button() == Qt.MouseButton.LeftButton:
            self.goal_clicked.emit(self.goal_data['id'])
        super().mousePressEvent(event)

class DailyProgressWidget(QWidget):
    """Widget showing today's progress across all goals"""
    
    def __init__(self, goals_manager):
        super().__init__()
        self.goals_manager = goals_manager
        self.setup_ui()
        
        # Auto-refresh every 5 minutes
        self.refresh_timer = QTimer()
        self.refresh_timer.timeout.connect(self.refresh_progress)
        self.refresh_timer.start(300000)  # 5 minutes
    
    def setup_ui(self):
        """Set up daily progress UI"""
        layout = QVBoxLayout()
        layout.setContentsMargins(10, 10, 10, 10)
        
        # Header
        header_layout = QHBoxLayout()
        
        title = QLabel("📅 Today's Progress")
        title.setFont(QFont("Arial", 16, QFont.Weight.Bold))
        
        self.refresh_btn = QPushButton("🔄")
        self.refresh_btn.setMaximumWidth(40)
        self.refresh_btn.clicked.connect(self.refresh_progress)
        self.refresh_btn.setToolTip("Refresh progress")
        
        header_layout.addWidget(title)
        header_layout.addStretch()
        header_layout.addWidget(self.refresh_btn)
        
        layout.addLayout(header_layout)
        
        # Overall status
        self.overall_status = QLabel()
        self.overall_status.setFont(QFont("Arial", 14, QFont.Weight.Bold))
        self.overall_status.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.overall_status)
        
        # Progress list
        self.progress_scroll = QScrollArea()
        self.progress_widget = QWidget()
        self.progress_layout = QVBoxLayout()
        self.progress_widget.setLayout(self.progress_layout)
        self.progress_scroll.setWidget(self.progress_widget)
        self.progress_scroll.setWidgetResizable(True)
        
        layout.addWidget(self.progress_scroll)
        
        self.setLayout(layout)
        self.refresh_progress()
    
    def refresh_progress(self):
        """Refresh today's progress display"""
        try:
            # Clear existing progress items
            for i in reversed(range(self.progress_layout.count())):
                child = self.progress_layout.itemAt(i).widget()
                if child:
                    child.setParent(None)
            
            # Get today's progress
            today_progress = self.goals_manager.get_today_progress()
            
            # Update overall status
            self._update_overall_status(today_progress['overall_status'])
            
            # Add daily goals
            for goal in today_progress['daily_goals']:
                self._add_daily_goal_item(goal)
            
            # Add deadline goals
            for goal in today_progress['deadline_goals']:
                self._add_deadline_goal_item(goal)
                
            if not today_progress['daily_goals'] and not today_progress['deadline_goals']:
                no_goals_label = QLabel("No active goals found.\nCreate your first goal to get started!")
                no_goals_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
                no_goals_label.setStyleSheet("color: #6c757d; padding: 20px;")
                self.progress_layout.addWidget(no_goals_label)
            
        except Exception as e:
            logger.error(f"Error refreshing daily progress: {e}")
            error_label = QLabel("Error loading progress data")
            error_label.setStyleSheet("color: #dc3545; padding: 20px;")
            self.progress_layout.addWidget(error_label)
    
    def _update_overall_status(self, status):
        """Update overall daily status display"""
        status_messages = {
            'all_completed': ('🎉', 'All daily goals completed!', '#28a745'),
            'mostly_completed': ('👍', 'Most goals completed', '#17a2b8'),
            'partially_completed': ('⚡', 'Keep going!', '#ffc107'),
            'none_completed': ('💪', 'Let\'s get started!', '#fd7e14'),
            'no_goals': ('🎯', 'Create goals to track progress', '#6c757d')
        }
        
        icon, message, color = status_messages.get(status, ('ℹ️', 'Unknown status', '#6c757d'))
        
        self.overall_status.setText(f"{icon} {message}")
        self.overall_status.setStyleSheet(f"""
            QLabel {{
                color: {color};
                background-color: {color}20;
                border-radius: 6px;
                padding: 10px;
                margin: 5px 0px;
            }}
        """)
    
    def _add_daily_goal_item(self, goal):
        """Add a daily goal progress item"""
        item_frame = QFrame()
        item_frame.setFrameStyle(QFrame.Shape.Box)
        item_frame.setLineWidth(1)
        
        layout = QHBoxLayout()
        layout.setContentsMargins(10, 8, 10, 8)
        
        # Goal info
        goal_type = "⏰" if goal['target_type'] == 'daily_time' else "📄"
        goal_label = QLabel(f"{goal_type} {goal['topic_name']}")
        goal_label.setFont(QFont("Arial", 12, QFont.Weight.Bold))
        
        # Progress
        if goal['target_type'] == 'daily_time':
            current = goal['time_spent_today']
            target = goal['target_value']
            unit = "min"
        else:
            current = goal['pages_read_today']
            target = goal['target_value']
            unit = "pages"
        
        progress_label = QLabel(f"{current}/{target} {unit}")
        
        # Progress bar
        progress_bar = QProgressBar()
        progress_bar.setMaximum(target)
        progress_bar.setValue(current)
        progress_bar.setMaximumHeight(20)
        
        # Status
        status_label = self._create_daily_status_label(goal)
        
        layout.addWidget(goal_label)
        layout.addWidget(progress_bar)
        layout.addWidget(progress_label)
        layout.addWidget(status_label)
        
        item_frame.setLayout(layout)
        self.progress_layout.addWidget(item_frame)
    
    def _add_deadline_goal_item(self, goal):
        """Add a deadline goal progress item"""
        item_frame = QFrame()
        item_frame.setFrameStyle(QFrame.Shape.Box)
        item_frame.setLineWidth(1)
        
        layout = QVBoxLayout()
        layout.setContentsMargins(10, 8, 10, 8)
        
        # Header
        header_layout = QHBoxLayout()
        goal_label = QLabel(f"📅 {goal['topic_name']}")
        goal_label.setFont(QFont("Arial", 12, QFont.Weight.Bold))
        
        status_label = QLabel("📊 Deadline Goal")
        status_label.setFont(QFont("Arial", 10))
        
        header_layout.addWidget(goal_label)
        header_layout.addStretch()
        header_layout.addWidget(status_label)
        
        layout.addLayout(header_layout)
        
        # Today's contribution
        contribution_label = QLabel(f"Today: {goal['pages_read_today']} pages, {goal['time_spent_today']} minutes")
        layout.addWidget(contribution_label)
        
        item_frame.setLayout(layout)
        self.progress_layout.addWidget(item_frame)
    
    def _create_daily_status_label(self, goal):
        """Create status label for daily goals"""
        status = goal.get('status', 'not_started')
        
        status_info = {
            'completed': ('✅', '#28a745'),
            'almost_done': ('🔥', '#17a2b8'),
            'halfway': ('⚡', '#ffc107'),
            'started': ('📚', '#fd7e14'),
            'not_started': ('💤', '#6c757d')
        }
        
        icon, color = status_info.get(status, ('❓', '#6c757d'))
        
        label = QLabel(icon)
        label.setStyleSheet(f"color: {color}; font-size: 16px;")
        label.setToolTip(status.replace('_', ' ').title())
        
        return label

class GoalsAnalyticsWidget(QWidget):
    """Widget for displaying goal analytics and insights"""
    
    def __init__(self, goals_manager):
        super().__init__()
        self.goals_manager = goals_manager
        self.setup_ui()
    
    def setup_ui(self):
        """Set up analytics UI"""
        layout = QVBoxLayout()
        
        # Header
        header = QLabel("📊 Goals Analytics")
        header.setFont(QFont("Arial", 16, QFont.Weight.Bold))
        layout.addWidget(header)
        
        # Tab widget for different analytics views
        self.tabs = QTabWidget()
        
        # Overview tab
        self.overview_tab = self._create_overview_tab()
        self.tabs.addTab(self.overview_tab, "📈 Overview")
        
        # Trends tab
        self.trends_tab = self._create_trends_tab()
        self.tabs.addTab(self.trends_tab, "📉 Trends")
        
        # Insights tab
        self.insights_tab = self._create_insights_tab()
        self.tabs.addTab(self.insights_tab, "💡 Insights")
        
        layout.addWidget(self.tabs)
        self.setLayout(layout)
    
    def _create_overview_tab(self):
        """Create overview analytics tab"""
        widget = QWidget()
        layout = QVBoxLayout()
        
        # Summary cards
        summary_layout = QGridLayout()
        
        self.active_goals_card = self._create_metric_card("🎯", "Active Goals", "0")
        self.completed_goals_card = self._create_metric_card("✅", "Completed", "0") 
        self.success_rate_card = self._create_metric_card("📈", "Success Rate", "0%")
        self.streak_card = self._create_metric_card("🔥", "Current Streak", "0 days")
        
        summary_layout.addWidget(self.active_goals_card, 0, 0)
        summary_layout.addWidget(self.completed_goals_card, 0, 1)
        summary_layout.addWidget(self.success_rate_card, 1, 0)
        summary_layout.addWidget(self.streak_card, 1, 1)
        
        layout.addLayout(summary_layout)
        
        # Recent activity
        activity_group = QGroupBox("Recent Activity")
        self.activity_list = QVBoxLayout()
        activity_group.setLayout(self.activity_list)
        layout.addWidget(activity_group)
        
        widget.setLayout(layout)
        return widget
    
    def _create_trends_tab(self):
        """Create trends analytics tab"""
        widget = QWidget()
        layout = QVBoxLayout()
        
        trends_label = QLabel("📊 Trend analysis coming soon...")
        trends_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(trends_label)
        
        widget.setLayout(layout)
        return widget
    
    def _create_insights_tab(self):
        """Create insights tab"""
        widget = QWidget()
        layout = QVBoxLayout()
        
        self.insights_text = QTextEdit()
        self.insights_text.setReadOnly(True)
        layout.addWidget(self.insights_text)
        
        widget.setLayout(layout)
        return widget
    
    def _create_metric_card(self, icon, title, value):
        """Create a metric display card"""
        card = QFrame()
        card.setFrameStyle(QFrame.Shape.Box)
        card.setLineWidth(1)
        card.setStyleSheet("""
            QFrame {
                background-color: white;
                border: 2px solid #dee2e6;
                border-radius: 8px;
                padding: 10px;
            }
        """)
        
        layout = QVBoxLayout()
        
        icon_label = QLabel(icon)
        icon_label.setFont(QFont("Arial", 24))
        icon_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        title_label = QLabel(title)
        title_label.setFont(QFont("Arial", 12))
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        value_label = QLabel(value)
        value_label.setFont(QFont("Arial", 18, QFont.Weight.Bold))
        value_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        value_label.setObjectName("value_label")  # For easy updates
        
        layout.addWidget(icon_label)
        layout.addWidget(title_label)
        layout.addWidget(value_label)
        
        card.setLayout(layout)
        return card
    
    def refresh_analytics(self):
        """Refresh analytics data"""
        try:
            # Get all active goals
            active_goals = self.goals_manager.get_active_goals()
            
            # Update metric cards
            self._update_metric_card(self.active_goals_card, str(len(active_goals)))
            
            # Calculate other metrics
            completed_count = len([g for g in active_goals if g.get('status') == 'completed'])
            self._update_metric_card(self.completed_goals_card, str(completed_count))
            
            # TODO: Calculate success rate and streak from actual data
            self._update_metric_card(self.success_rate_card, "85%")
            self._update_metric_card(self.streak_card, "3 days")
            
            # Update insights
            self._update_insights(active_goals)
            
        except Exception as e:
            logger.error(f"Error refreshing analytics: {e}")
    
    def _update_metric_card(self, card, value):
        """Update a metric card's value"""
        value_label = card.findChild(QLabel, "value_label")
        if value_label:
            value_label.setText(value)
    
    def _update_insights(self, goals):
        """Update insights text"""
        insights = []
        
        if not goals:
            insights.append("🎯 Create your first goal to start tracking progress!")
        else:
            insights.append(f"📊 You have {len(goals)} active goals")
            
            # Analyze goal types
            goal_types = {}
            for goal in goals:
                goal_type = goal['target_type']
                goal_types[goal_type] = goal_types.get(goal_type, 0) + 1
            
            for goal_type, count in goal_types.items():
                type_name = goal_type.replace('_', ' ').title()
                insights.append(f"• {count} {type_name} goal(s)")
            
            # Check for goals at risk
            behind_goals = [g for g in goals if g.get('status') in ['behind', 'very_behind']]
            if behind_goals:
                insights.append(f"⚠️ {len(behind_goals)} goal(s) are behind schedule")
            
            # Motivational messages
            on_track_goals = [g for g in goals if g.get('status') == 'on_track']
            if on_track_goals:
                insights.append(f"🟢 {len(on_track_goals)} goal(s) are on track - keep it up!")
        
        self.insights_text.setText('\n'.join(insights))

class GoalsMainWidget(QWidget):
    """Main goals management widget"""
    
    def __init__(self, db_manager):
        super().__init__()
        self.db_manager = db_manager
        self.goals_manager = GoalsManager(db_manager)
        self.setup_ui()
        self.refresh_goals()
    
    def setup_ui(self):
        """Set up main goals UI"""
        layout = QVBoxLayout()
        layout.setContentsMargins(10, 10, 10, 10)
        
        # Header with actions
        header_layout = QHBoxLayout()
        
        title = QLabel("🎯 Study Goals")
        title.setFont(QFont("Arial", 18, QFont.Weight.Bold))
        
        self.create_goal_btn = QPushButton("➕ Create Goal")
        self.create_goal_btn.clicked.connect(self.create_new_goal)
        self.create_goal_btn.setStyleSheet("""
            QPushButton {
                background-color: #007acc;
                color: white;
                font-weight: bold;
                padding: 10px 20px;
                border-radius: 6px;
                border: none;
            }
            QPushButton:hover {
                background-color: #005a9e;
            }
        """)
        
        header_layout.addWidget(title)
        header_layout.addStretch()
        header_layout.addWidget(self.create_goal_btn)
        
        layout.addLayout(header_layout)
        
        # Tab widget
        self.tabs = QTabWidget()
        
        # Active Goals tab
        self.active_goals_tab = QWidget()
        self.setup_active_goals_tab()
        self.tabs.addTab(self.active_goals_tab, "🎯 Active Goals")
        
        # Daily Progress tab
        self.daily_progress_widget = DailyProgressWidget(self.goals_manager)
        self.tabs.addTab(self.daily_progress_widget, "📅 Today's Progress")
        
        # Analytics tab
        self.analytics_widget = GoalsAnalyticsWidget(self.goals_manager)
        self.tabs.addTab(self.analytics_widget, "📊 Analytics")
        
        layout.addWidget(self.tabs)
        self.setLayout(layout)
    
    def setup_active_goals_tab(self):
        """Set up active goals tab"""
        layout = QVBoxLayout()
        
        # Goals container
        self.goals_scroll = QScrollArea()
        self.goals_widget = QWidget()
        self.goals_layout = QVBoxLayout()
        self.goals_widget.setLayout(self.goals_layout)
        self.goals_scroll.setWidget(self.goals_widget)
        self.goals_scroll.setWidgetResizable(True)
        
        layout.addWidget(self.goals_scroll)
        self.active_goals_tab.setLayout(layout)
    
    def create_new_goal(self):
        """Open create goal dialog"""
        try:
            # Get available topics
            topics = self.db_manager.get_all_topics()
            
            if not topics:
                QMessageBox.information(
                    self, "No Topics", 
                    "Please create at least one topic before setting goals."
                )
                return
            
            dialog = CreateGoalDialog(self.db_manager, topics, self)
            dialog.goal_created.connect(self.on_goal_created)
            dialog.exec()
            
        except Exception as e:
            logger.error(f"Error opening create goal dialog: {e}")
            QMessageBox.critical(self, "Error", f"Failed to open goal creation dialog: {str(e)}")
    
    @pyqtSlot(dict)
    def on_goal_created(self, goal_data):
        """Handle new goal creation"""
        self.refresh_goals()
        self.daily_progress_widget.refresh_progress()
        self.analytics_widget.refresh_analytics()
    
    def refresh_goals(self):
        """Refresh goals display"""
        try:
            # Clear existing goal cards
            for i in reversed(range(self.goals_layout.count())):
                child = self.goals_layout.itemAt(i).widget()
                if child:
                    child.setParent(None)
            
            # Get active goals
            active_goals = self.goals_manager.get_active_goals()
            
            if not active_goals:
                no_goals_label = QLabel("""
                    <div style='text-align: center; padding: 40px;'>
                        <h2>🎯 No Active Goals</h2>
                        <p>Create your first study goal to start tracking your progress!</p>
                        <p style='color: #666;'>Goals help you stay motivated and organized.</p>
                    </div>
                """)
                no_goals_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
                self.goals_layout.addWidget(no_goals_label)
            else:
                for goal in active_goals:
                    goal_card = GoalCard(goal, goal.get('daily_plan'))
                    goal_card.goal_clicked.connect(self.on_goal_clicked)
                    goal_card.goal_modified.connect(self.on_goal_modified)
                    self.goals_layout.addWidget(goal_card)
            
            self.goals_layout.addStretch()
            
        except Exception as e:
            logger.error(f"Error refreshing goals: {e}")
    
    @pyqtSlot(int)
    def on_goal_clicked(self, goal_id):
        """Handle goal card click"""
        try:
            # Get goal analytics
            analytics = self.goals_manager.get_goal_analytics(goal_id)
            
            # Show goal details dialog
            self.show_goal_details(goal_id, analytics)
            
        except Exception as e:
            logger.error(f"Error handling goal click: {e}")
    
    @pyqtSlot(int)
    def on_goal_modified(self, goal_id):
        """Handle goal modification"""
        self.refresh_goals()
        self.daily_progress_widget.refresh_progress()
        self.analytics_widget.refresh_analytics()
    
    def show_goal_details(self, goal_id, analytics):
        """Show detailed goal information dialog"""
        # TODO: Implement detailed goal view dialog
        QMessageBox.information(self, "Goal Details", f"Goal {goal_id} analytics: {len(analytics.get('progress_data', []))} days of data")
    
    def update_after_session(self, topic_id, pages_read, time_spent_seconds):
        """Update goals after a study session"""
        try:
            self.goals_manager.update_progress_after_session(
                topic_id=topic_id,
                pages_read=pages_read, 
                time_spent_seconds=time_spent_seconds
            )
            
            # Refresh all displays
            self.refresh_goals()
            self.daily_progress_widget.refresh_progress()
            self.analytics_widget.refresh_analytics()
            
        except Exception as e:
            logger.error(f"Error updating goals after session: {e}")