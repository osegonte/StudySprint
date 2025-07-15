# src/ui/goals_widget.py - Optimized Version
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
                            QPushButton, QLabel, QComboBox, QSpinBox, QDateEdit,
                            QGroupBox, QScrollArea, QProgressBar, QFrame,
                            QDialog, QFormLayout, QRadioButton, QButtonGroup,
                            QMessageBox, QCheckBox)
from PyQt6.QtCore import Qt, QDate, QTimer, pyqtSignal, pyqtSlot
from PyQt6.QtGui import QFont
from datetime import datetime, date, timedelta
import logging

from utils.goals_manager import GoalsManager, GoalType, GoalStatus

logger = logging.getLogger(__name__)

class CreateGoalDialog(QDialog):
    """Streamlined goal creation dialog"""
    
    goal_created = pyqtSignal(dict)
    
    def __init__(self, db_manager, topics, parent=None):
        super().__init__(parent)
        self.db_manager = db_manager
        self.topics = topics
        self.goals_manager = GoalsManager(db_manager)
        
        self.setWindowTitle("Create Study Goal")
        self.setMinimumSize(450, 350)
        self.setup_ui()
        self.connect_signals()
    
    def setup_ui(self):
        layout = QVBoxLayout()
        
        # Header
        header = QLabel("🎯 Create New Study Goal")
        header.setFont(QFont("Arial", 16, QFont.Weight.Bold))
        header.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(header)
        
        # Form
        form_group = QGroupBox("Goal Details")
        form_layout = QFormLayout()
        
        # Topic selection
        self.topic_combo = QComboBox()
        self.topic_combo.addItem("Select a topic...", None)
        for topic in self.topics:
            self.topic_combo.addItem(f"📁 {topic['name']}", topic['id'])
        form_layout.addRow("📚 Topic:", self.topic_combo)
        
        # Goal type
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
        
        # Target inputs
        self.target_frame = QFrame()
        target_layout = QFormLayout()
        
        # Deadline
        self.deadline_date = QDateEdit()
        self.deadline_date.setCalendarPopup(True)
        self.deadline_date.setDate(QDate.currentDate().addDays(30))
        self.deadline_date.setMinimumDate(QDate.currentDate().addDays(1))
        target_layout.addRow("📅 Deadline:", self.deadline_date)
        
        # Minutes
        self.minutes_spin = QSpinBox()
        self.minutes_spin.setRange(5, 480)
        self.minutes_spin.setValue(30)
        self.minutes_spin.setSuffix(" minutes")
        target_layout.addRow("⏰ Daily Time:", self.minutes_spin)
        
        # Pages
        self.pages_spin = QSpinBox()
        self.pages_spin.setRange(1, 100)
        self.pages_spin.setValue(5)
        self.pages_spin.setSuffix(" pages")
        target_layout.addRow("📄 Daily Pages:", self.pages_spin)
        
        self.target_frame.setLayout(target_layout)
        form_layout.addRow(self.target_frame)
        
        # Preview
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
            QPushButton:hover { background-color: #1e7e34; }
        """)
        
        button_layout.addStretch()
        button_layout.addWidget(cancel_btn)
        button_layout.addWidget(self.create_btn)
        
        layout.addLayout(button_layout)
        self.setLayout(layout)
        
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
        """Update UI based on goal type"""
        selected_id = self.goal_type_group.checkedId()
        
        # Show/hide inputs
        self.deadline_date.setVisible(selected_id == 0)
        self.minutes_spin.setVisible(selected_id == 1)
        self.pages_spin.setVisible(selected_id == 2)
        
        # Update labels visibility
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
        """Update goal preview"""
        topic_index = self.topic_combo.currentIndex()
        if topic_index <= 0:
            self.preview_label.setText("Please select a topic to see goal preview")
            self.create_btn.setEnabled(False)
            return
        
        topic_name = self.topic_combo.currentText().replace("📁 ", "")
        selected_id = self.goal_type_group.checkedId()
        
        if selected_id == 0:
            qdate = self.deadline_date.date()
            deadline = date(qdate.year(), qdate.month(), qdate.day())
            days_until = (deadline - date.today()).days
            preview_text = f"🎯 Finish all PDFs in '{topic_name}' by {deadline.strftime('%B %d, %Y')} ({days_until} days)"
        elif selected_id == 1:
            minutes = self.minutes_spin.value()
            preview_text = f"⏰ Study '{topic_name}' for {minutes} minutes every day"
        else:
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
            
            if selected_id == 0:
                goal_type = GoalType.FINISH_BY_DATE
                target_value = 0
                qdate = self.deadline_date.date()
                deadline = date(qdate.year(), qdate.month(), qdate.day())
            elif selected_id == 1:
                goal_type = GoalType.DAILY_TIME
                target_value = self.minutes_spin.value()
                deadline = None
            else:
                goal_type = GoalType.DAILY_PAGES
                target_value = self.pages_spin.value()
                deadline = None
            
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
                QMessageBox.warning(self, "Error", "Failed to create goal")
                
        except Exception as e:
            logger.error(f"Error creating goal: {e}")
            QMessageBox.critical(self, "Error", f"Failed to create goal: {str(e)}")


class GoalCard(QFrame):
    """Optimized goal display card"""
    
    goal_clicked = pyqtSignal(int)
    
    def __init__(self, goal_data):
        super().__init__()
        self.goal_data = goal_data
        self.setup_ui()
        self.setMouseTracking(True)
    
    def setup_ui(self):
        """Set up goal card UI"""
        self.setFrameStyle(QFrame.Shape.Box)
        self.setLineWidth(2)
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        
        layout = QVBoxLayout()
        layout.setContentsMargins(15, 15, 15, 15)
        
        # Header
        header_layout = QHBoxLayout()
        
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
            
        elif goal_type == 'daily_time':
            details_layout.addWidget(QLabel(f"⏰ Target: {self.goal_data['target_value']} minutes/day"))
            
        else:
            details_layout.addWidget(QLabel(f"📄 Target: {self.goal_data['target_value']} pages/day"))
        
        layout.addLayout(details_layout)
        self.setLayout(layout)
        self._apply_status_styling()
    
    def _create_status_label(self):
        """Create status indicator"""
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
        """Apply styling based on status"""
        status = self.goal_data.get('status', 'on_track')
        
        colors = {
            'on_track': "#28a745",
            'slightly_behind': "#ffc107", 
            'behind': "#fd7e14",
            'very_behind': "#dc3545",
            'ahead': "#17a2b8",
            'completed': "#6f42c1"
        }
        
        color = colors.get(status, "#6c757d")
        
        self.setStyleSheet(f"""
            GoalCard {{
                border: 2px solid {color};
                border-radius: 8px;
                background-color: white;
            }}
            GoalCard:hover {{
                background-color: #f8f9fa;
            }}
        """)
    
    def mousePressEvent(self, event):
        """Handle click events"""
        if event.button() == Qt.MouseButton.LeftButton:
            self.goal_clicked.emit(self.goal_data['id'])
        super().mousePressEvent(event)


class DailyProgressWidget(QWidget):
    """Optimized daily progress display"""
    
    def __init__(self, goals_manager):
        super().__init__()
        self.goals_manager = goals_manager
        self.setup_ui()
        
        # Auto-refresh timer
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
        
        header_layout.addWidget(title)
        header_layout.addStretch()
        header_layout.addWidget(self.refresh_btn)
        
        layout.addLayout(header_layout)
        
        # Overall status
        self.overall_status = QLabel()
        self.overall_status.setFont(QFont("Arial", 14, QFont.Weight.Bold))
        self.overall_status.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.overall_status)
        
        # Progress container
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
        """Refresh today's progress"""
        try:
            # Clear existing items
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
            logger.error(f"Error refreshing progress: {e}")
    
    def _update_overall_status(self, status):
        """Update overall status display"""
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
        """Add daily goal progress item"""
        item_frame = QFrame()
        item_frame.setFrameStyle(QFrame.Shape.Box)
        
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
        status_icon = "✅" if goal['target_met_today'] else "📚"
        status_label = QLabel(status_icon)
        
        layout.addWidget(goal_label)
        layout.addWidget(progress_bar)
        layout.addWidget(progress_label)
        layout.addWidget(status_label)
        
        item_frame.setLayout(layout)
        self.progress_layout.addWidget(item_frame)
    
    def _add_deadline_goal_item(self, goal):
        """Add deadline goal progress item"""
        item_frame = QFrame()
        item_frame.setFrameStyle(QFrame.Shape.Box)
        
        layout = QVBoxLayout()
        layout.setContentsMargins(10, 8, 10, 8)
        
        goal_label = QLabel(f"📅 {goal['topic_name']}")
        goal_label.setFont(QFont("Arial", 12, QFont.Weight.Bold))
        
        contribution_label = QLabel(f"Today: {goal['pages_read_today']} pages, {goal['time_spent_today']} minutes")
        
        layout.addWidget(goal_label)
        layout.addWidget(contribution_label)
        
        item_frame.setLayout(layout)
        self.progress_layout.addWidget(item_frame)


class GoalsMainWidget(QWidget):
    """Main optimized goals widget"""
    
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
        
        # Header
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
            QPushButton:hover { background-color: #005a9e; }
        """)
        
        header_layout.addWidget(title)
        header_layout.addStretch()
        header_layout.addWidget(self.create_goal_btn)
        
        layout.addLayout(header_layout)
        
        # Tab widget
        from PyQt6.QtWidgets import QTabWidget
        self.tabs = QTabWidget()
        
        # Active Goals tab
        self.active_goals_tab = QWidget()
        self.setup_active_goals_tab()
        self.tabs.addTab(self.active_goals_tab, "🎯 Active Goals")
        
        # Daily Progress tab
        self.daily_progress_widget = DailyProgressWidget(self.goals_manager)
        self.tabs.addTab(self.daily_progress_widget, "📅 Today's Progress")
        
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
    
    @pyqtSlot(dict)
    def on_goal_created(self, goal_data):
        """Handle new goal creation"""
        self.refresh_goals()
        self.daily_progress_widget.refresh_progress()
    
    def refresh_goals(self):
        """Refresh goals display"""
        try:
            # Clear existing cards
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
                        <p>Create your first study goal to start tracking progress!</p>
                        <p style='color: #666;'>Goals help you stay motivated and organized.</p>
                    </div>
                """)
                no_goals_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
                self.goals_layout.addWidget(no_goals_label)
            else:
                for goal in active_goals:
                    goal_card = GoalCard(goal)
                    goal_card.goal_clicked.connect(self.on_goal_clicked)
                    self.goals_layout.addWidget(goal_card)
            
            self.goals_layout.addStretch()
            
        except Exception as e:
            logger.error(f"Error refreshing goals: {e}")
    
    @pyqtSlot(int)
    def on_goal_clicked(self, goal_id):
        """Handle goal card click"""
        try:
            analytics = self.goals_manager.get_goal_analytics(goal_id)
            self.show_goal_details(goal_id, analytics)
        except Exception as e:
            logger.error(f"Error handling goal click: {e}")
    
    def show_goal_details(self, goal_id, analytics):
        """Show goal details"""
        progress_data = analytics.get('progress_data', [])
        QMessageBox.information(
            self, "Goal Details", 
            f"Goal {goal_id} has {len(progress_data)} days of progress data"
        )
    
    def update_after_session(self, topic_id, pages_read, time_spent_seconds):
        """Update goals after study session"""
        try:
            self.goals_manager.update_progress_after_session(
                topic_id=topic_id,
                pages_read=pages_read, 
                time_spent_seconds=time_spent_seconds
            )
            
            # Refresh displays
            self.refresh_goals()
            self.daily_progress_widget.refresh_progress()
            
        except Exception as e:
            logger.error(f"Error updating goals after session: {e}")