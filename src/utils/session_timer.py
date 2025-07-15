# src/utils/session_timer.py - Optimized Version
import time
from datetime import datetime, timedelta
from PyQt6.QtCore import QObject, QTimer, QElapsedTimer, pyqtSignal
from PyQt6.QtWidgets import QApplication
import logging

logger = logging.getLogger(__name__)

class SessionTimer(QObject):
    """Optimized reading session tracker with essential timing features"""
    
    # Core signals
    session_started = pyqtSignal(int)  # session_id
    session_ended = pyqtSignal(int, dict)  # session_id, stats
    session_paused = pyqtSignal(int, bool)  # session_id, is_manual_pause
    session_resumed = pyqtSignal(int)  # session_id
    page_changed = pyqtSignal(int, int, int)  # session_id, old_page, new_page
    idle_detected = pyqtSignal(bool)  # is_idle
    stats_updated = pyqtSignal(dict)  # current session stats
    
    def __init__(self, db_manager):
        super().__init__()
        self.db_manager = db_manager
        
        # Core session state
        self.current_session_id = None
        self.pdf_id = None
        self.exercise_pdf_id = None
        self.topic_id = None
        self.session_start_time = None
        
        # Timing
        self.session_timer = QElapsedTimer()
        self.last_activity_time = QElapsedTimer()
        self.idle_start_timer = QElapsedTimer()
        
        # Page tracking
        self.current_page = 1
        self.pages_visited = set()
        
        # Idle state
        self.is_idle = False
        self.is_manually_paused = False
        self.total_idle_time = 0
        
        # Timers
        self.idle_timer = QTimer()
        self.idle_timer.timeout.connect(self._check_idle)
        self.stats_timer = QTimer()
        self.stats_timer.timeout.connect(self._emit_stats)
        
    def start_session(self, pdf_id=None, exercise_pdf_id=None, topic_id=None):
        """Start new session"""
        try:
            if self.current_session_id:
                self.end_session()
            
            # Create session
            self.current_session_id = self.db_manager.create_session(
                pdf_id=pdf_id, exercise_pdf_id=exercise_pdf_id, topic_id=topic_id
            )
            
            # Initialize state
            self.pdf_id = pdf_id
            self.exercise_pdf_id = exercise_pdf_id
            self.topic_id = topic_id
            self.session_start_time = datetime.now()
            
            # Reset tracking
            self.session_timer.start()
            self.last_activity_time.start()
            self.pages_visited.clear()
            self.current_page = 1
            self.total_idle_time = 0
            self.is_idle = False
            self.is_manually_paused = False
            
            # Start monitoring
            self.idle_timer.start(1000)
            self.stats_timer.start(3000)
            
            logger.info(f"Started session {self.current_session_id}")
            self.session_started.emit(self.current_session_id)
            return self.current_session_id
            
        except Exception as e:
            logger.error(f"Error starting session: {e}")
            return None
    
    def end_session(self):
        """End current session"""
        if not self.current_session_id:
            return None
            
        try:
            # Stop timers
            self.idle_timer.stop()
            self.stats_timer.stop()
            
            # Calculate stats
            total_time = self.session_timer.elapsed() // 1000
            if self.is_idle and self.idle_start_timer.isValid():
                self.total_idle_time += self.idle_start_timer.elapsed() // 1000
            
            active_time = max(0, total_time - self.total_idle_time)
            pages_count = len(self.pages_visited)
            
            # Update database
            session_stats = self.db_manager.end_session(
                session_id=self.current_session_id,
                total_time_seconds=total_time,
                active_time_seconds=active_time,
                idle_time_seconds=self.total_idle_time,
                pages_visited=pages_count
            )
            
            # Update reading metrics
            if pages_count > 0 and active_time > 0:
                pages_per_minute = pages_count / (active_time / 60.0)
                avg_time_per_page = active_time / pages_count
                
                self.db_manager.update_reading_metrics(
                    pdf_id=self.pdf_id,
                    exercise_pdf_id=self.exercise_pdf_id,
                    topic_id=self.topic_id,
                    pages_per_minute=pages_per_minute,
                    average_time_per_page_seconds=avg_time_per_page,
                    pages_read=pages_count,
                    time_spent_seconds=active_time
                )
            
            session_id = self.current_session_id
            final_stats = session_stats or {}
            final_stats.update({
                'total_time_seconds': total_time,
                'active_time_seconds': active_time,
                'pages_visited': pages_count
            })
            
            logger.info(f"Ended session {session_id}: {total_time}s, {pages_count} pages")
            self.session_ended.emit(session_id, final_stats)
            
            # Reset state
            self._reset_state()
            return final_stats
            
        except Exception as e:
            logger.error(f"Error ending session: {e}")
            return None
    
    def change_page(self, new_page):
        """Handle page change"""
        if not self.current_session_id or new_page == self.current_page:
            return
            
        old_page = self.current_page
        self.current_page = new_page
        self.pages_visited.add(new_page)
        
        self._record_activity()
        self.page_changed.emit(self.current_session_id, old_page, new_page)
    
    def record_interaction(self, interaction_type="general"):
        """Record user interaction"""
        if self.current_session_id:
            self._record_activity()
    
    def pause_session(self, manual=True):
        """Pause session"""
        if not self.current_session_id or self.is_idle:
            return
            
        self._set_idle_state(True, manual)
        self.session_paused.emit(self.current_session_id, manual)
    
    def resume_session(self):
        """Resume session"""
        if not self.current_session_id or not self.is_idle:
            return
            
        self._record_activity()
        self.session_resumed.emit(self.current_session_id)
    
    def get_current_stats(self):
        """Get current session stats"""
        if not self.current_session_id:
            return None
            
        total_time = self.session_timer.elapsed() // 1000
        current_idle = 0
        if self.is_idle and self.idle_start_timer.isValid():
            current_idle = self.idle_start_timer.elapsed() // 1000
        
        total_idle = self.total_idle_time + current_idle
        active_time = max(0, total_time - total_idle)
        pages_count = len(self.pages_visited)
        
        return {
            'session_id': self.current_session_id,
            'pdf_id': self.pdf_id,
            'exercise_pdf_id': self.exercise_pdf_id,
            'topic_id': self.topic_id,
            'is_exercise': self.exercise_pdf_id is not None,
            'total_time_seconds': total_time,
            'active_time_seconds': active_time,
            'idle_time_seconds': total_idle,
            'pages_visited': pages_count,
            'current_page': self.current_page,
            'is_idle': self.is_idle,
            'is_manually_paused': self.is_manually_paused,
            'reading_speed_ppm': (pages_count / (active_time / 60.0)) if active_time > 0 else 0,
            'session_start_time': self.session_start_time.isoformat() if self.session_start_time else None
        }
    
    def _record_activity(self):
        """Record activity and manage idle state"""
        self.last_activity_time.restart()
        
        if self.is_idle:
            if self.idle_start_timer.isValid():
                self.total_idle_time += self.idle_start_timer.elapsed() // 1000
            self._set_idle_state(False)
    
    def _check_idle(self):
        """Check for idle timeout"""
        if not self.last_activity_time.isValid() or self.is_manually_paused:
            return
            
        if not self.is_idle and self.last_activity_time.elapsed() > 120000:  # 2 minutes
            self._set_idle_state(True, manual=False)
    
    def _set_idle_state(self, is_idle, manual=False):
        """Set idle state"""
        if self.is_idle == is_idle:
            return
            
        self.is_idle = is_idle
        self.is_manually_paused = manual if is_idle else False
        
        if is_idle:
            self.idle_start_timer.start()
        else:
            if self.idle_start_timer.isValid():
                self.total_idle_time += self.idle_start_timer.elapsed() // 1000
                self.idle_start_timer.invalidate()
        
        self.idle_detected.emit(is_idle)
    
    def _emit_stats(self):
        """Emit current stats"""
        stats = self.get_current_stats()
        if stats:
            self.stats_updated.emit(stats)
    
    def _reset_state(self):
        """Reset all state"""
        self.current_session_id = None
        self.pdf_id = None
        self.exercise_pdf_id = None
        self.topic_id = None
        self.session_start_time = None
        self.current_page = 1
        self.pages_visited.clear()
        self.total_idle_time = 0
        self.is_idle = False
        self.is_manually_paused = False


class ReadingIntelligence(QObject):
    """Optimized reading analytics and time estimation"""
    
    def __init__(self, db_manager):
        super().__init__()
        self.db_manager = db_manager
    
    def get_reading_speed(self, pdf_id=None, exercise_pdf_id=None, topic_id=None, user_wide=False):
        """Get reading speed metrics"""
        try:
            return self.db_manager.get_reading_metrics(
                pdf_id=pdf_id,
                exercise_pdf_id=exercise_pdf_id,
                topic_id=topic_id,
                user_wide=user_wide
            )
        except Exception as e:
            logger.error(f"Error getting reading speed: {e}")
            return None
    
    def estimate_finish_time(self, pdf_id=None, exercise_pdf_id=None, current_page=1, total_pages=1):
        """Estimate reading finish time"""
        try:
            # Get reading metrics
            metrics = self.get_reading_speed(pdf_id=pdf_id, exercise_pdf_id=exercise_pdf_id)
            
            # Fallback to topic or user metrics
            if not metrics and pdf_id:
                pdf_info = self.db_manager.get_pdf_by_id(pdf_id)
                if pdf_info:
                    metrics = self.get_reading_speed(topic_id=pdf_info.get('topic_id'))
            
            if not metrics:
                metrics = self.get_reading_speed(user_wide=True)
            
            # Use defaults if no metrics
            if not metrics or not metrics.get('average_time_per_page_seconds'):
                avg_time_per_page = 120 if exercise_pdf_id else 90
                confidence = 'low'
            else:
                avg_time_per_page = float(metrics['average_time_per_page_seconds'])
                pages_read = metrics.get('total_pages_read', 0)
                confidence = 'high' if pages_read >= 20 else 'medium' if pages_read >= 5 else 'low'
            
            # Calculate estimates
            pages_remaining = max(0, total_pages - current_page + 1)
            estimated_minutes = (pages_remaining * avg_time_per_page) / 60
            sessions_needed = max(1, estimated_minutes / 25)  # 25-min sessions
            
            return {
                'pages_remaining': pages_remaining,
                'estimated_minutes': estimated_minutes,
                'sessions_needed': round(sessions_needed),
                'confidence': confidence,
                'reading_pace_description': self._get_pace_description(avg_time_per_page)
            }
            
        except Exception as e:
            logger.error(f"Error estimating finish time: {e}")
            return None
    
    def get_daily_stats(self, date=None):
        """Get daily reading statistics"""
        try:
            if date is None:
                date = datetime.now().date()
            return self.db_manager.get_daily_reading_stats(date)
        except Exception as e:
            logger.error(f"Error getting daily stats: {e}")
            return None
    
    def get_session_history(self, days=7, pdf_id=None, exercise_pdf_id=None):
        """Get session history"""
        try:
            return self.db_manager.get_session_history(
                days=days, pdf_id=pdf_id, exercise_pdf_id=exercise_pdf_id
            )
        except Exception as e:
            logger.error(f"Error getting session history: {e}")
            return []
    
    def _get_pace_description(self, avg_time_per_page):
        """Get pace description"""
        if avg_time_per_page < 60:
            return "fast pace"
        elif avg_time_per_page < 90:
            return "moderate pace"
        elif avg_time_per_page < 150:
            return "careful pace"
        else:
            return "thorough pace"