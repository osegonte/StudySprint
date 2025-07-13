# src/utils/session_timer.py
import time
from datetime import datetime, timedelta
from PyQt6.QtCore import QObject, QTimer, QElapsedTimer, pyqtSignal
from PyQt6.QtWidgets import QApplication
import logging

logger = logging.getLogger(__name__)

class SessionTimer(QObject):
    """Tracks reading sessions with idle detection and page timing"""
    
    # Signals
    session_started = pyqtSignal(int)  # session_id
    session_ended = pyqtSignal(int, dict)  # session_id, stats
    page_changed = pyqtSignal(int, int, int)  # session_id, old_page, new_page
    idle_detected = pyqtSignal(bool)  # is_idle
    stats_updated = pyqtSignal(dict)  # current session stats
    
    def __init__(self, db_manager):
        super().__init__()
        self.db_manager = db_manager
        
        # Session state
        self.current_session_id = None
        self.pdf_id = None
        self.exercise_pdf_id = None
        self.topic_id = None
        self.is_exercise = False
        
        # Timing
        self.session_start_time = None
        self.session_timer = QElapsedTimer()
        self.page_timer = QElapsedTimer()
        self.current_page = 1
        self.pages_visited = set()
        
        # Idle detection
        self.idle_timer = QTimer()
        self.idle_timer.timeout.connect(self._check_idle_timeout)
        self.idle_threshold_ms = 120000  # 2 minutes
        self.last_activity_time = QElapsedTimer()
        self.is_idle = False
        self.idle_time_accumulated = 0
        
        # Stats update timer
        self.stats_timer = QTimer()
        self.stats_timer.timeout.connect(self._emit_stats_update)
        self.stats_timer.start(5000)  # Update stats every 5 seconds
        
    def start_session(self, pdf_id=None, exercise_pdf_id=None, topic_id=None):
        """Start a new reading session"""
        try:
            # End current session if active
            if self.current_session_id:
                self.end_session()
            
            self.pdf_id = pdf_id
            self.exercise_pdf_id = exercise_pdf_id
            self.topic_id = topic_id
            self.is_exercise = exercise_pdf_id is not None
            
            # Create session in database
            self.current_session_id = self.db_manager.create_session(
                pdf_id=pdf_id,
                exercise_pdf_id=exercise_pdf_id,
                topic_id=topic_id
            )
            
            # Initialize timers
            self.session_start_time = datetime.now()
            self.session_timer.start()
            self.last_activity_time.start()
            self.pages_visited.clear()
            self.idle_time_accumulated = 0
            self.is_idle = False
            
            # Start idle detection
            self.idle_timer.start(1000)  # Check every second
            
            logger.info(f"Started session {self.current_session_id} for {'exercise' if self.is_exercise else 'main'} PDF {pdf_id or exercise_pdf_id}")
            self.session_started.emit(self.current_session_id)
            
            return self.current_session_id
            
        except Exception as e:
            logger.error(f"Error starting session: {e}")
            return None
    
    def end_session(self):
        """End the current reading session"""
        if not self.current_session_id:
            return None
            
        try:
            # Stop timers
            self.idle_timer.stop()
            
            # Save current page if active
            if self.page_timer.isValid():
                self._save_current_page_time()
            
            # Calculate session stats
            total_time = self.session_timer.elapsed() // 1000  # Convert to seconds
            active_time = max(0, total_time - self.idle_time_accumulated)
            pages_count = len(self.pages_visited)
            
            # Update session in database
            session_stats = self.db_manager.end_session(
                session_id=self.current_session_id,
                total_time_seconds=total_time,
                active_time_seconds=active_time,
                idle_time_seconds=self.idle_time_accumulated,
                pages_visited=pages_count
            )
            
            logger.info(f"Ended session {self.current_session_id}: {total_time}s total, {active_time}s active, {pages_count} pages")
            
            # Update reading metrics
            self._update_reading_metrics()
            
            session_id = self.current_session_id
            self.session_ended.emit(session_id, session_stats or {})
            
            # Reset state
            self._reset_session_state()
            
            return session_stats
            
        except Exception as e:
            logger.error(f"Error ending session: {e}")
            return None
    
    def change_page(self, new_page):
        """Handle page changes during reading"""
        if not self.current_session_id:
            return
            
        try:
            old_page = self.current_page
            
            # Save timing for previous page
            if self.page_timer.isValid() and old_page != new_page:
                self._save_current_page_time()
            
            # Start timing for new page
            self.current_page = new_page
            self.pages_visited.add(new_page)
            self.page_timer.start()
            
            # Record activity
            self._record_activity()
            
            self.page_changed.emit(self.current_session_id, old_page, new_page)
            logger.debug(f"Page changed from {old_page} to {new_page}")
            
        except Exception as e:
            logger.error(f"Error handling page change: {e}")
    
    def record_interaction(self):
        """Record user interaction (scroll, click, etc.)"""
        self._record_activity()
    
    def pause_session(self):
        """Manually pause the session"""
        if self.current_session_id and not self.is_idle:
            self._set_idle_state(True, manual=True)
    
    def resume_session(self):
        """Manually resume the session"""
        if self.current_session_id and self.is_idle:
            self._record_activity()
    
    def get_current_stats(self):
        """Get current session statistics"""
        if not self.current_session_id:
            return None
            
        total_time = self.session_timer.elapsed() // 1000
        active_time = max(0, total_time - self.idle_time_accumulated)
        pages_count = len(self.pages_visited)
        
        return {
            'session_id': self.current_session_id,
            'total_time_seconds': total_time,
            'active_time_seconds': active_time,
            'idle_time_seconds': self.idle_time_accumulated,
            'pages_visited': pages_count,
            'current_page': self.current_page,
            'is_idle': self.is_idle,
            'is_exercise': self.is_exercise
        }
    
    def _save_current_page_time(self):
        """Save timing data for the current page"""
        if not self.page_timer.isValid():
            return
            
        try:
            duration = self.page_timer.elapsed() // 1000  # Convert to seconds
            
            # Only save if meaningful time spent (> 1 second)
            if duration > 1:
                self.db_manager.save_page_time(
                    session_id=self.current_session_id,
                    pdf_id=self.pdf_id,
                    exercise_pdf_id=self.exercise_pdf_id,
                    page_number=self.current_page,
                    duration_seconds=duration
                )
                
                logger.debug(f"Saved page {self.current_page} time: {duration}s")
                
        except Exception as e:
            logger.error(f"Error saving page time: {e}")
    
    def _record_activity(self):
        """Record user activity and resume from idle if needed"""
        self.last_activity_time.restart()
        
        if self.is_idle:
            self._set_idle_state(False)
    
    def _check_idle_timeout(self):
        """Check if user has been idle too long"""
        if not self.last_activity_time.isValid():
            return
            
        elapsed = self.last_activity_time.elapsed()
        
        if not self.is_idle and elapsed > self.idle_threshold_ms:
            self._set_idle_state(True)
    
    def _set_idle_state(self, is_idle, manual=False):
        """Set idle state and handle timing"""
        if self.is_idle == is_idle:
            return
            
        self.is_idle = is_idle
        
        if is_idle:
            # Entering idle state - save current page time
            if self.page_timer.isValid():
                self._save_current_page_time()
                self.page_timer.invalidate()  # Stop page timer during idle
            
            logger.debug(f"Entering idle state {'(manual)' if manual else '(auto)'}")
        else:
            # Exiting idle state - restart page timer
            if self.current_session_id:
                self.page_timer.start()
            
            logger.debug("Exiting idle state")
        
        self.idle_detected.emit(is_idle)
    
    def _emit_stats_update(self):
        """Emit current stats for UI updates"""
        stats = self.get_current_stats()
        if stats:
            self.stats_updated.emit(stats)
    
    def _update_reading_metrics(self):
        """Update reading speed metrics after session"""
        if not self.current_session_id:
            return
            
        try:
            # Calculate metrics for this session
            stats = self.get_current_stats()
            if not stats or stats['pages_visited'] == 0:
                return
            
            pages_read = stats['pages_visited']
            active_time_minutes = stats['active_time_seconds'] / 60.0
            
            if active_time_minutes > 0:
                pages_per_minute = pages_read / active_time_minutes
                avg_time_per_page = stats['active_time_seconds'] / pages_read
                
                # Update or create reading metrics
                self.db_manager.update_reading_metrics(
                    pdf_id=self.pdf_id,
                    exercise_pdf_id=self.exercise_pdf_id,
                    topic_id=self.topic_id,
                    pages_per_minute=pages_per_minute,
                    average_time_per_page_seconds=avg_time_per_page,
                    pages_read=pages_read,
                    time_spent_seconds=stats['active_time_seconds']
                )
                
                logger.info(f"Updated reading metrics: {pages_per_minute:.2f} PPM, {avg_time_per_page:.1f}s per page")
                
        except Exception as e:
            logger.error(f"Error updating reading metrics: {e}")
    
    def _reset_session_state(self):
        """Reset all session state variables"""
        self.current_session_id = None
        self.pdf_id = None
        self.exercise_pdf_id = None
        self.topic_id = None
        self.is_exercise = False
        self.session_start_time = None
        self.current_page = 1
        self.pages_visited.clear()
        self.idle_time_accumulated = 0
        self.is_idle = False
        
        # Invalidate timers
        if self.page_timer.isValid():
            self.page_timer.invalidate()
        if self.session_timer.isValid():
            self.session_timer.invalidate()
        if self.last_activity_time.isValid():
            self.last_activity_time.invalidate()


class ReadingIntelligence(QObject):
    """Provides reading analytics and time estimation"""
    
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
        """Estimate time to finish reading a PDF"""
        try:
            # Get reading metrics for this PDF or similar content
            metrics = self.get_reading_speed(pdf_id=pdf_id, exercise_pdf_id=exercise_pdf_id)
            
            if not metrics or not metrics.get('average_time_per_page_seconds'):
                # Fall back to topic-level or user-wide metrics
                if pdf_id:
                    pdf_info = self.db_manager.get_pdf_by_id(pdf_id)
                    topic_id = pdf_info.get('topic_id') if pdf_info else None
                elif exercise_pdf_id:
                    exercise_info = self.db_manager.get_exercise_pdf_by_id(exercise_pdf_id)
                    if exercise_info:
                        parent_pdf = self.db_manager.get_pdf_by_id(exercise_info['parent_pdf_id'])
                        topic_id = parent_pdf.get('topic_id') if parent_pdf else None
                    else:
                        topic_id = None
                
                metrics = self.get_reading_speed(topic_id=topic_id)
                
                if not metrics:
                    metrics = self.get_reading_speed(user_wide=True)
            
            if not metrics or not metrics.get('average_time_per_page_seconds'):
                # Default estimation: 1.5 minutes per page
                avg_time_per_page = 90
            else:
                avg_time_per_page = metrics['average_time_per_page_seconds']
            
            pages_remaining = max(0, total_pages - current_page + 1)
            estimated_seconds = pages_remaining * avg_time_per_page
            
            return {
                'pages_remaining': pages_remaining,
                'estimated_seconds': estimated_seconds,
                'estimated_minutes': estimated_seconds / 60,
                'estimated_hours': estimated_seconds / 3600,
                'average_time_per_page': avg_time_per_page,
                'confidence': 'high' if metrics and metrics.get('total_pages_read', 0) >= 5 else 'low'
            }
            
        except Exception as e:
            logger.error(f"Error estimating finish time: {e}")
            return None
    
    def get_daily_stats(self, date=None):
        """Get reading statistics for a specific date"""
        try:
            if date is None:
                date = datetime.now().date()
            
            return self.db_manager.get_daily_reading_stats(date)
            
        except Exception as e:
            logger.error(f"Error getting daily stats: {e}")
            return None
    
    def get_topic_progress(self, topic_id):
        """Get detailed progress for a topic"""
        try:
            return self.db_manager.get_topic_progress_summary(topic_id)
            
        except Exception as e:
            logger.error(f"Error getting topic progress: {e}")
            return None
    
    def get_session_history(self, days=7, pdf_id=None, exercise_pdf_id=None):
        """Get recent session history"""
        try:
            return self.db_manager.get_session_history(
                days=days,
                pdf_id=pdf_id,
                exercise_pdf_id=exercise_pdf_id
            )
            
        except Exception as e:
            logger.error(f"Error getting session history: {e}")
            return None
