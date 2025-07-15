# src/utils/session_timer.py - Enhanced Version
import time
from datetime import datetime, timedelta
from PyQt6.QtCore import QObject, QTimer, QElapsedTimer, pyqtSignal, QCoreApplication
from PyQt6.QtWidgets import QApplication
import logging
import json

logger = logging.getLogger(__name__)

class SessionTimer(QObject):
    """Enhanced reading session tracker with comprehensive timing and analytics"""
    
    # Signals
    session_started = pyqtSignal(int)  # session_id
    session_ended = pyqtSignal(int, dict)  # session_id, stats
    session_paused = pyqtSignal(int, bool)  # session_id, is_manual_pause
    session_resumed = pyqtSignal(int)  # session_id
    page_changed = pyqtSignal(int, int, int)  # session_id, old_page, new_page
    idle_detected = pyqtSignal(bool)  # is_idle
    stats_updated = pyqtSignal(dict)  # current session stats
    reading_speed_updated = pyqtSignal(dict)  # speed metrics
    finish_time_estimated = pyqtSignal(dict)  # time estimation
    
    def __init__(self, db_manager):
        super().__init__()
        self.db_manager = db_manager
        
        # Session state
        self.current_session_id = None
        self.pdf_id = None
        self.exercise_pdf_id = None
        self.topic_id = None
        self.is_exercise = False
        self.session_start_time = None
        
        # Timing mechanisms
        self.session_timer = QElapsedTimer()
        self.page_timer = QElapsedTimer()
        self.idle_start_timer = QElapsedTimer()
        
        # Page tracking
        self.current_page = 1
        self.previous_page = 1
        self.pages_visited = set()
        self.page_visit_log = []  # [(page, start_time, end_time, duration)]
        self.page_start_time = None
        
        # Idle detection
        self.idle_timer = QTimer()
        self.idle_timer.timeout.connect(self._check_idle_timeout)
        self.idle_threshold_ms = 120000  # 2 minutes
        self.last_activity_time = QElapsedTimer()
        self.is_idle = False
        self.is_manually_paused = False
        self.total_idle_time = 0
        self.current_idle_duration = 0
        
        # Activity detection
        self.activity_timer = QTimer()
        self.activity_timer.timeout.connect(self._record_heartbeat)
        self.activity_timer.start(30000)  # Heartbeat every 30 seconds
        
        # Stats update timer
        self.stats_timer = QTimer()
        self.stats_timer.timeout.connect(self._emit_stats_update)
        self.stats_timer.start(3000)  # Update stats every 3 seconds
        
        # Reading speed calculation timer
        self.speed_timer = QTimer()
        self.speed_timer.timeout.connect(self._calculate_reading_speed)
        self.speed_timer.start(10000)  # Calculate speed every 10 seconds
        
        # App state monitoring
        QCoreApplication.instance().aboutToQuit.connect(self._handle_app_quit)
        
    def start_session(self, pdf_id=None, exercise_pdf_id=None, topic_id=None, pdf_title=""):
        """Start a new comprehensive reading session"""
        try:
            # End current session if active
            if self.current_session_id:
                self.end_session()
            
            # Set session parameters
            self.pdf_id = pdf_id
            self.exercise_pdf_id = exercise_pdf_id
            self.topic_id = topic_id
            self.is_exercise = exercise_pdf_id is not None
            self.pdf_title = pdf_title
            
            # Create session in database
            self.current_session_id = self.db_manager.create_session(
                pdf_id=pdf_id,
                exercise_pdf_id=exercise_pdf_id,
                topic_id=topic_id
            )
            
            # Initialize timing
            self.session_start_time = datetime.now()
            self.session_timer.start()
            self.last_activity_time.start()
            
            # Reset tracking variables
            self.pages_visited.clear()
            self.page_visit_log.clear()
            self.current_page = 1
            self.previous_page = 1
            self.total_idle_time = 0
            self.current_idle_duration = 0
            self.is_idle = False
            self.is_manually_paused = False
            self.page_start_time = time.time()
            
            # Start monitoring
            self.idle_timer.start(1000)  # Check every second
            self.page_timer.start()
            
            logger.info(f"📖 Started session {self.current_session_id} for {'exercise' if self.is_exercise else 'main'} PDF {pdf_id or exercise_pdf_id}")
            self.session_started.emit(self.current_session_id)
            
            return self.current_session_id
            
        except Exception as e:
            logger.error(f"❌ Error starting session: {e}")
            return None
    
    def end_session(self):
        """End the current reading session with comprehensive cleanup"""
        if not self.current_session_id:
            return None
            
        try:
            # Stop all timers
            self.idle_timer.stop()
            
            # Save final page time if active
            if self.page_timer.isValid() and not self.is_idle:
                self._save_current_page_time()
            
            # Calculate comprehensive session stats
            total_elapsed_ms = self.session_timer.elapsed()
            total_time_seconds = total_elapsed_ms // 1000
            
            # Calculate idle time properly
            if self.is_idle and self.idle_start_timer.isValid():
                self.current_idle_duration = self.idle_start_timer.elapsed() // 1000
                self.total_idle_time += self.current_idle_duration
            
            active_time_seconds = max(0, total_time_seconds - self.total_idle_time)
            pages_count = len(self.pages_visited)
            
            # Update session in database
            session_stats = self.db_manager.end_session(
                session_id=self.current_session_id,
                total_time_seconds=total_time_seconds,
                active_time_seconds=active_time_seconds,
                idle_time_seconds=self.total_idle_time,
                pages_visited=pages_count
            )
            
            # Calculate and update reading metrics
            reading_metrics = self._calculate_final_reading_metrics(
                active_time_seconds, pages_count
            )
            
            # Save comprehensive session metadata
            self._save_session_metadata(session_stats, reading_metrics)
            
            logger.info(f"✅ Ended session {self.current_session_id}: {total_time_seconds}s total, {active_time_seconds}s active, {pages_count} pages")
            
            session_id = self.current_session_id
            final_stats = {
                **(session_stats or {}),
                **reading_metrics,
                'page_visit_log': self.page_visit_log.copy()
            }
            
            self.session_ended.emit(session_id, final_stats)
            
            # Reset state
            self._reset_session_state()
            
            return final_stats
            
        except Exception as e:
            logger.error(f"❌ Error ending session: {e}")
            return None
    
    def change_page(self, new_page):
        """Enhanced page change handling with detailed timing"""
        if not self.current_session_id:
            return
            
        try:
            old_page = self.current_page
            
            # Handle same page (no change)
            if old_page == new_page:
                self._record_activity()
                return
            
            # Save timing for previous page
            if self.page_timer.isValid():
                self._save_current_page_time()
            
            # Update page state
            self.previous_page = old_page
            self.current_page = new_page
            self.pages_visited.add(new_page)
            
            # Start timing for new page
            self.page_start_time = time.time()
            self.page_timer.restart()
            
            # Record activity
            self._record_activity()
            
            # Emit signals
            self.page_changed.emit(self.current_session_id, old_page, new_page)
            
            # Update reading speed estimate
            self._calculate_reading_speed()
            
            logger.debug(f"📄 Page changed: {old_page} → {new_page} (Session {self.current_session_id})")
            
        except Exception as e:
            logger.error(f"❌ Error handling page change: {e}")
    
    def record_interaction(self, interaction_type="general"):
        """Record user interaction with detailed logging"""
        if not self.current_session_id:
            return
            
        self._record_activity()
        
        # Log interaction type for analytics
        if hasattr(self, 'interaction_log'):
            self.interaction_log.append({
                'type': interaction_type,
                'page': self.current_page,
                'timestamp': time.time(),
                'session_id': self.current_session_id
            })
    
    def pause_session(self, manual=True):
        """Pause the session manually or automatically"""
        if not self.current_session_id or self.is_idle:
            return
            
        self._set_idle_state(True, manual=manual)
        self.is_manually_paused = manual
        
        if manual:
            logger.info(f"⏸️ Session {self.current_session_id} manually paused")
        
        self.session_paused.emit(self.current_session_id, manual)
    
    def resume_session(self):
        """Resume a paused session"""
        if not self.current_session_id or not self.is_idle:
            return
            
        self._record_activity()
        self.is_manually_paused = False
        
        logger.info(f"▶️ Session {self.current_session_id} resumed")
        self.session_resumed.emit(self.current_session_id)
    
    def get_current_stats(self):
        """Get comprehensive current session statistics"""
        if not self.current_session_id:
            return None
            
        total_elapsed = self.session_timer.elapsed() // 1000
        
        # Calculate current idle time
        current_idle = 0
        if self.is_idle and self.idle_start_timer.isValid():
            current_idle = self.idle_start_timer.elapsed() // 1000
        
        total_idle = self.total_idle_time + current_idle
        active_time = max(0, total_elapsed - total_idle)
        pages_count = len(self.pages_visited)
        
        # Calculate reading speed
        reading_speed = 0
        avg_time_per_page = 0
        if active_time > 0 and pages_count > 0:
            reading_speed = (pages_count / (active_time / 60.0))  # pages per minute
            avg_time_per_page = active_time / pages_count
        
        return {
            'session_id': self.current_session_id,
            'pdf_id': self.pdf_id,
            'exercise_pdf_id': self.exercise_pdf_id,
            'topic_id': self.topic_id,
            'is_exercise': self.is_exercise,
            'total_time_seconds': total_elapsed,
            'active_time_seconds': active_time,
            'idle_time_seconds': total_idle,
            'pages_visited': pages_count,
            'current_page': self.current_page,
            'is_idle': self.is_idle,
            'is_manually_paused': self.is_manually_paused,
            'reading_speed_ppm': reading_speed,
            'avg_time_per_page': avg_time_per_page,
            'session_start_time': self.session_start_time.isoformat() if self.session_start_time else None,
            'unique_pages_visited': list(self.pages_visited),
            'page_visit_count': len(self.page_visit_log)
        }
    
    def get_session_summary(self):
        """Get a formatted session summary for display"""
        stats = self.get_current_stats()
        if not stats:
            return "No active session"
        
        total_time = stats['total_time_seconds']
        active_time = stats['active_time_seconds']
        pages = stats['pages_visited']
        speed = stats['reading_speed_ppm']
        
        hours = total_time // 3600
        minutes = (total_time % 3600) // 60
        seconds = total_time % 60
        
        if hours > 0:
            time_str = f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            time_str = f"{minutes}m {seconds}s"
        else:
            time_str = f"{seconds}s"
        
        return f"📖 {time_str} • {pages} pages • {speed:.1f} PPM"
    
    def _save_current_page_time(self):
        """Save detailed timing data for the current page"""
        if not self.page_timer.isValid() or self.is_idle:
            return
            
        try:
            duration_ms = self.page_timer.elapsed()
            duration_seconds = duration_ms // 1000
            
            # Only save if meaningful time spent (> 2 seconds)
            if duration_seconds >= 2:
                end_time = time.time()
                
                # Add to visit log
                visit_record = {
                    'page': self.current_page,
                    'start_time': self.page_start_time,
                    'end_time': end_time,
                    'duration_seconds': duration_seconds,
                    'session_id': self.current_session_id
                }
                self.page_visit_log.append(visit_record)
                
                # Save to database
                self.db_manager.save_page_time(
                    session_id=self.current_session_id,
                    pdf_id=self.pdf_id,
                    exercise_pdf_id=self.exercise_pdf_id,
                    page_number=self.current_page,
                    duration_seconds=duration_seconds
                )
                
                logger.debug(f"💾 Saved page {self.current_page} time: {duration_seconds}s")
                
        except Exception as e:
            logger.error(f"❌ Error saving page time: {e}")
    
    def _record_activity(self):
        """Record user activity and handle idle state changes"""
        self.last_activity_time.restart()
        
        if self.is_idle:
            # Calculate idle duration before resuming
            if self.idle_start_timer.isValid():
                idle_duration = self.idle_start_timer.elapsed() // 1000
                self.total_idle_time += idle_duration
                self.current_idle_duration = 0
            
            self._set_idle_state(False)
    
    def _check_idle_timeout(self):
        """Enhanced idle detection with grace period"""
        if not self.last_activity_time.isValid() or self.is_manually_paused:
            return
            
        elapsed = self.last_activity_time.elapsed()
        
        if not self.is_idle and elapsed > self.idle_threshold_ms:
            # Check if application is active/visible
            app = QApplication.instance()
            if app and not app.activeWindow():
                # App is minimized/not focused - go idle immediately
                self._set_idle_state(True, manual=False)
            else:
                # Normal idle timeout
                self._set_idle_state(True, manual=False)
    
    def _set_idle_state(self, is_idle, manual=False):
        """Enhanced idle state management with proper timing"""
        if self.is_idle == is_idle:
            return
            
        previous_state = self.is_idle
        self.is_idle = is_idle
        
        if is_idle:
            # Entering idle state
            self.idle_start_timer.start()
            
            # Save current page time before going idle
            if self.page_timer.isValid():
                self._save_current_page_time()
                self.page_timer.invalidate()
            
            idle_type = "manual" if manual else "auto"
            logger.debug(f"😴 Entering idle state ({idle_type}) - Session {self.current_session_id}")
            
        else:
            # Exiting idle state
            if self.idle_start_timer.isValid():
                # Add the idle period to total
                idle_duration = self.idle_start_timer.elapsed() // 1000
                self.total_idle_time += idle_duration
                self.idle_start_timer.invalidate()
            
            # Restart page timer if we have a session
            if self.current_session_id:
                self.page_start_time = time.time()
                self.page_timer.start()
            
            logger.debug(f"🔄 Exiting idle state - Session {self.current_session_id}")
        
        self.idle_detected.emit(is_idle)
    
    def _calculate_reading_speed(self):
        """Calculate and emit current reading speed metrics"""
        stats = self.get_current_stats()
        if not stats or stats['pages_visited'] == 0:
            return
        
        try:
            # Calculate instantaneous speed
            active_minutes = stats['active_time_seconds'] / 60.0
            if active_minutes > 0:
                current_speed = stats['pages_visited'] / active_minutes
                avg_page_time = stats['active_time_seconds'] / stats['pages_visited']
                
                speed_metrics = {
                    'session_id': self.current_session_id,
                    'current_speed_ppm': current_speed,
                    'average_time_per_page': avg_page_time,
                    'pages_read_this_session': stats['pages_visited'],
                    'active_time_minutes': active_minutes,
                    'efficiency_percent': (stats['active_time_seconds'] / stats['total_time_seconds']) * 100
                }
                
                self.reading_speed_updated.emit(speed_metrics)
                
        except Exception as e:
            logger.error(f"❌ Error calculating reading speed: {e}")
    
    def _calculate_final_reading_metrics(self, active_time_seconds, pages_count):
        """Calculate final reading metrics for session end"""
        if pages_count == 0 or active_time_seconds == 0:
            return {}
        
        pages_per_minute = (pages_count / (active_time_seconds / 60.0))
        avg_time_per_page = active_time_seconds / pages_count
        
        return {
            'final_reading_speed_ppm': pages_per_minute,
            'final_avg_time_per_page': avg_time_per_page,
            'reading_efficiency': (active_time_seconds / self.session_timer.elapsed() * 1000) * 100
        }
    
    def _save_session_metadata(self, session_stats, reading_metrics):
        """Save comprehensive session metadata for analytics"""
        try:
            # Update reading metrics in database
            if reading_metrics.get('final_reading_speed_ppm'):
                self.db_manager.update_reading_metrics(
                    pdf_id=self.pdf_id,
                    exercise_pdf_id=self.exercise_pdf_id,
                    topic_id=self.topic_id,
                    pages_per_minute=reading_metrics['final_reading_speed_ppm'],
                    average_time_per_page_seconds=reading_metrics['final_avg_time_per_page'],
                    pages_read=len(self.pages_visited),
                    time_spent_seconds=session_stats.get('active_time_seconds', 0) if session_stats else 0
                )
                
                logger.info(f"📊 Updated reading metrics: {reading_metrics['final_reading_speed_ppm']:.2f} PPM")
                
        except Exception as e:
            logger.error(f"❌ Error saving session metadata: {e}")
    
    def _record_heartbeat(self):
        """Record periodic heartbeat for app state monitoring"""
        if self.current_session_id and not self.is_idle:
            logger.debug(f"💓 Session heartbeat - {self.current_session_id}")
    
    def _emit_stats_update(self):
        """Emit current stats for UI updates"""
        stats = self.get_current_stats()
        if stats:
            self.stats_updated.emit(stats)
    
    def _handle_app_quit(self):
        """Handle graceful session cleanup on app quit"""
        if self.current_session_id:
            logger.info(f"🔚 App quit detected - ending session {self.current_session_id}")
            self.end_session()
    
    def _reset_session_state(self):
        """Reset all session state variables"""
        self.current_session_id = None
        self.pdf_id = None
        self.exercise_pdf_id = None
        self.topic_id = None
        self.is_exercise = False
        self.session_start_time = None
        self.current_page = 1
        self.previous_page = 1
        self.pages_visited.clear()
        self.page_visit_log.clear()
        self.total_idle_time = 0
        self.current_idle_duration = 0
        self.is_idle = False
        self.is_manually_paused = False
        self.page_start_time = None
        
        # Invalidate timers
        if self.page_timer.isValid():
            self.page_timer.invalidate()
        if self.session_timer.isValid():
            self.session_timer.invalidate()
        if self.last_activity_time.isValid():
            self.last_activity_time.invalidate()
        if self.idle_start_timer.isValid():
            self.idle_start_timer.invalidate()


class ReadingIntelligence(QObject):
    """Enhanced reading analytics and intelligent time estimation"""
    
    def __init__(self, db_manager):
        super().__init__()
        self.db_manager = db_manager
        
    def get_reading_speed(self, pdf_id=None, exercise_pdf_id=None, topic_id=None, user_wide=False):
        """Get detailed reading speed metrics with confidence scoring"""
        try:
            metrics = self.db_manager.get_reading_metrics(
                pdf_id=pdf_id,
                exercise_pdf_id=exercise_pdf_id,
                topic_id=topic_id,
                user_wide=user_wide
            )
            
            if metrics:
                # Add confidence scoring
                pages_read = metrics.get('total_pages_read', 0)
                if pages_read >= 20:
                    confidence = 'high'
                elif pages_read >= 5:
                    confidence = 'medium'
                else:
                    confidence = 'low'
                
                metrics['confidence'] = confidence
                metrics['sample_size'] = pages_read
                
            return metrics
            
        except Exception as e:
            logger.error(f"❌ Error getting reading speed: {e}")
            return None
    
    def estimate_finish_time(self, pdf_id=None, exercise_pdf_id=None, current_page=1, total_pages=1):
        """Intelligent finish time estimation with multiple fallback strategies"""
        try:
            # Strategy 1: PDF-specific metrics
            metrics = self.get_reading_speed(pdf_id=pdf_id, exercise_pdf_id=exercise_pdf_id)
            
            # Strategy 2: Topic-level metrics
            if not metrics or metrics.get('confidence') == 'low':
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
                
                if topic_id:
                    metrics = self.get_reading_speed(topic_id=topic_id)
            
            # Strategy 3: User-wide metrics
            if not metrics or metrics.get('confidence') == 'low':
                metrics = self.get_reading_speed(user_wide=True)
            
            # Strategy 4: Intelligent defaults based on content type
            if not metrics or not metrics.get('average_time_per_page_seconds'):
                if exercise_pdf_id:
                    # Exercise PDFs typically take longer
                    avg_time_per_page = 120  # 2 minutes per page
                    confidence = 'low'
                else:
                    # Regular reading material
                    avg_time_per_page = 90   # 1.5 minutes per page
                    confidence = 'low'
            else:
                avg_time_per_page = float(metrics['average_time_per_page_seconds'])
                confidence = metrics.get('confidence', 'low')
            
            # Calculate estimates
            pages_remaining = max(0, total_pages - current_page + 1)
            estimated_seconds = pages_remaining * avg_time_per_page
            estimated_minutes = estimated_seconds / 60
            
            # Calculate reading sessions needed (assuming 25-minute sessions)
            avg_session_length = 25 * 60  # 25 minutes in seconds
            sessions_needed = max(1, estimated_seconds / avg_session_length)
            
            # Estimate finish date based on user's reading patterns
            finish_date_estimate = self._estimate_finish_date(estimated_minutes)
            
            return {
                'pages_remaining': pages_remaining,
                'estimated_seconds': estimated_seconds,
                'estimated_minutes': estimated_minutes,
                'estimated_hours': estimated_seconds / 3600,
                'sessions_needed': round(sessions_needed),
                'average_time_per_page': avg_time_per_page,
                'confidence': confidence,
                'strategy_used': self._get_strategy_description(metrics),
                'finish_date_estimate': finish_date_estimate,
                'reading_pace_description': self._get_pace_description(avg_time_per_page)
            }
            
        except Exception as e:
            logger.error(f"❌ Error estimating finish time: {e}")
            return None
    
    def get_daily_stats(self, date=None):
        """Get comprehensive daily reading statistics"""
        try:
            if date is None:
                date = datetime.now().date()
            
            stats = self.db_manager.get_daily_reading_stats(date)
            
            if stats:
                # Add derived metrics
                sessions_count = stats.get('sessions_count', 0)
                total_time = stats.get('total_time_seconds', 0)
                
                if sessions_count > 0:
                    stats['avg_session_length'] = total_time / sessions_count
                else:
                    stats['avg_session_length'] = 0
                
                # Add goal progress (if goals are implemented)
                stats['daily_goal_progress'] = self._calculate_daily_goal_progress(stats)
                
            return stats
            
        except Exception as e:
            logger.error(f"❌ Error getting daily stats: {e}")
            return None
    
    def get_streak_analytics(self):
        """Get detailed reading streak analytics"""
        try:
            # Get reading streaks
            streaks = self.db_manager.get_reading_streaks() if hasattr(self.db_manager, 'get_reading_streaks') else None
            
            if streaks:
                # Add streak quality metrics
                streak_quality = self._calculate_streak_quality(streaks)
                streaks.update(streak_quality)
                
            return streaks
            
        except Exception as e:
            logger.error(f"❌ Error getting streak analytics: {e}")
            return None
    
    def get_topic_analytics(self, topic_id):
        """Get comprehensive topic-level analytics"""
        try:
            # Get basic topic data
            pdfs = self.db_manager.get_pdfs_by_topic(topic_id)
            
            # Calculate topic metrics
            total_pages = sum(pdf.get('total_pages', 0) for pdf in pdfs)
            read_pages = sum(pdf.get('current_page', 1) - 1 for pdf in pdfs)
            
            # Get topic reading sessions
            topic_sessions = []
            for pdf in pdfs:
                sessions = self.get_session_history(pdf_id=pdf['id'])
                topic_sessions.extend(sessions or [])
            
            # Calculate comprehensive topic analytics
            analytics = {
                'topic_id': topic_id,
                'total_pdfs': len(pdfs),
                'total_pages': total_pages,
                'pages_read': read_pages,
                'progress_percent': (read_pages / total_pages * 100) if total_pages > 0 else 0,
                'total_sessions': len(topic_sessions),
                'total_study_time': sum(s.get('total_time_seconds', 0) for s in topic_sessions),
                'average_session_length': 0,
                'estimated_completion_time': self._estimate_topic_completion(pdfs),
                'reading_velocity': self._calculate_reading_velocity(topic_sessions),
                'consistency_score': self._calculate_consistency_score(topic_sessions)
            }
            
            if analytics['total_sessions'] > 0:
                analytics['average_session_length'] = analytics['total_study_time'] / analytics['total_sessions']
            
            return analytics
            
        except Exception as e:
            logger.error(f"❌ Error getting topic analytics: {e}")
            return None
    
    def get_session_history(self, days=7, pdf_id=None, exercise_pdf_id=None):
        """Get enhanced session history with analytics"""
        try:
            sessions = self.db_manager.get_session_history(
                days=days,
                pdf_id=pdf_id,
                exercise_pdf_id=exercise_pdf_id
            )
            
            # Enhance sessions with derived metrics
            if sessions:
                for session in sessions:
                    session['efficiency'] = self._calculate_session_efficiency(session)
                    session['pace_rating'] = self._rate_reading_pace(session)
                    session['quality_score'] = self._calculate_session_quality(session)
            
            return sessions
            
        except Exception as e:
            logger.error(f"❌ Error getting session history: {e}")
            return []
    
    def _estimate_finish_date(self, estimated_minutes):
        """Estimate when user will finish based on reading patterns"""
        try:
            # Get user's average daily reading time from recent history
            recent_sessions = self.get_session_history(days=14)
            if not recent_sessions:
                return None
            
            # Calculate average daily reading time
            daily_totals = {}
            for session in recent_sessions:
                session_date = session.get('start_time', '')
                if isinstance(session_date, str):
                    session_date = session_date.split('T')[0]
                else:
                    session_date = session_date.strftime('%Y-%m-%d') if session_date else ''
                if session_date:
                    daily_totals[session_date] = daily_totals.get(session_date, 0) + session.get('total_time_seconds', 0)
            
            if not daily_totals:
                return None
            
            avg_daily_minutes = sum(daily_totals.values()) / len(daily_totals) / 60
            
            if avg_daily_minutes > 0:
                days_needed = estimated_minutes / avg_daily_minutes
                finish_date = datetime.now() + timedelta(days=days_needed)
                return {
                    'finish_date': finish_date.isoformat(),
                    'days_needed': round(days_needed),
                    'avg_daily_minutes': round(avg_daily_minutes)
                }
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Error estimating finish date: {e}")
            return None
    
    def _get_strategy_description(self, metrics):
        """Get description of which estimation strategy was used"""
        if not metrics:
            return "Default estimation (no reading history)"
        
        confidence = metrics.get('confidence', 'low')
        sample_size = metrics.get('sample_size', 0)
        
        if confidence == 'high':
            return f"Based on your reading data ({sample_size} pages)"
        elif confidence == 'medium':
            return f"Based on limited data ({sample_size} pages)"
        else:
            return "Estimated (insufficient reading data)"
    
    def _get_pace_description(self, avg_time_per_page):
        """Get descriptive text for reading pace"""
        if avg_time_per_page < 60:
            return "Fast pace"
        elif avg_time_per_page < 90:
            return "Moderate pace"
        elif avg_time_per_page < 150:
            return "Careful pace"
        else:
            return "Thorough pace"
    
    def _calculate_daily_goal_progress(self, daily_stats):
        """Calculate progress toward daily reading goals"""
        # This would integrate with a goal system if implemented
        target_minutes = 60  # Default 1 hour daily goal
        actual_minutes = daily_stats.get('total_time_seconds', 0) / 60
        
        return {
            'target_minutes': target_minutes,
            'actual_minutes': actual_minutes,
            'progress_percent': min(100, (actual_minutes / target_minutes) * 100),
            'goal_met': actual_minutes >= target_minutes
        }
    
    def _calculate_streak_quality(self, streaks):
        """Calculate quality metrics for reading streaks"""
        current_streak = streaks.get('current_streak_days', 0)
        streak_time = streaks.get('streak_total_time', 0)
        
        if current_streak > 0:
            avg_time_per_day = streak_time / current_streak / 60  # minutes per day
            
            quality = 'excellent' if avg_time_per_day >= 60 else \
                     'good' if avg_time_per_day >= 30 else \
                     'fair' if avg_time_per_day >= 15 else 'minimal'
            
            return {
                'streak_quality': quality,
                'avg_minutes_per_day': round(avg_time_per_day),
                'consistency_rating': self._rate_consistency(current_streak, avg_time_per_day)
            }
        
        return {'streak_quality': 'none', 'avg_minutes_per_day': 0, 'consistency_rating': 'none'}
    
    def _estimate_topic_completion(self, pdfs):
        """Estimate time to complete all PDFs in a topic"""
        total_remaining_pages = 0
        
        for pdf in pdfs:
            total_pages = pdf.get('total_pages', 0)
            current_page = pdf.get('current_page', 1)
            remaining = max(0, total_pages - current_page + 1)
            total_remaining_pages += remaining
        
        # Use user's average reading speed
        user_metrics = self.get_reading_speed(user_wide=True)
        if user_metrics and user_metrics.get('average_time_per_page_seconds'):
            avg_time = user_metrics['average_time_per_page_seconds']
        else:
            avg_time = 90  # Default 1.5 minutes per page
        
        estimated_seconds = total_remaining_pages * avg_time
        
        return {
            'remaining_pages': total_remaining_pages,
            'estimated_hours': estimated_seconds / 3600,
            'estimated_sessions': max(1, estimated_seconds / (25 * 60))  # 25-min sessions
        }
    
    def _calculate_reading_velocity(self, sessions):
        """Calculate reading velocity trend over time"""
        if len(sessions) < 2:
            return {'trend': 'insufficient_data', 'velocity': 0}
        
        # Sort sessions by date
        sorted_sessions = sorted(sessions, key=lambda x: x.get('start_time', ''))
        
        velocities = []
        for session in sorted_sessions:
            active_time = session.get('active_time_seconds', 0)
            pages = session.get('pages_visited', 0)
            if active_time > 0 and pages > 0:
                velocity = pages / (active_time / 60)  # pages per minute
                velocities.append(velocity)
        
        if len(velocities) < 2:
            return {'trend': 'insufficient_data', 'velocity': 0}
        
        # Calculate trend
        recent_avg = sum(velocities[-3:]) / len(velocities[-3:])
        early_avg = sum(velocities[:3]) / len(velocities[:3])
        
        trend = 'improving' if recent_avg > early_avg * 1.1 else \
               'declining' if recent_avg < early_avg * 0.9 else 'stable'
        
        return {
            'trend': trend,
            'current_velocity': recent_avg,
            'improvement_percent': ((recent_avg - early_avg) / early_avg) * 100 if early_avg > 0 else 0
        }
    
    def _calculate_consistency_score(self, sessions):
        """Calculate reading consistency score (0-100)"""
        if len(sessions) < 3:
            return 0
        
        # Group sessions by date
        daily_times = {}
        for session in sessions:
            date = session.get('start_time', '').split('T')[0] if isinstance(session.get('start_time', ''), str) else (session.get('start_time').strftime('%Y-%m-%d') if session.get('start_time') else '')
            if date:
                daily_times[date] = daily_times.get(date, 0) + session.get('total_time_seconds', 0)
        
        if len(daily_times) < 2:
            return 0
        
        # Calculate coefficient of variation (lower = more consistent)
        times = list(daily_times.values())
        if len(times) < 2:
            return 0
        
        mean_time = sum(times) / len(times)
        if mean_time == 0:
            return 0
        
        variance = sum((t - mean_time) ** 2 for t in times) / len(times)
        std_dev = variance ** 0.5
        cv = std_dev / mean_time
        
        # Convert to 0-100 score (lower CV = higher score)
        consistency_score = max(0, 100 - (cv * 100))
        
        return round(consistency_score)
    
    def _calculate_session_efficiency(self, session):
        """Calculate session efficiency (active time / total time)"""
        total_time = session.get('total_time_seconds', 0)
        active_time = session.get('active_time_seconds', 0)
        
        if total_time > 0:
            return (active_time / total_time) * 100
        return 0
    
    def _rate_reading_pace(self, session):
        """Rate the reading pace of a session"""
        active_time = session.get('active_time_seconds', 0)
        pages = session.get('pages_visited', 0)
        
        if active_time > 0 and pages > 0:
            pace = pages / (active_time / 60)  # pages per minute
            
            if pace >= 1.5:
                return 'fast'
            elif pace >= 0.8:
                return 'moderate'
            elif pace >= 0.4:
                return 'careful'
            else:
                return 'slow'
        
        return 'unknown'
    
    def _calculate_session_quality(self, session):
        """Calculate overall session quality score"""
        efficiency = self._calculate_session_efficiency(session)
        
        # Base score on efficiency
        quality_score = efficiency * 0.6  # 60% weight for efficiency
        
        # Add bonus for longer sessions (up to 20 points)
        total_time = session.get('total_time_seconds', 0)
        time_bonus = min(20, (total_time / 1800) * 20)  # Bonus for 30+ minute sessions
        quality_score += time_bonus
        
        # Add bonus for pages read (up to 20 points)
        pages = session.get('pages_visited', 0)
        page_bonus = min(20, pages * 2)  # 2 points per page, max 20
        quality_score += page_bonus
        
        return min(100, round(quality_score))
    
    def _rate_consistency(self, streak_days, avg_minutes_per_day):
        """Rate reading consistency"""
        if streak_days >= 7 and avg_minutes_per_day >= 30:
            return 'excellent'
        elif streak_days >= 5 and avg_minutes_per_day >= 20:
            return 'good'
        elif streak_days >= 3 and avg_minutes_per_day >= 15:
            return 'fair'
        else:
            return 'poor'