# src/utils/goals_manager.py - Comprehensive Goal Setting & Progress Tracking
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple, Any
import logging
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class GoalType(Enum):
    FINISH_BY_DATE = "finish_by_date"
    DAILY_TIME = "daily_time"
    DAILY_PAGES = "daily_pages"

class GoalStatus(Enum):
    ON_TRACK = "on_track"
    SLIGHTLY_BEHIND = "slightly_behind"
    BEHIND = "behind"
    VERY_BEHIND = "very_behind"
    AHEAD = "ahead"
    COMPLETED = "completed"

@dataclass
class Goal:
    id: Optional[int]
    topic_id: int
    target_type: GoalType
    target_value: int
    deadline: Optional[date]
    is_active: bool
    is_completed: bool
    created_at: datetime
    completion_date: Optional[datetime] = None

@dataclass
class GoalProgress:
    goal_id: int
    date: date
    pages_read: int
    time_spent_minutes: int
    sessions_count: int
    target_met: bool

@dataclass
class DailyPlan:
    goal_id: int
    goal_type: GoalType
    pages_needed_today: int
    time_needed_today: int
    pages_behind: int
    days_remaining: int
    adjusted_daily_target: int
    status: GoalStatus
    message: str

class GoalsManager:
    """Comprehensive goal setting and progress tracking system"""
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
        
    def create_goal(self, topic_id: int, target_type: GoalType, target_value: int, 
                   deadline: Optional[date] = None) -> Optional[int]:
        """Create a new study goal with validation"""
        try:
            # Validate inputs
            if not self._validate_goal_inputs(target_type, target_value, deadline):
                return None
            
            # Check for existing active goals of same type for topic
            existing = self._get_existing_goals(topic_id, target_type)
            if existing:
                logger.warning(f"Active {target_type.value} goal already exists for topic {topic_id}")
                return None
            
            # Create goal in database
            with self.db_manager.transaction():
                self.db_manager.cursor.execute("""
                    INSERT INTO goals (topic_id, target_type, target_value, deadline)
                    VALUES (%s, %s, %s, %s) RETURNING id
                """, (topic_id, target_type.value, target_value, deadline))
                
                goal_id = self.db_manager.cursor.fetchone()['id']
                
                logger.info(f"Created {target_type.value} goal for topic {topic_id}: {target_value}")
                return goal_id
                
        except Exception as e:
            logger.error(f"Error creating goal: {e}")
            return None
    
    def get_active_goals(self, topic_id: Optional[int] = None) -> List[Dict]:
        """Get all active goals, optionally filtered by topic"""
        try:
            base_query = """
                SELECT g.*, t.name as topic_name,
                       COALESCE(SUM(gp.pages_read), 0) as total_pages_read,
                       COALESCE(SUM(gp.time_spent_minutes), 0) as total_time_spent
                FROM goals g
                LEFT JOIN topics t ON g.topic_id = t.id
                LEFT JOIN goal_progress gp ON g.id = gp.goal_id
                WHERE g.is_active = TRUE AND g.is_completed = FALSE
            """
            
            params = []
            if topic_id:
                base_query += " AND g.topic_id = %s"
                params.append(topic_id)
            
            base_query += " GROUP BY g.id, t.name ORDER BY g.created_at DESC"
            
            self.db_manager.cursor.execute(base_query, params)
            goals = self.db_manager.cursor.fetchall()
            
            # Enhance goals with status and progress
            enhanced_goals = []
            for goal in goals:
                goal_dict = dict(goal)
                goal_dict['status'] = self._calculate_goal_status(goal_dict)
                goal_dict['progress_percentage'] = self._calculate_progress_percentage(goal_dict)
                goal_dict['daily_plan'] = self.get_daily_plan(goal_dict['id'])
                enhanced_goals.append(goal_dict)
            
            return enhanced_goals
            
        except Exception as e:
            logger.error(f"Error getting active goals: {e}")
            return []
    
    def update_progress_after_session(self, topic_id: int, pages_read: int, 
                                    time_spent_seconds: int, session_date: Optional[date] = None):
        """Update goal progress after a study session"""
        try:
            if session_date is None:
                session_date = date.today()
                
            time_spent_minutes = time_spent_seconds // 60
            
            # Call database function to update progress
            self.db_manager.cursor.execute("""
                SELECT update_goal_progress_after_session(%s, %s, %s)
            """, (topic_id, pages_read, time_spent_minutes))
            
            self.db_manager.connection.commit()
            
            # Check if any goals need adjustment
            self._check_and_adjust_goals(topic_id)
            
            logger.info(f"Updated goal progress: topic {topic_id}, {pages_read} pages, {time_spent_minutes}m")
            
        except Exception as e:
            logger.error(f"Error updating progress: {e}")
    
    def get_daily_plan(self, goal_id: int) -> Optional[DailyPlan]:
        """Calculate daily plan for a specific goal"""
        try:
            # Get goal details
            self.db_manager.cursor.execute("""
                SELECT g.*, t.name as topic_name
                FROM goals g
                LEFT JOIN topics t ON g.topic_id = t.id
                WHERE g.id = %s
            """, (goal_id,))
            
            goal = self.db_manager.cursor.fetchone()
            if not goal:
                return None
            
            goal_type = GoalType(goal['target_type'])
            
            if goal_type == GoalType.FINISH_BY_DATE:
                return self._calculate_deadline_plan(goal)
            elif goal_type == GoalType.DAILY_PAGES:
                return self._calculate_daily_pages_plan(goal)
            elif goal_type == GoalType.DAILY_TIME:
                return self._calculate_daily_time_plan(goal)
                
        except Exception as e:
            logger.error(f"Error calculating daily plan: {e}")
            return None
    
    def get_today_progress(self, topic_id: Optional[int] = None) -> Dict:
        """Get today's progress for all or specific topic goals"""
        try:
            base_query = """
                SELECT * FROM daily_goal_status
            """
            
            params = []
            if topic_id:
                base_query += " WHERE goal_id IN (SELECT id FROM goals WHERE topic_id = %s)"
                params.append(topic_id)
            
            self.db_manager.cursor.execute(base_query, params)
            results = self.db_manager.cursor.fetchall()
            
            # Organize by goal type
            today_status = {
                'daily_goals': [],
                'deadline_goals': [],
                'overall_status': self._calculate_overall_daily_status(results)
            }
            
            for result in results:
                goal_dict = dict(result)
                goal_dict['status'] = self._get_daily_status(goal_dict)
                
                if goal_dict['target_type'] in ['daily_pages', 'daily_time']:
                    today_status['daily_goals'].append(goal_dict)
                else:
                    today_status['deadline_goals'].append(goal_dict)
            
            return today_status
            
        except Exception as e:
            logger.error(f"Error getting today's progress: {e}")
            return {'daily_goals': [], 'deadline_goals': [], 'overall_status': 'error'}
    
    def get_goal_analytics(self, goal_id: int, days: int = 30) -> Dict:
        """Get comprehensive analytics for a specific goal"""
        try:
            # Get goal progress over time
            self.db_manager.cursor.execute("""
                SELECT date, pages_read, time_spent_minutes, target_met, sessions_count
                FROM goal_progress
                WHERE goal_id = %s AND date >= CURRENT_DATE - INTERVAL '%s days'
                ORDER BY date DESC
            """, (goal_id, days))
            
            progress_data = [dict(row) for row in self.db_manager.cursor.fetchall()]
            
            # Get goal adjustments
            self.db_manager.cursor.execute("""
                SELECT adjustment_date, old_daily_target, new_daily_target, 
                       reason, pages_behind, days_remaining
                FROM goal_adjustments
                WHERE goal_id = %s
                ORDER BY adjustment_date DESC
            """, (goal_id,))
            
            adjustments = [dict(row) for row in self.db_manager.cursor.fetchall()]
            
            # Calculate analytics
            analytics = {
                'goal_id': goal_id,
                'progress_data': progress_data,
                'adjustments': adjustments,
                'streak_days': self._calculate_goal_streak(progress_data),
                'average_daily_pages': self._calculate_average_daily_pages(progress_data),
                'average_daily_time': self._calculate_average_daily_time(progress_data),
                'target_met_percentage': self._calculate_target_met_percentage(progress_data),
                'consistency_score': self._calculate_consistency_score(progress_data),
                'trend_analysis': self._analyze_progress_trend(progress_data)
            }
            
            return analytics
            
        except Exception as e:
            logger.error(f"Error getting goal analytics: {e}")
            return {}
    
    def adjust_goal(self, goal_id: int, new_target_value: int, reason: str = "manual_adjustment"):
        """Manually adjust a goal's target value"""
        try:
            with self.db_manager.transaction():
                # Get current goal
                self.db_manager.cursor.execute("""
                    SELECT target_value FROM goals WHERE id = %s
                """, (goal_id,))
                
                current = self.db_manager.cursor.fetchone()
                if not current:
                    return False
                
                old_target = current['target_value']
                
                # Update goal
                self.db_manager.cursor.execute("""
                    UPDATE goals SET 
                        target_value = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (new_target_value, goal_id))
                
                # Record adjustment
                self.db_manager.cursor.execute("""
                    INSERT INTO goal_adjustments 
                    (goal_id, adjustment_date, old_daily_target, new_daily_target, reason)
                    VALUES (%s, CURRENT_DATE, %s, %s, %s)
                """, (goal_id, old_target, new_target_value, reason))
                
                logger.info(f"Adjusted goal {goal_id}: {old_target} → {new_target_value} ({reason})")
                return True
                
        except Exception as e:
            logger.error(f"Error adjusting goal: {e}")
            return False
    
    def complete_goal(self, goal_id: int):
        """Mark a goal as completed"""
        try:
            with self.db_manager.transaction():
                self.db_manager.cursor.execute("""
                    UPDATE goals SET
                        is_completed = TRUE,
                        completion_date = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (goal_id,))
                
                logger.info(f"Completed goal {goal_id}")
                return True
                
        except Exception as e:
            logger.error(f"Error completing goal: {e}")
            return False
    
    def deactivate_goal(self, goal_id: int):
        """Deactivate a goal without completing it"""
        try:
            with self.db_manager.transaction():
                self.db_manager.cursor.execute("""
                    UPDATE goals SET
                        is_active = FALSE,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (goal_id,))
                
                logger.info(f"Deactivated goal {goal_id}")
                return True
                
        except Exception as e:
            logger.error(f"Error deactivating goal: {e}")
            return False
    
    def _validate_goal_inputs(self, target_type: GoalType, target_value: int, 
                            deadline: Optional[date]) -> bool:
        """Validate goal creation inputs"""
        # For finish_by_date goals, target_value is not used, so allow 0
        if target_type != GoalType.FINISH_BY_DATE and target_value <= 0:
            logger.error("Target value must be positive")
            return False
        
        if target_type == GoalType.FINISH_BY_DATE:
            if not deadline:
                logger.error("Deadline required for finish_by_date goals")
                return False
            if deadline <= date.today():
                logger.error("Deadline must be in the future")
                return False
        else:
            if deadline:
                logger.error("Deadline not allowed for daily goals")
                return False
        
        # Reasonable limits
        if target_type == GoalType.DAILY_TIME and target_value > 480:  # 8 hours
            logger.error("Daily time goal too high (max 8 hours)")
            return False
        
        if target_type == GoalType.DAILY_PAGES and target_value > 100:  # 100 pages
            logger.error("Daily pages goal too high (max 100 pages)")
            return False
        
        return True
    
    def _get_existing_goals(self, topic_id: int, target_type: GoalType) -> List[Dict]:
        """Check for existing active goals of same type"""
        try:
            self.db_manager.cursor.execute("""
                SELECT * FROM goals
                WHERE topic_id = %s AND target_type = %s 
                AND is_active = TRUE AND is_completed = FALSE
            """, (topic_id, target_type.value))
            
            return [dict(row) for row in self.db_manager.cursor.fetchall()]
            
        except Exception as e:
            logger.error(f"Error checking existing goals: {e}")
            return []
    
    def _calculate_deadline_plan(self, goal: Dict) -> DailyPlan:
        """Calculate daily plan for finish_by_date goals"""
        try:
            today = date.today()
            deadline = goal['deadline']
            days_remaining = (deadline - today).days
            
            # Get remaining pages for topic
            self.db_manager.cursor.execute("""
                SELECT COALESCE(SUM(total_pages - GREATEST(current_page - 1, 0)), 0) as remaining_pages
                FROM pdfs WHERE topic_id = %s
            """, (goal['topic_id'],))
            
            remaining_pages = self.db_manager.cursor.fetchone()['remaining_pages']
            
            # Calculate pages behind using database function
            self.db_manager.cursor.execute("""
                SELECT calculate_pages_behind(%s) as pages_behind
            """, (goal['id'],))
            
            pages_behind = self.db_manager.cursor.fetchone()['pages_behind']
            
            # Calculate adjusted daily target
            if days_remaining > 0:
                adjusted_daily_target = max(1, (remaining_pages + pages_behind) // days_remaining)
            else:
                adjusted_daily_target = remaining_pages
            
            # Determine status
            status = self._determine_deadline_status(pages_behind, days_remaining)
            
            # Generate message
            message = self._generate_deadline_message(status, pages_behind, days_remaining, adjusted_daily_target)
            
            return DailyPlan(
                goal_id=goal['id'],
                goal_type=GoalType.FINISH_BY_DATE,
                pages_needed_today=adjusted_daily_target,
                time_needed_today=0,  # Not applicable
                pages_behind=pages_behind,
                days_remaining=days_remaining,
                adjusted_daily_target=adjusted_daily_target,
                status=status,
                message=message
            )
            
        except Exception as e:
            logger.error(f"Error calculating deadline plan: {e}")
            return None
    
    def _calculate_daily_pages_plan(self, goal: Dict) -> DailyPlan:
        """Calculate plan for daily pages goals"""
        try:
            # Get today's progress
            self.db_manager.cursor.execute("""
                SELECT COALESCE(pages_read, 0) as pages_today
                FROM goal_progress
                WHERE goal_id = %s AND date = CURRENT_DATE
            """, (goal['id'],))
            
            result = self.db_manager.cursor.fetchone()
            pages_today = result['pages_today'] if result else 0
            
            target_pages = goal['target_value']
            pages_needed = max(0, target_pages - pages_today)
            
            # Determine status
            if pages_today >= target_pages:
                status = GoalStatus.COMPLETED
                message = f"🎉 Daily goal completed! You read {pages_today} pages (target: {target_pages})"
            elif pages_today >= target_pages * 0.8:
                status = GoalStatus.ON_TRACK
                message = f"📖 Almost there! {pages_needed} more pages to reach your daily goal"
            elif pages_today >= target_pages * 0.5:
                status = GoalStatus.SLIGHTLY_BEHIND
                message = f"⚡ Pick up the pace! {pages_needed} pages remaining for today"
            else:
                status = GoalStatus.BEHIND
                message = f"🚀 Let's get reading! {pages_needed} pages to reach your daily goal"
            
            return DailyPlan(
                goal_id=goal['id'],
                goal_type=GoalType.DAILY_PAGES,
                pages_needed_today=pages_needed,
                time_needed_today=0,
                pages_behind=0,
                days_remaining=0,
                adjusted_daily_target=target_pages,
                status=status,
                message=message
            )
            
        except Exception as e:
            logger.error(f"Error calculating daily pages plan: {e}")
            return None
    
    def _calculate_daily_time_plan(self, goal: Dict) -> DailyPlan:
        """Calculate plan for daily time goals"""
        try:
            # Get today's progress
            self.db_manager.cursor.execute("""
                SELECT COALESCE(time_spent_minutes, 0) as time_today
                FROM goal_progress
                WHERE goal_id = %s AND date = CURRENT_DATE
            """, (goal['id'],))
            
            result = self.db_manager.cursor.fetchone()
            time_today = result['time_today'] if result else 0
            
            target_minutes = goal['target_value']
            time_needed = max(0, target_minutes - time_today)
            
            # Determine status
            if time_today >= target_minutes:
                status = GoalStatus.COMPLETED
                message = f"⏰ Daily time goal achieved! You studied for {time_today} minutes (target: {target_minutes})"
            elif time_today >= target_minutes * 0.8:
                status = GoalStatus.ON_TRACK
                message = f"🕒 Almost there! {time_needed} more minutes to reach your daily goal"
            elif time_today >= target_minutes * 0.5:
                status = GoalStatus.SLIGHTLY_BEHIND
                message = f"⏳ Keep going! {time_needed} minutes remaining for today"
            else:
                status = GoalStatus.BEHIND
                message = f"📚 Time to study! {time_needed} minutes to reach your daily goal"
            
            return DailyPlan(
                goal_id=goal['id'],
                goal_type=GoalType.DAILY_TIME,
                pages_needed_today=0,
                time_needed_today=time_needed,
                pages_behind=0,
                days_remaining=0,
                adjusted_daily_target=target_minutes,
                status=status,
                message=message
            )
            
        except Exception as e:
            logger.error(f"Error calculating daily time plan: {e}")
            return None
    
    def _calculate_goal_status(self, goal: Dict) -> GoalStatus:
        """Calculate overall status for a goal"""
        try:
            goal_type = GoalType(goal['target_type'])
            
            if goal['is_completed']:
                return GoalStatus.COMPLETED
            
            if goal_type == GoalType.FINISH_BY_DATE:
                return self._calculate_deadline_goal_status(goal)
            else:
                return self._calculate_daily_goal_status(goal)
                
        except Exception as e:
            logger.error(f"Error calculating goal status: {e}")
            return GoalStatus.ON_TRACK
    
    def _calculate_deadline_goal_status(self, goal: Dict) -> GoalStatus:
        """Calculate status for deadline-based goals"""
        try:
            today = date.today()
            deadline = goal['deadline']
            days_remaining = (deadline - today).days
            
            # Get pages behind
            self.db_manager.cursor.execute("""
                SELECT calculate_pages_behind(%s) as pages_behind
            """, (goal['id'],))
            
            pages_behind = self.db_manager.cursor.fetchone()['pages_behind']
            
            return self._determine_deadline_status(pages_behind, days_remaining)
            
        except Exception as e:
            logger.error(f"Error calculating deadline goal status: {e}")
            return GoalStatus.ON_TRACK
    
    def _calculate_daily_goal_status(self, goal: Dict) -> GoalStatus:
        """Calculate status for daily goals based on recent performance"""
        try:
            # Get last 7 days of progress
            self.db_manager.cursor.execute("""
                SELECT target_met
                FROM goal_progress
                WHERE goal_id = %s AND date >= CURRENT_DATE - INTERVAL '7 days'
                ORDER BY date DESC
            """, (goal['id'],))
            
            recent_progress = [row['target_met'] for row in self.db_manager.cursor.fetchall()]
            
            if not recent_progress:
                return GoalStatus.ON_TRACK
            
            success_rate = sum(recent_progress) / len(recent_progress)
            
            if success_rate >= 0.8:
                return GoalStatus.ON_TRACK
            elif success_rate >= 0.6:
                return GoalStatus.SLIGHTLY_BEHIND
            else:
                return GoalStatus.BEHIND
                
        except Exception as e:
            logger.error(f"Error calculating daily goal status: {e}")
            return GoalStatus.ON_TRACK
    
    def _determine_deadline_status(self, pages_behind: int, days_remaining: int) -> GoalStatus:
        """Determine status based on pages behind and days remaining"""
        if pages_behind <= 0:
            return GoalStatus.ON_TRACK
        elif days_remaining <= 0:
            return GoalStatus.VERY_BEHIND
        else:
            pages_per_day_to_catch_up = pages_behind / days_remaining
            if pages_per_day_to_catch_up <= 2:
                return GoalStatus.SLIGHTLY_BEHIND
            elif pages_per_day_to_catch_up <= 5:
                return GoalStatus.BEHIND
            else:
                return GoalStatus.VERY_BEHIND
    
    def _generate_deadline_message(self, status: GoalStatus, pages_behind: int, 
                                 days_remaining: int, adjusted_target: int) -> str:
        """Generate user-friendly message for deadline goals"""
        if status == GoalStatus.ON_TRACK:
            return f"🎯 On track! Read {adjusted_target} pages daily to finish on time"
        elif status == GoalStatus.SLIGHTLY_BEHIND:
            return f"⚡ {pages_behind} pages behind. Read {adjusted_target} pages daily to catch up"
        elif status == GoalStatus.BEHIND:
            return f"🚀 {pages_behind} pages behind. Increase to {adjusted_target} pages daily"
        elif status == GoalStatus.VERY_BEHIND:
            if days_remaining <= 0:
                return f"⏰ Deadline passed. Consider adjusting your goal"
            else:
                return f"🔥 Significantly behind! Need {adjusted_target} pages daily to finish"
        else:
            return "📚 Keep up the great work!"
    
    def _calculate_progress_percentage(self, goal: Dict) -> float:
        """Calculate completion percentage for a goal"""
        try:
            goal_type = GoalType(goal['target_type'])
            
            if goal_type == GoalType.FINISH_BY_DATE:
                # Get total pages needed vs read
                self.db_manager.cursor.execute("""
                    SELECT 
                        COALESCE(SUM(total_pages), 0) as total_pages,
                        COALESCE(SUM(GREATEST(current_page - 1, 0)), 0) as pages_read
                    FROM pdfs WHERE topic_id = %s
                """, (goal['topic_id'],))
                
                result = self.db_manager.cursor.fetchone()
                total_pages = result['total_pages']
                pages_read = result['pages_read']
                
                if total_pages > 0:
                    return min(100.0, (pages_read / total_pages) * 100)
                
            else:
                # For daily goals, calculate success rate over last 30 days
                self.db_manager.cursor.execute("""
                    SELECT 
                        COUNT(*) as total_days,
                        COUNT(CASE WHEN target_met THEN 1 END) as successful_days
                    FROM goal_progress
                    WHERE goal_id = %s AND date >= CURRENT_DATE - INTERVAL '30 days'
                """, (goal['id'],))
                
                result = self.db_manager.cursor.fetchone()
                if result and result['total_days'] > 0:
                    return (result['successful_days'] / result['total_days']) * 100
            
            return 0.0
            
        except Exception as e:
            logger.error(f"Error calculating progress percentage: {e}")
            return 0.0
    
    def _check_and_adjust_goals(self, topic_id: int):
        """Check if goals need automatic adjustment and apply them"""
        try:
            # Get active deadline goals for topic
            self.db_manager.cursor.execute("""
                SELECT * FROM goals
                WHERE topic_id = %s AND target_type = 'finish_by_date'
                AND is_active = TRUE AND is_completed = FALSE
            """, (topic_id,))
            
            goals = self.db_manager.cursor.fetchall()
            
            for goal in goals:
                goal_dict = dict(goal)
                plan = self._calculate_deadline_plan(goal_dict)
                
                if plan and plan.status in [GoalStatus.BEHIND, GoalStatus.VERY_BEHIND]:
                    # Check if we need to record an adjustment
                    if plan.adjusted_daily_target != goal['target_value']:
                        self._record_automatic_adjustment(
                            goal['id'], 
                            goal['target_value'], 
                            plan.adjusted_daily_target,
                            plan.pages_behind,
                            plan.days_remaining
                        )
                        
        except Exception as e:
            logger.error(f"Error checking goal adjustments: {e}")
    
    def _record_automatic_adjustment(self, goal_id: int, old_target: int, 
                                   new_target: int, pages_behind: int, days_remaining: int):
        """Record automatic goal adjustment"""
        try:
            with self.db_manager.transaction():
                self.db_manager.cursor.execute("""
                    INSERT INTO goal_adjustments
                    (goal_id, adjustment_date, old_daily_target, new_daily_target,
                     reason, pages_behind, days_remaining)
                    VALUES (%s, CURRENT_DATE, %s, %s, %s, %s, %s)
                """, (goal_id, old_target, new_target, 'behind_schedule', 
                      pages_behind, days_remaining))
                
                logger.info(f"Recorded adjustment for goal {goal_id}: {old_target} → {new_target}")
                
        except Exception as e:
            logger.error(f"Error recording adjustment: {e}")
    
    def _calculate_overall_daily_status(self, results: List[Dict]) -> str:
        """Calculate overall daily status across all goals"""
        if not results:
            return 'no_goals'
        
        completed_count = sum(1 for r in results if r['target_met_today'])
        total_count = len(results)
        
        if completed_count == total_count:
            return 'all_completed'
        elif completed_count >= total_count * 0.7:
            return 'mostly_completed'
        elif completed_count > 0:
            return 'partially_completed'
        else:
            return 'none_completed'
    
    def _get_daily_status(self, goal_dict: Dict) -> str:
        """Get status for a single daily goal"""
        if goal_dict['target_met_today']:
            return 'completed'
        
        if goal_dict['target_type'] == 'daily_pages':
            progress = goal_dict['pages_read_today'] / goal_dict['target_value']
        else:  # daily_time
            progress = goal_dict['time_spent_today'] / goal_dict['target_value']
        
        if progress >= 0.8:
            return 'almost_done'
        elif progress >= 0.5:
            return 'halfway'
        elif progress > 0:
            return 'started'
        else:
            return 'not_started'
    
    # Analytics helper methods
    def _calculate_goal_streak(self, progress_data: List[Dict]) -> int:
        """Calculate current streak of meeting daily targets"""
        streak = 0
        for day in progress_data:
            if day['target_met']:
                streak += 1
            else:
                break
        return streak
    
    def _calculate_average_daily_pages(self, progress_data: List[Dict]) -> float:
        """Calculate average pages read per day"""
        if not progress_data:
            return 0.0
        
        total_pages = sum(day['pages_read'] for day in progress_data)
        return total_pages / len(progress_data)
    
    def _calculate_average_daily_time(self, progress_data: List[Dict]) -> float:
        """Calculate average study time per day"""
        if not progress_data:
            return 0.0
        
        total_time = sum(day['time_spent_minutes'] for day in progress_data)
        return total_time / len(progress_data)
    
    def _calculate_target_met_percentage(self, progress_data: List[Dict]) -> float:
        """Calculate percentage of days where target was met"""
        if not progress_data:
            return 0.0
        
        met_count = sum(1 for day in progress_data if day['target_met'])
        return (met_count / len(progress_data)) * 100
    
    def _calculate_consistency_score(self, progress_data: List[Dict]) -> float:
        """Calculate consistency score based on daily variance"""
        if len(progress_data) < 2:
            return 0.0
        
        # Calculate coefficient of variation for pages read
        pages = [day['pages_read'] for day in progress_data]
        if not pages:
            return 0.0
        
        mean_pages = sum(pages) / len(pages)
        if mean_pages == 0:
            return 0.0
        
        variance = sum((p - mean_pages) ** 2 for p in pages) / len(pages)
        std_dev = variance ** 0.5
        cv = std_dev / mean_pages
        
        # Convert to 0-100 score (lower CV = higher consistency)
        return max(0, 100 - (cv * 50))
    
    def _analyze_progress_trend(self, progress_data: List[Dict]) -> Dict:
        """Analyze progress trend over time"""
        if len(progress_data) < 3:
            return {'trend': 'insufficient_data'}
        
        # Split into recent and earlier periods
        recent = progress_data[:len(progress_data)//2]
        earlier = progress_data[len(progress_data)//2:]
        
        recent_avg = sum(day['pages_read'] for day in recent) / len(recent)
        earlier_avg = sum(day['pages_read'] for day in earlier) / len(earlier)
        
        if recent_avg > earlier_avg * 1.1:
            trend = 'improving'
        elif recent_avg < earlier_avg * 0.9:
            trend = 'declining'
        else:
            trend = 'stable'
        
        change_percent = ((recent_avg - earlier_avg) / earlier_avg * 100) if earlier_avg > 0 else 0
        
        return {
            'trend': trend,
            'change_percent': round(change_percent, 1),
            'recent_average': round(recent_avg, 1),
            'earlier_average': round(earlier_avg, 1)
        }