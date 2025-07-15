# src/utils/goals_manager.py - Optimized Version
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional
from enum import Enum
from dataclasses import dataclass
import logging

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
class DailyPlan:
    goal_id: int
    pages_needed: int
    time_needed: int
    status: GoalStatus
    message: str

class GoalsManager:
    """Optimized goal management system"""
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
    
    def create_goal(self, topic_id: int, target_type: GoalType, target_value: int, 
                   deadline: Optional[date] = None) -> Optional[int]:
        """Create a new goal"""
        try:
            # Validate inputs
            if target_type == GoalType.FINISH_BY_DATE:
                if not deadline or deadline <= date.today():
                    return None
                target_value = 0
            elif target_value <= 0:
                return None
            
            # Create in database
            with self.db_manager.transaction():
                self.db_manager.cursor.execute("""
                    INSERT INTO goals (topic_id, target_type, target_value, deadline)
                    VALUES (%s, %s, %s, %s) RETURNING id
                """, (topic_id, target_type.value, target_value, deadline))
                
                goal_id = self.db_manager.cursor.fetchone()['id']
                logger.info(f"Created {target_type.value} goal for topic {topic_id}")
                return goal_id
                
        except Exception as e:
            logger.error(f"Error creating goal: {e}")
            return None
    
    def get_active_goals(self, topic_id: Optional[int] = None) -> List[Dict]:
        """Get all active goals with enhanced data"""
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
            
            # Enhance with status and progress
            enhanced_goals = []
            for goal in goals:
                goal_dict = dict(goal)
                goal_dict.update(self._calculate_goal_status(goal_dict))
                enhanced_goals.append(goal_dict)
            
            return enhanced_goals
            
        except Exception as e:
            logger.error(f"Error getting active goals: {e}")
            return []
    
    def update_progress_after_session(self, topic_id: int, pages_read: int, 
                                    time_spent_seconds: int, session_date: Optional[date] = None):
        """Update goal progress after session"""
        try:
            if session_date is None:
                session_date = date.today()
                
            time_spent_minutes = time_spent_seconds // 60
            
            with self.db_manager.transaction():
                # Get active goals for topic
                self.db_manager.cursor.execute("""
                    SELECT id, target_type, target_value 
                    FROM goals 
                    WHERE topic_id = %s AND is_active = TRUE AND is_completed = FALSE
                """, (topic_id,))
                
                goals = self.db_manager.cursor.fetchall()
                
                for goal in goals:
                    self._update_goal_progress(goal, pages_read, time_spent_minutes, session_date)
            
            logger.info(f"Updated goals for topic {topic_id}: {pages_read} pages, {time_spent_minutes}m")
            
        except Exception as e:
            logger.error(f"Error updating progress: {e}")
    
    def get_today_progress(self, topic_id: Optional[int] = None) -> Dict:
        """Get today's goal progress"""
        try:
            today = date.today()
            
            # Get daily goals
            daily_query = """
                SELECT g.*, t.name as topic_name,
                       COALESCE(gp.pages_read, 0) as pages_read_today,
                       COALESCE(gp.time_spent_minutes, 0) as time_spent_today,
                       COALESCE(gp.target_met, FALSE) as target_met_today
                FROM goals g
                LEFT JOIN topics t ON g.topic_id = t.id
                LEFT JOIN goal_progress gp ON g.id = gp.goal_id AND gp.date = %s
                WHERE g.is_active = TRUE AND g.is_completed = FALSE
                AND g.target_type IN ('daily_pages', 'daily_time')
            """
            
            # Get deadline goals
            deadline_query = """
                SELECT g.*, t.name as topic_name,
                       COALESCE(gp.pages_read, 0) as pages_read_today,
                       COALESCE(gp.time_spent_minutes, 0) as time_spent_today
                FROM goals g
                LEFT JOIN topics t ON g.topic_id = t.id
                LEFT JOIN goal_progress gp ON g.id = gp.goal_id AND gp.date = %s
                WHERE g.is_active = TRUE AND g.is_completed = FALSE
                AND g.target_type = 'finish_by_date'
            """
            
            params = [today]
            if topic_id:
                daily_query += " AND g.topic_id = %s"
                deadline_query += " AND g.topic_id = %s"
                params.append(topic_id)
            
            # Execute queries
            self.db_manager.cursor.execute(daily_query, params)
            daily_goals = [dict(row) for row in self.db_manager.cursor.fetchall()]
            
            self.db_manager.cursor.execute(deadline_query, params[:1] + ([topic_id] if topic_id else []))
            deadline_goals = [dict(row) for row in self.db_manager.cursor.fetchall()]
            
            # Calculate overall status
            completed_daily = sum(1 for g in daily_goals if g['target_met_today'])
            total_daily = len(daily_goals)
            
            if total_daily == 0:
                overall_status = 'no_goals'
            elif completed_daily == total_daily:
                overall_status = 'all_completed'
            elif completed_daily >= total_daily * 0.7:
                overall_status = 'mostly_completed'
            elif completed_daily > 0:
                overall_status = 'partially_completed'
            else:
                overall_status = 'none_completed'
            
            return {
                'daily_goals': daily_goals,
                'deadline_goals': deadline_goals,
                'overall_status': overall_status
            }
            
        except Exception as e:
            logger.error(f"Error getting today's progress: {e}")
            return {'daily_goals': [], 'deadline_goals': [], 'overall_status': 'error'}
    
    def get_goal_analytics(self, goal_id: int, days: int = 30) -> Dict:
        """Get goal analytics"""
        try:
            self.db_manager.cursor.execute("""
                SELECT date, pages_read, time_spent_minutes, target_met
                FROM goal_progress
                WHERE goal_id = %s AND date >= CURRENT_DATE - INTERVAL '%s days'
                ORDER BY date DESC
            """, (goal_id, days))
            
            progress_data = [dict(row) for row in self.db_manager.cursor.fetchall()]
            
            return {
                'goal_id': goal_id,
                'progress_data': progress_data,
                'analytics': self._calculate_analytics(progress_data)
            }
            
        except Exception as e:
            logger.error(f"Error getting goal analytics: {e}")
            return {}
    
    def _update_goal_progress(self, goal, pages_read, time_spent_minutes, session_date):
        """Update individual goal progress"""
        goal_id = goal['id']
        target_type = goal['target_type']
        target_value = goal['target_value']
        
        # Insert or update progress
        self.db_manager.cursor.execute("""
            INSERT INTO goal_progress (goal_id, date, pages_read, time_spent_minutes, sessions_count)
            VALUES (%s, %s, %s, %s, 1)
            ON CONFLICT (goal_id, date) 
            DO UPDATE SET
                pages_read = goal_progress.pages_read + EXCLUDED.pages_read,
                time_spent_minutes = goal_progress.time_spent_minutes + EXCLUDED.time_spent_minutes,
                sessions_count = goal_progress.sessions_count + EXCLUDED.sessions_count,
                updated_at = CURRENT_TIMESTAMP
        """, (goal_id, session_date, pages_read, time_spent_minutes))
        
        # Update target_met status
        self.db_manager.cursor.execute("""
            UPDATE goal_progress SET
                target_met = CASE 
                    WHEN %s = 'daily_pages' THEN pages_read >= %s
                    WHEN %s = 'daily_time' THEN time_spent_minutes >= %s
                    ELSE target_met
                END,
                updated_at = CURRENT_TIMESTAMP
            WHERE goal_id = %s AND date = %s
        """, (target_type, target_value, target_type, target_value, goal_id, session_date))
    
    def _calculate_goal_status(self, goal) -> Dict:
        """Calculate goal status and progress"""
        target_type = goal['target_type']
        
        if target_type == 'finish_by_date':
            return self._calculate_deadline_status(goal)
        else:
            return self._calculate_daily_status(goal)
    
    def _calculate_deadline_status(self, goal) -> Dict:
        """Calculate status for deadline goals"""
        try:
            deadline = goal['deadline']
            if isinstance(deadline, str):
                deadline = datetime.fromisoformat(deadline).date()
            
            days_remaining = (deadline - date.today()).days
            
            # Get topic progress
            topic_id = goal['topic_id']
            pdfs = self.db_manager.get_pdfs_by_topic(topic_id)
            
            total_pages = sum(pdf.get('total_pages', 0) for pdf in pdfs)
            read_pages = sum(pdf.get('current_page', 1) - 1 for pdf in pdfs)
            
            progress_percent = (read_pages / total_pages * 100) if total_pages > 0 else 0
            
            # Determine status
            if progress_percent >= 100:
                status = GoalStatus.COMPLETED
            elif days_remaining <= 0:
                status = GoalStatus.VERY_BEHIND
            else:
                expected_progress = ((goal['created_at'].date() - deadline).days + days_remaining) / (goal['created_at'].date() - deadline).days * 100
                if progress_percent >= expected_progress:
                    status = GoalStatus.ON_TRACK
                elif progress_percent >= expected_progress * 0.8:
                    status = GoalStatus.SLIGHTLY_BEHIND
                else:
                    status = GoalStatus.BEHIND
            
            return {
                'status': status.value,
                'progress_percentage': progress_percent,
                'days_remaining': days_remaining,
                'pages_remaining': max(0, total_pages - read_pages)
            }
            
        except Exception as e:
            logger.error(f"Error calculating deadline status: {e}")
            return {'status': 'on_track', 'progress_percentage': 0}
    
    def _calculate_daily_status(self, goal) -> Dict:
        """Calculate status for daily goals"""
        today_progress = goal.get('total_pages_read', 0) if goal['target_type'] == 'daily_pages' else goal.get('total_time_spent', 0)
        target = goal['target_value']
        
        progress_percent = (today_progress / target * 100) if target > 0 else 0
        
        if progress_percent >= 100:
            status = GoalStatus.COMPLETED
        elif progress_percent >= 75:
            status = GoalStatus.ON_TRACK
        elif progress_percent >= 50:
            status = GoalStatus.SLIGHTLY_BEHIND
        else:
            status = GoalStatus.BEHIND
        
        return {
            'status': status.value,
            'progress_percentage': min(100, progress_percent),
            'current_value': today_progress,
            'target_value': target
        }
    
    def _calculate_analytics(self, progress_data) -> Dict:
        """Calculate basic analytics from progress data"""
        if not progress_data:
            return {}
        
        total_days = len(progress_data)
        met_days = sum(1 for day in progress_data if day['target_met'])
        completion_rate = (met_days / total_days * 100) if total_days > 0 else 0
        
        return {
            'total_days': total_days,
            'successful_days': met_days,
            'completion_rate': completion_rate,
            'streak_days': self._calculate_current_streak(progress_data)
        }
    
    def _calculate_current_streak(self, progress_data) -> int:
        """Calculate current streak of successful days"""
        streak = 0
        for day in sorted(progress_data, key=lambda x: x['date'], reverse=True):
            if day['target_met']:
                streak += 1
            else:
                break
        return streak