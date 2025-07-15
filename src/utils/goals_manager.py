# src/utils/goals_manager.py - Fixed version
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
    """Goal setting and progress tracking system"""
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
        
    def create_goal(self, topic_id: int, target_type: GoalType, target_value: int, 
                   deadline: Optional[date] = None) -> Optional[int]:
        """Create a new study goal"""
        try:
            # Validate inputs
            if target_type == GoalType.FINISH_BY_DATE:
                if not deadline or deadline <= date.today():
                    logger.error("Invalid deadline for finish_by_date goal")
                    return None
                target_value = 0  # Not used for deadline goals
            elif target_value <= 0:
                logger.error("Target value must be positive for daily goals")
                return None
            
            # Create goal in database
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
        """Get all active goals"""
        try:
            base_query = """
                SELECT g.*, t.name as topic_name
                FROM goals g
                LEFT JOIN topics t ON g.topic_id = t.id
                WHERE g.is_active = TRUE AND g.is_completed = FALSE
            """
            
            params = []
            if topic_id:
                base_query += " AND g.topic_id = %s"
                params.append(topic_id)
            
            base_query += " ORDER BY g.created_at DESC"
            
            self.db_manager.cursor.execute(base_query, params)
            goals = self.db_manager.cursor.fetchall()
            
            # Add basic status and progress
            enhanced_goals = []
            for goal in goals:
                goal_dict = dict(goal)
                goal_dict['status'] = 'on_track'  # Simple default
                goal_dict['progress_percentage'] = 0.0  # Simple default
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
            
            # Manual progress update (safe fallback)
            self._manual_update_progress(topic_id, pages_read, time_spent_minutes, session_date)
            
            logger.info(f"Updated goal progress: topic {topic_id}, {pages_read} pages, {time_spent_minutes}m")
            
        except Exception as e:
            logger.error(f"Error updating progress: {e}")
    
    def _manual_update_progress(self, topic_id: int, pages_read: int, time_spent_minutes: int, session_date: date):
        """Manual progress update - safe fallback method"""
        try:
            with self.db_manager.transaction():
                # Get active goals for this topic
                self.db_manager.cursor.execute("""
                    SELECT id, target_type, target_value 
                    FROM goals 
                    WHERE topic_id = %s AND is_active = TRUE AND is_completed = FALSE
                """, (topic_id,))
                
                goals = self.db_manager.cursor.fetchall()
                
                for goal in goals:
                    goal_id = goal['id']
                    target_type = goal['target_type']
                    target_value = goal['target_value']
                    
                    # Insert or update today's progress
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
                
        except Exception as e:
            logger.error(f"Error in manual update progress: {e}")
            raise
    
    def get_today_progress(self, topic_id: Optional[int] = None) -> Dict:
        """Get today's progress for goals"""
        try:
            # Simple implementation - get daily goals for today
            today = date.today()
            
            base_query = """
                SELECT g.*, t.name as topic_name,
                       COALESCE(gp.pages_read, 0) as pages_read_today,
                       COALESCE(gp.time_spent_minutes, 0) as time_spent_today,
                       COALESCE(gp.target_met, FALSE) as target_met_today
                FROM goals g
                LEFT JOIN topics t ON g.topic_id = t.id
                LEFT JOIN goal_progress gp ON g.id = gp.goal_id AND gp.date = %s
                WHERE g.is_active = TRUE AND g.is_completed = FALSE
            """
            
            params = [today]
            if topic_id:
                base_query += " AND g.topic_id = %s"
                params.append(topic_id)
            
            self.db_manager.cursor.execute(base_query, params)
            results = self.db_manager.cursor.fetchall()
            
            # Organize by goal type
            daily_goals = []
            deadline_goals = []
            
            for result in results:
                goal_dict = dict(result)
                if goal_dict['target_type'] in ['daily_pages', 'daily_time']:
                    daily_goals.append(goal_dict)
                else:
                    deadline_goals.append(goal_dict)
            
            completed_count = sum(1 for g in daily_goals if g['target_met_today'])
            total_count = len(daily_goals)
            
            if total_count == 0:
                overall_status = 'no_goals'
            elif completed_count == total_count:
                overall_status = 'all_completed'
            elif completed_count >= total_count * 0.7:
                overall_status = 'mostly_completed'
            elif completed_count > 0:
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
        """Get basic analytics for a goal"""
        try:
            # Simple implementation
            self.db_manager.cursor.execute("""
                SELECT date, pages_read, time_spent_minutes, target_met
                FROM goal_progress
                WHERE goal_id = %s AND date >= CURRENT_DATE - INTERVAL '%s days'
                ORDER BY date DESC
            """, (goal_id, days))
            
            progress_data = [dict(row) for row in self.db_manager.cursor.fetchall()]
            
            return {
                'goal_id': goal_id,
                'progress_data': progress_data
            }
            
        except Exception as e:
            logger.error(f"Error getting goal analytics: {e}")
            return {}
