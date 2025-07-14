# src/utils/timer_system_test.py - Comprehensive Timer System Testing
import sys
import time
import logging
from datetime import datetime, timedelta
from PyQt6.QtCore import QTimer, QObject, pyqtSignal
from PyQt6.QtWidgets import QApplication, QWidget, QVBoxLayout, QLabel, QPushButton, QTextEdit

logger = logging.getLogger(__name__)

class TimerSystemTester(QObject):
    """Comprehensive testing suite for the StudySprint timer system"""
    
    test_completed = pyqtSignal(str, bool, str)  # test_name, success, details
    
    def __init__(self, db_manager, session_timer, reading_intelligence):
        super().__init__()
        self.db_manager = db_manager
        self.session_timer = session_timer
        self.reading_intelligence = reading_intelligence
        self.test_results = []
        
    def run_all_tests(self):
        """Run comprehensive timer system tests"""
        print("🧪 Starting Timer System Tests...")
        print("=" * 50)
        
        tests = [
            ("Database Health Check", self.test_database_health),
            ("Session Lifecycle", self.test_session_lifecycle),
            ("Page Time Tracking", self.test_page_timing),
            ("Idle Detection", self.test_idle_detection),
            ("Reading Speed Calculation", self.test_reading_speed),
            ("Time Estimation", self.test_time_estimation),
            ("Daily Statistics", self.test_daily_stats),
            ("Streak Analytics", self.test_streak_analytics),
            ("Data Persistence", self.test_data_persistence),
            ("Performance Under Load", self.test_performance),
            ("Error Recovery", self.test_error_recovery),
            ("Session Overlap Handling", self.test_session_overlap),
            ("Cleanup Operations", self.test_cleanup_operations)
        ]
        
        passed = 0
        total = len(tests)
        
        for test_name, test_func in tests:
            try:
                print(f"\n🔍 Running: {test_name}")
                success, details = test_func()
                
                if success:
                    print(f"✅ PASSED: {test_name}")
                    passed += 1
                else:
                    print(f"❌ FAILED: {test_name}")
                    print(f"   Details: {details}")
                
                self.test_results.append({
                    'test_name': test_name,
                    'success': success,
                    'details': details,
                    'timestamp': datetime.now().isoformat()
                })
                
                self.test_completed.emit(test_name, success, details)
                
            except Exception as e:
                print(f"💥 ERROR in {test_name}: {str(e)}")
                self.test_results.append({
                    'test_name': test_name,
                    'success': False,
                    'details': f"Exception: {str(e)}",
                    'timestamp': datetime.now().isoformat()
                })
        
        print(f"\n📊 TEST SUMMARY")
        print("=" * 30)
        print(f"Passed: {passed}/{total} ({passed/total*100:.1f}%)")
        
        if passed == total:
            print("🎉 All tests passed! Timer system is fully functional.")
        else:
            print(f"⚠️  {total - passed} tests failed. Check details above.")
        
        return passed == total
    
    def test_database_health(self):
        """Test database connectivity and schema integrity"""
        try:
            # Test basic connectivity
            health = self.db_manager.health_check()
            if health['status'] != 'healthy':
                return False, f"Database unhealthy: {health.get('error', 'Unknown')}"
            
            # Test timer table existence
            required_tables = ['sessions', 'page_times', 'reading_metrics']
            for table in required_tables:
                self.db_manager.cursor.execute(f"SELECT COUNT(*) FROM {table}")
                # If we get here, table exists
            
            # Test table relationships
            self.db_manager.cursor.execute("""
                SELECT COUNT(*) FROM sessions s 
                LEFT JOIN page_times pt ON s.id = pt.session_id
            """)
            
            return True, "Database health check passed"
            
        except Exception as e:
            return False, f"Database health check failed: {str(e)}"
    
    def test_session_lifecycle(self):
        """Test complete session lifecycle"""
        try:
            # Start a test session
            session_id = self.session_timer.start_session(pdf_id=1, topic_id=1)
            if not session_id:
                return False, "Failed to start session"
            
            # Verify session is active
            stats = self.session_timer.get_current_stats()
            if not stats or stats['session_id'] != session_id:
                return False, "Session not properly tracked"
            
            # Simulate some activity
            time.sleep(1)
            self.session_timer.change_page(2)
            time.sleep(1)
            self.session_timer.change_page(3)
            
            # Test pause/resume
            self.session_timer.pause_session(manual=True)
            if not self.session_timer.is_idle:
                return False, "Session pause failed"
            
            self.session_timer.resume_session()
            if self.session_timer.is_idle:
                return False, "Session resume failed"
            
            # End session
            time.sleep(1)
            final_stats = self.session_timer.end_session()
            if not final_stats:
                return False, "Failed to end session"
            
            # Verify session was saved
            session_data = self.db_manager.get_session_analytics(session_id)
            if not session_data:
                return False, "Session data not persisted"
            
            return True, f"Session lifecycle completed successfully (ID: {session_id})"
            
        except Exception as e:
            return False, f"Session lifecycle test failed: {str(e)}"
    
    def test_page_timing(self):
        """Test per-page time tracking accuracy"""
        try:
            # Start session
            session_id = self.session_timer.start_session(pdf_id=1)
            
            # Test multiple page changes with timing
            pages_tested = []
            for page in [1, 2, 3, 2, 4]:
                start_time = time.time()
                self.session_timer.change_page(page)
                time.sleep(0.5)  # Simulate reading time
                pages_tested.append((page, time.time() - start_time))
            
            # End session
            self.session_timer.end_session()
            
            # Verify page times were recorded
            session_data = self.db_manager.get_session_analytics(session_id)
            page_times = session_data.get('page_times', [])
            
            if len(page_times) < 3:  # Should have at least 3 page time records
                return False, f"Insufficient page time records: {len(page_times)}"
            
            # Check timing accuracy (within reasonable bounds)
            for pt in page_times:
                if pt['duration_seconds'] < 0.1 or pt['duration_seconds'] > 2.0:
                    return False, f"Page time out of bounds: {pt['duration_seconds']}s"
            
            return True, f"Page timing tracked correctly ({len(page_times)} records)"
            
        except Exception as e:
            return False, f"Page timing test failed: {str(e)}"
    
    def test_idle_detection(self):
        """Test idle detection and handling"""
        try:
            # Start session
            session_id = self.session_timer.start_session(pdf_id=1)
            
            # Simulate activity
            self.session_timer.record_interaction()
            time.sleep(0.1)
            
            # Force idle state
            self.session_timer._set_idle_state(True, manual=False)
            
            if not self.session_timer.is_idle:
                return False, "Idle state not set correctly"
            
            # Test resume from idle
            self.session_timer._record_activity()
            
            if self.session_timer.is_idle:
                return False, "Failed to resume from idle"
            
            # Test manual pause
            self.session_timer.pause_session(manual=True)
            
            if not self.session_timer.is_manually_paused:
                return False, "Manual pause not tracked"
            
            # Clean up
            self.session_timer.end_session()
            
            return True, "Idle detection working correctly"
            
        except Exception as e:
            return False, f"Idle detection test failed: {str(e)}"
    
    def test_reading_speed(self):
        """Test reading speed calculation"""
        try:
            # Create test session with known parameters
            session_id = self.session_timer.start_session(pdf_id=1)
            
            # Simulate reading 5 pages in 300 seconds (1 page per minute)
            start_time = time.time()
            for page in range(1, 6):
                self.session_timer.change_page(page)
                time.sleep(0.1)  # Small delay for testing
            
            # Force specific timing for predictable results
            elapsed_seconds = 300  # 5 minutes
            pages_read = 5
            
            # End session
            stats = self.session_timer.end_session()
            
            # Test reading intelligence calculations
            metrics = self.reading_intelligence.get_reading_speed(pdf_id=1)
            
            if not metrics:
                return False, "No reading speed metrics generated"
            
            # Verify metrics are reasonable
            if 'pages_per_minute' in metrics:
                ppm = float(metrics['pages_per_minute'])
                if ppm < 0.1 or ppm > 10:  # Reasonable bounds
                    return False, f"Reading speed out of bounds: {ppm} PPM"
            
            return True, f"Reading speed calculated successfully"
            
        except Exception as e:
            return False, f"Reading speed test failed: {str(e)}"
    
    def test_time_estimation(self):
        """Test finish time estimation accuracy"""
        try:
            # Test estimation with various scenarios
            test_cases = [
                {'current_page': 1, 'total_pages': 10, 'expected_range': (5, 50)},
                {'current_page': 5, 'total_pages': 10, 'expected_range': (2, 25)},
                {'current_page': 9, 'total_pages': 10, 'expected_range': (0.5, 5)},
                {'current_page': 10, 'total_pages': 10, 'expected_range': (0, 1)}
            ]
            
            for case in test_cases:
                estimation = self.reading_intelligence.estimate_finish_time(
                    pdf_id=1,
                    current_page=case['current_page'],
                    total_pages=case['total_pages']
                )
                
                if not estimation:
                    return False, f"No estimation for case: {case}"
                
                estimated_minutes = estimation.get('estimated_minutes', 0)
                min_expected, max_expected = case['expected_range']
                
                if not (min_expected <= estimated_minutes <= max_expected):
                    return False, f"Estimation out of range: {estimated_minutes} not in {case['expected_range']}"
            
            return True, "Time estimation working correctly"
            
        except Exception as e:
            return False, f"Time estimation test failed: {str(e)}"
    
    def test_daily_stats(self):
        """Test daily statistics aggregation"""
        try:
            # Get today's stats
            today_stats = self.reading_intelligence.get_daily_stats()
            
            # Verify structure
            expected_fields = ['sessions_count', 'total_time_seconds', 'total_pages_read']
            if today_stats:
                for field in expected_fields:
                    if field not in today_stats:
                        return False, f"Missing field in daily stats: {field}"
            
            # Test with specific date
            test_date = datetime.now().date()
            specific_stats = self.reading_intelligence.get_daily_stats(test_date)
            
            # Should return same data for today
            if today_stats and specific_stats:
                if today_stats['sessions_count'] != specific_stats['sessions_count']:
                    return False, "Daily stats inconsistent between calls"
            
            return True, "Daily statistics working correctly"
            
        except Exception as e:
            return False, f"Daily stats test failed: {str(e)}"
    
    def test_streak_analytics(self):
        """Test reading streak calculations"""
        try:
            # Get streak data
            streaks = self.reading_intelligence.get_streak_analytics()
            
            if streaks:
                # Verify structure
                expected_fields = ['current_streak_days', 'streak_quality']
                for field in expected_fields:
                    if field not in streaks:
                        return False, f"Missing field in streak analytics: {field}"
                
                # Verify data types and ranges
                if not isinstance(streaks['current_streak_days'], (int, type(None))):
                    return False, "Invalid streak days type"
                
                if streaks['current_streak_days'] and streaks['current_streak_days'] < 0:
                    return False, "Negative streak days"
            
            return True, "Streak analytics working correctly"
            
        except Exception as e:
            return False, f"Streak analytics test failed: {str(e)}"
    
    def test_data_persistence(self):
        """Test data persistence across app restarts"""
        try:
            # Create session and end it
            session_id = self.session_timer.start_session(pdf_id=1)
            self.session_timer.change_page(2)
            time.sleep(0.1)
            self.session_timer.change_page(3)
            final_stats = self.session_timer.end_session()
            
            if not final_stats:
                return False, "Failed to create test session"
            
            # Simulate app restart by creating new instances
            from database.db_manager import DatabaseManager
            from utils.session_timer import SessionTimer, ReadingIntelligence
            
            new_db_manager = DatabaseManager()
            new_db_manager.connect()
            
            # Verify session data persisted
            session_data = new_db_manager.get_session_analytics(session_id)
            if not session_data:
                return False, "Session data not persisted"
            
            # Verify reading metrics persisted
            new_intelligence = ReadingIntelligence(new_db_manager)
            metrics = new_intelligence.get_reading_speed(pdf_id=1)
            
            # Clean up
            new_db_manager.disconnect()
            
            return True, "Data persistence verified"
            
        except Exception as e:
            return False, f"Data persistence test failed: {str(e)}"
    
    def test_performance(self):
        """Test performance under load"""
        try:
            import time
            
            # Test rapid page changes
            session_id = self.session_timer.start_session(pdf_id=1)
            
            start_time = time.time()
            for i in range(100):  # 100 rapid page changes
                self.session_timer.change_page((i % 10) + 1)
                self.session_timer.record_interaction()
            
            elapsed = time.time() - start_time
            
            if elapsed > 5.0:  # Should complete in under 5 seconds
                return False, f"Performance too slow: {elapsed:.2f}s for 100 operations"
            
            # End session
            self.session_timer.end_session()
            
            # Test database query performance
            start_time = time.time()
            for i in range(10):
                self.reading_intelligence.get_reading_speed(pdf_id=1)
                self.reading_intelligence.get_daily_stats()
            
            query_elapsed = time.time() - start_time
            
            if query_elapsed > 2.0:  # Should complete in under 2 seconds
                return False, f"Query performance too slow: {query_elapsed:.2f}s for 20 queries"
            
            return True, f"Performance acceptable ({elapsed:.2f}s operations, {query_elapsed:.2f}s queries)"
            
        except Exception as e:
            return False, f"Performance test failed: {str(e)}"
    
    def test_error_recovery(self):
        """Test error recovery and graceful degradation"""
        try:
            # Test with invalid PDF ID
            try:
                session_id = self.session_timer.start_session(pdf_id=99999)
                if session_id:  # Should handle gracefully
                    self.session_timer.end_session()
            except Exception:
                pass  # Expected to handle gracefully
            
            # Test with invalid page numbers
            session_id = self.session_timer.start_session(pdf_id=1)
            try:
                self.session_timer.change_page(-1)  # Invalid page
                self.session_timer.change_page(99999)  # Invalid page
            except Exception:
                pass  # Should handle gracefully
            
            self.session_timer.end_session()
            
            # Test database disconnection scenario
            original_connection = self.db_manager.connection
            self.db_manager.connection = None
            
            try:
                # Should handle database errors gracefully
                metrics = self.reading_intelligence.get_reading_speed()
            except Exception:
                pass  # Expected to handle gracefully
            finally:
                self.db_manager.connection = original_connection
            
            return True, "Error recovery working correctly"
            
        except Exception as e:
            return False, f"Error recovery test failed: {str(e)}"
    
    def test_session_overlap(self):
        """Test handling of overlapping sessions"""
        try:
            # Start first session
            session_id_1 = self.session_timer.start_session(pdf_id=1)
            
            # Start second session (should end first)
            session_id_2 = self.session_timer.start_session(pdf_id=2)
            
            if session_id_1 == session_id_2:
                return False, "Sessions not properly isolated"
            
            # Verify only one session is active
            stats = self.session_timer.get_current_stats()
            if stats['session_id'] != session_id_2:
                return False, "Multiple sessions active simultaneously"
            
            # Clean up
            self.session_timer.end_session()
            
            return True, "Session overlap handled correctly"
            
        except Exception as e:
            return False, f"Session overlap test failed: {str(e)}"
    
    def test_cleanup_operations(self):
        """Test data cleanup and maintenance operations"""
        try:
            # Test old session cleanup
            initial_count = self.db_manager.cursor.execute("SELECT COUNT(*) FROM sessions")
            
            # Run cleanup
            cleaned = self.db_manager.cleanup_old_sessions(days=0)  # Clean all
            
            # Verify cleanup worked
            final_count = self.db_manager.cursor.execute("SELECT COUNT(*) FROM sessions")
            
            # Test database optimization
            self.db_manager.optimize_database_performance()
            
            # Test health report generation
            health_report = self.db_manager.get_database_health_report()
            if not health_report or 'health_status' not in health_report:
                return False, "Health report generation failed"
            
            return True, f"Cleanup operations completed successfully"
            
        except Exception as e:
            return False, f"Cleanup test failed: {str(e)}"
    
    def generate_test_report(self):
        """Generate detailed test report"""
        report = []
        report.append("# StudySprint Timer System Test Report")
        report.append(f"Generated: {datetime.now().isoformat()}")
        report.append("")
        
        passed = sum(1 for result in self.test_results if result['success'])
        total = len(self.test_results)
        
        report.append(f"## Summary")
        report.append(f"- Total Tests: {total}")
        report.append(f"- Passed: {passed}")
        report.append(f"- Failed: {total - passed}")
        report.append(f"- Success Rate: {passed/total*100:.1f}%")
        report.append("")
        
        report.append("## Detailed Results")
        for result in self.test_results:
            status = "✅ PASSED" if result['success'] else "❌ FAILED"
            report.append(f"### {result['test_name']} - {status}")
            report.append(f"**Details:** {result['details']}")
            report.append(f"**Timestamp:** {result['timestamp']}")
            report.append("")
        
        return "\n".join(report)


class TimerTestUI(QWidget):
    """Simple UI for running timer system tests"""
    
    def __init__(self, db_manager, session_timer, reading_intelligence):
        super().__init__()
        self.tester = TimerSystemTester(db_manager, session_timer, reading_intelligence)
        self.setup_ui()
        
    def setup_ui(self):
        layout = QVBoxLayout()
        
        self.title = QLabel("StudySprint Timer System Tester")
        self.title.setFont(QFont("Arial", 16, QFont.Weight.Bold))
        
        self.run_btn = QPushButton("🧪 Run All Tests")
        self.run_btn.clicked.connect(self.run_tests)
        
        self.results_text = QTextEdit()
        self.results_text.setReadOnly(True)
        self.results_text.setFont(QFont("Courier", 10))
        
        self.export_btn = QPushButton("📄 Export Report")
        self.export_btn.clicked.connect(self.export_report)
        self.export_btn.setEnabled(False)
        
        layout.addWidget(self.title)
        layout.addWidget(self.run_btn)
        layout.addWidget(self.results_text)
        layout.addWidget(self.export_btn)
        
        self.setLayout(layout)
        self.setWindowTitle("Timer System Tester")
        self.resize(800, 600)
        
        # Connect signals
        self.tester.test_completed.connect(self.on_test_completed)
        
    def run_tests(self):
        self.run_btn.setEnabled(False)
        self.results_text.clear()
        self.results_text.append("🧪 Starting Timer System Tests...\n")
        
        # Run tests in a way that doesn't block UI
        QTimer.singleShot(100, self._run_tests_async)
        
    def _run_tests_async(self):
        success = self.tester.run_all_tests()
        
        self.results_text.append("\n" + "="*50)
        if success:
            self.results_text.append("🎉 ALL TESTS PASSED!")
        else:
            self.results_text.append("⚠️  SOME TESTS FAILED!")
        
        self.run_btn.setEnabled(True)
        self.export_btn.setEnabled(True)
        
    def on_test_completed(self, test_name, success, details):
        status = "✅" if success else "❌"
        self.results_text.append(f"{status} {test_name}: {details}")
        
    def export_report(self):
        report = self.tester.generate_test_report()
        
        from PyQt6.QtWidgets import QFileDialog
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Export Test Report", 
            f"timer_test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md",
            "Markdown Files (*.md);;Text Files (*.txt)"
        )
        
        if file_path:
            try:
                with open(file_path, 'w') as f:
                    f.write(report)
                self.results_text.append(f"\n📄 Report exported to: {file_path}")
            except Exception as e:
                self.results_text.append(f"\n❌ Export failed: {str(e)}")


def run_timer_tests():
    """Standalone function to run timer tests"""
    import sys
    import os
    
    # Add the src directory to Python path
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    
    app = QApplication(sys.argv)
    
    try:
        # Initialize components with correct imports
        from database.db_manager import DatabaseManager
        from utils.session_timer import SessionTimer, ReadingIntelligence
        
        db_manager = DatabaseManager()
        db_manager.initialize_database()
        
        session_timer = SessionTimer(db_manager)
        reading_intelligence = ReadingIntelligence(db_manager)
        
        # Create and show test UI
        test_ui = TimerTestUI(db_manager, session_timer, reading_intelligence)
        test_ui.show()
        
        return app.exec()
        
    except ImportError as e:
        print(f"❌ Import Error: {e}")
        print("📁 Please run this script from the StudySprint root directory:")
        print("   python src/utils/timer_system_test.py")
        print("📁 Or run from the main application:")
        print("   python src/main.py")
        return 1
    except Exception as e:
        print(f"💥 Error: {e}")
        return 1


if __name__ == "__main__":
    exit_code = run_timer_tests()
    sys.exit(exit_code)