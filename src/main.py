# src/main.py - COMPATIBLE VERSION Phase 2.1
"""
StudySprint Phase 2.1 - Compatible Main Application
This version focuses on compatibility while adding optimizations
"""

import sys
import os
import logging
from PyQt6.QtWidgets import QApplication, QMessageBox
from PyQt6.QtCore import Qt
from dotenv import load_dotenv

# Load environment variables first
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO if os.getenv('DEBUG', 'False').lower() == 'true' else logging.WARNING,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('studysprint.log', mode='a')
    ]
)

logger = logging.getLogger(__name__)

def optimized_main():
    """Compatible main function with enhanced error handling"""
    app = None
    main_window = None
    db_manager = None
    
    try:
        # Initialize Qt Application
        logger.info("🚀 Starting StudySprint Phase 2.1...")
        app = QApplication(sys.argv)
        app.setApplicationName(os.getenv('APP_NAME', 'StudySprint'))
        app.setApplicationVersion(os.getenv('APP_VERSION', '2.1.0'))
        app.setOrganizationName('StudySprint')
        
        # Try to apply optimizations, but don't fail if they don't work
        try:
            app.setAttribute(Qt.ApplicationAttribute.AA_DontCreateNativeWidgetSiblings)
        except:
            logger.debug("Could not apply AA_DontCreateNativeWidgetSiblings")
        
        # Initialize database manager
        logger.info("📊 Initializing database...")
        try:
            from database.db_manager import OptimizedDatabaseManager
            db_manager = OptimizedDatabaseManager()
            logger.info("Using OptimizedDatabaseManager")
        except ImportError:
            logger.info("OptimizedDatabaseManager not found, using DatabaseManager")
            from database.db_manager import DatabaseManager
            db_manager = DatabaseManager()
        
        # Initialize database schema
        try:
            if hasattr(db_manager, 'initialize_optimized_database'):
                db_manager.initialize_optimized_database()
            elif hasattr(db_manager, 'initialize_database'):
                db_manager.initialize_database()
            else:
                logger.warning("No database initialization method found")
            logger.info("✅ Database initialized successfully")
        except Exception as e:
            logger.error(f"❌ Database initialization failed: {e}")
            return 1
        
        # Create and setup main window
        logger.info("🖥️ Creating main window...")
        try:
            from ui.main_window import OptimizedMainWindow
            main_window = OptimizedMainWindow()
            logger.info("Using OptimizedMainWindow")
        except ImportError:
            logger.info("OptimizedMainWindow not found, using MainWindow")
            from ui.main_window import MainWindow
            main_window = MainWindow()
        
        main_window.db_manager = db_manager
        
        # Complete initialization if available
        if hasattr(main_window, 'complete_initialization'):
            try:
                main_window.complete_initialization()
                logger.info("✅ Enhanced initialization complete")
            except Exception as e:
                logger.warning(f"Enhanced initialization failed, using basic setup: {e}")
        
        # Set up cleanup on exit
        def cleanup_on_exit():
            try:
                logger.info("🧹 Starting application cleanup...")
                
                # End any active sessions
                if (hasattr(main_window, 'session_timer') and 
                    hasattr(main_window, 'current_session_id') and 
                    main_window.current_session_id):
                    main_window.session_timer.end_session()
                
                # Save current page
                if hasattr(main_window, 'save_current_page'):
                    main_window.save_current_page()
                
                # Clean up temp files
                if db_manager:
                    if hasattr(db_manager, 'cleanup_temp_files'):
                        db_manager.cleanup_temp_files()
                    if hasattr(db_manager, 'disconnect'):
                        db_manager.disconnect()
                
                logger.info("✅ Application cleanup complete")
            except Exception as e:
                logger.error(f"❌ Error during cleanup: {e}")
        
        app.aboutToQuit.connect(cleanup_on_exit)
        
        # Show main window
        main_window.show()
        
        # Log successful startup
        logger.info("🎉 StudySprint Phase 2.1 startup complete")
        
        # Run application
        return app.exec()
        
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
        return 0
    except Exception as e:
        logger.critical(f"💥 Critical application error: {e}")
        
        # Show error dialog if possible
        try:
            msg_box = QMessageBox()
            msg_box.setIcon(QMessageBox.Icon.Critical)
            msg_box.setWindowTitle("Critical Error")
            msg_box.setText("A critical error occurred.")
            msg_box.setDetailedText(str(e))
            msg_box.exec()
        except:
            print(f"CRITICAL ERROR: {e}")
        
        return 1

def main():
    """Main entry point"""
    return optimized_main()

if __name__ == '__main__':
    sys.exit(main())