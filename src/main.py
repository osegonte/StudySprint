# At the top of your existing src/main.py
import sys
import os
import logging
from PyQt6.QtWidgets import QApplication, QSplashScreen
from PyQt6.QtCore import Qt, QTimer
from PyQt6.QtGui import QPixmap, QFont
from dotenv import load_dotenv

# Load environment variables first
load_dotenv()

# Configure optimized logging
logging.basicConfig(
    level=logging.INFO if os.getenv('DEBUG', 'False').lower() == 'true' else logging.WARNING,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('studysprint.log', mode='a')
    ]
)

logger = logging.getLogger(__name__)

# Keep your existing imports
from ui.main_window import MainWindow
from database.db_manager import DatabaseManager  # Or OptimizedDatabaseManager

def optimized_main():
    """Optimized main function with enhanced error handling"""
    app = None
    main_window = None
    db_manager = None
    
    try:
        # Initialize Qt Application
        app = QApplication(sys.argv)
        app.setApplicationName(os.getenv('APP_NAME', 'StudySprint'))
        app.setApplicationVersion(os.getenv('APP_VERSION', '2.1.0'))
        app.setOrganizationName('StudySprint')
        
        # Optimize Qt settings
        app.setAttribute(Qt.ApplicationAttribute.AA_DontCreateNativeWidgetSiblings)
        app.setAttribute(Qt.ApplicationAttribute.AA_UseHighDpiPixmaps)
        
        # Initialize database with optimizations
        db_manager = DatabaseManager()  # Use your existing or OptimizedDatabaseManager
        
        # Initialize database schema
        try:
            db_manager.initialize_database()
            logger.info("✅ Database initialized successfully")
        except Exception as e:
            logger.error(f"❌ Database initialization failed: {e}")
            return 1
        
        # Create and setup main window
        main_window = MainWindow()
        main_window.db_manager = db_manager
        
        # Set up cleanup on exit
        def cleanup_on_exit():
            try:
                logger.info("🧹 Starting application cleanup...")
                
                # End any active sessions
                if hasattr(main_window, 'session_timer') and main_window.current_session_id:
                    main_window.session_timer.end_session()
                
                # Save current page
                if hasattr(main_window, 'save_current_page'):
                    main_window.save_current_page()
                
                # Clean up temp files
                if db_manager:
                    db_manager.cleanup_temp_files()
                    db_manager.disconnect()
                
                logger.info("✅ Application cleanup complete")
            except Exception as e:
                logger.error(f"❌ Error during cleanup: {e}")
        
        app.aboutToQuit.connect(cleanup_on_exit)
        
        # Show main window
        main_window.show()
        
        # Log successful startup
        logger.info("🚀 StudySprint Phase 2.1 startup complete")
        
        # Run application
        return app.exec()
        
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
        return 0
    except Exception as e:
        logger.critical(f"💥 Critical application error: {e}")
        
        # Show error dialog if possible
        try:
            from PyQt6.QtWidgets import QMessageBox
            msg_box = QMessageBox()
            msg_box.setIcon(QMessageBox.Icon.Critical)
            msg_box.setWindowTitle("Critical Error")
            msg_box.setText("A critical error occurred.")
            msg_box.setDetailedText(str(e))
            msg_box.exec()
        except:
            print(f"CRITICAL ERROR: {e}")
        
        return 1

# Replace your existing main() function
def main():
    return optimized_main()

if __name__ == '__main__':
    sys.exit(main())