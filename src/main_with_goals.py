import sys
import os
from PyQt6.QtWidgets import QApplication
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QIcon
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from ui.main_window import MainWindow
from database.db_manager import DatabaseManager

def main():
    app = QApplication(sys.argv)
    
    # Set application properties
    app.setApplicationName(os.getenv('APP_NAME', 'StudySprint'))
    app.setApplicationVersion(os.getenv('APP_VERSION', '2.1.0'))
    app.setOrganizationName('StudySprint')
    
    # Initialize database with goals system
    db_manager = DatabaseManager()
    
    # Initialize database with goals support
    try:
        db_manager.initialize_database()
        db_manager.create_goals_tables()
        print("✅ Database initialized with goals system")
    except Exception as e:
        print(f"❌ Database initialization failed: {e}")
        return 1
    
    # Create and show main window
    window = MainWindow()
    window.show()
    
    return app.exec()

if __name__ == '__main__':
    sys.exit(main())
