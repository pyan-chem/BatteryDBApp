import sys
import logging
from PySide6.QtWidgets import QApplication
from ui.main_window import MainWindow

# Get logger for this module
logger = logging.getLogger(__name__)


def setup_logging():
    """Configure comprehensive logging for the application.
    
    Sets up logging with both console and file handlers, enabling:
    - Console output for real-time debugging
    - File output for persistent logs (battery_manager.log)
    - Timestamp, module name, and log level in all messages
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),  # Console output
            logging.FileHandler('battery_manager.log')  # File output
        ]
    )
    logger.info("Logging initialization complete")


def main():
    """Main entry point for the Battery Database Application.
    
    Initializes:
    - Logging system
    - Qt application
    - Main window
    - Application event loop
    """
    # Set up logging before any other initialization
    setup_logging()
    logger.info("=== Battery Database Application Starting ===")
    
    try:
        app = QApplication(sys.argv)
        logger.info("Qt Application created successfully")
        
        # Load stylesheet if needed
        # app.setStyleSheet(...)
        
        window = MainWindow()
        logger.info("Main window created and displayed")
        window.show()
        
        logger.info("Entering application event loop")
        sys.exit(app.exec())
    except Exception as e:
        logger.error(f"Critical error in main: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
