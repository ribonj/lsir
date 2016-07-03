from app import config
import os, sys

# mod_wsgi:
# Set virtual environment as current application environment
#activate_this = './venv/bin/activate_this.py'
#execfile(config.VENV_PATH, dict(__file__=config.VENV_PATH))

# Set up system path to import the application's method and Spark libraries
sys.path.append(config.APP_PATH)
sys.path.append(config.SPARK_PATH)


# Create and run the application
from app import app

# Start development server
# Parameters:
#  - host='0.0.0.0': makes the server publicly available
#  - port=5000: server port
#  - debug=True: activate debug mode
app.run(host=config.FLASK_HOST, port=config.FLASK_PORT, debug=config.DEBUG)

