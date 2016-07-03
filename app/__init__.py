from flask import Flask

import config

# Creates the application with configuration, and initializes the database
app = Flask(
    __name__, 
    static_folder=config.STATIC_FOLDER, 
    template_folder=config.TEMPLATE_FOLDER)
 
# Configure application
app.debug = config.DEBUG 
app.config.from_object(config.CONFIG_FILE)


# Import Blueprints
# A Blueprint is a set of resources or operations rendering templates,
# which can be registered in a application.
from app.controllers.main import main
from app.controllers.spark import spark

from app.static import static
from app.model import model
#from app.models import Model
#from app.views import View

# Register Blueprints
app.register_blueprint(main)
app.register_blueprint(spark) #, url_prefix='/spark')

app.register_blueprint(static)
app.register_blueprint(model)
#app.register_blueprint(View)