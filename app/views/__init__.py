from app import config
from flask import Blueprint

View = Blueprint(
    config.TEMPLATE_FOLDER, 
    __name__, 
    static_folder=config.TEMPLATE_FOLDER)
