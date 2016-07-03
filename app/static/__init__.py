from app import config
from flask import Blueprint

static = Blueprint(
    config.STATIC_FOLDER, 
    __name__, 
    static_folder=config.STATIC_FOLDER)
