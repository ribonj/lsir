from app import config
from flask import Blueprint

test =  Blueprint(config.CONTROLLER_FOLDER + '/test', __name__,)

import spark, corenlp, elastic, hbase, upload

