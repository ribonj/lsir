
from app import config
from app.controllers.test import test

from flask import request, redirect, url_for, send_from_directory, render_template
from werkzeug import secure_filename

import time

# TODO: hard-code config information.
from pywebhdfs.webhdfs import PyWebHdfsClient
hdfs = PyWebHdfsClient(host='localhost',port='50070', user_name='julien')


@test.route('/list', methods=['GET'])
def list_file():
    file_names = []
    list = hdfs.list_dir('/data/documents')
    for status in list['FileStatuses']['FileStatus']:
        file_names.append(status['pathSuffix'])
        
    
    count = 3
    names = ["doc1", "doc2", "doc3"]
    table = {"name": "Table",
             "rows": names,
             "cols": names,
             "data": [ [0]*count for i in range(count)] }
    table['data'][0][1] = 1
        
    return render_template("dashboard.html", list=file_names, table=table)


# check if extension of current file is allowed
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1] in config.ALLOWED_EXTENSIONS
           
#@test.route(TEST_URL+'/uploads/<filename>')
#def uploaded_file(filename):
#    return send_from_directory(config.UPLOAD_FOLDER, filename)


@test.route('/upload', methods=['POST'])
def upload_file():
    
    file = request.files['file']
    
    if file and allowed_file(file.filename):
        # never trust user input:
        # always use that function to secure a filename before storing it 
        # directly on the filesystem.
        # variant: filename = secure_filename(str(time.time()) + file.filename)
        filename = secure_filename(str(time.time()) + file.filename)
        path = '/data/documents/' + filename
        
        hdfs.create_file(path, file) # HDFS
        return redirect(url_for('list_file'))

