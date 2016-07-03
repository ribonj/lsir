from app import config
from app.controllers.main import main, fs, net
from app.controllers.spark import default as spark

from flask import render_template, redirect, url_for, request
from werkzeug import secure_filename

import os, re


###############################################################################

@main.route('/')
def index():
    return render_template('index.html')


@main.route('/sources/')
def source():
    sources = fs.readAllItems()
    
    # sort based on type, format and name
    sources.sort(key=lambda x: x['name'].lower())
    sources.sort(key=lambda x: x['format'].lower())
    sources.sort(key=lambda x: x['type'].lower())
    
    return render_template('sources.html', 
                           sources=sources,
                           formats=config.ALLOWED_EXTENSIONS)


@main.route('/view')
def views():
    return render_template('views.html')


###############################################################################


@main.route('/sources/files/', methods=['POST'])
def upload_file():
    """
    Upload user file into the server (local) file system.
    """
    for f in request.files.getlist("file"):
        
        # secure file upload
        if f and fs.checkFileExtension(f.filename):
            file_name = secure_filename(f.filename)
            file_path = os.path.join(config.UPLOAD_FOLDER, file_name)
            file_path = os.path.abspath(file_path)
            
            # get free file name
            free = fs.findFreeName(file_path)
            file_name = free['name']
            file_path = free['source']
            
            # save file in local file system
            f.save(file_path)
            
            # create item for resource file
            item = {
                'name':file_name,
                'type': 'File',
                'format':fs.computeFormat(file_name),
                'source':file_path,
                'size':fs.computeReadableSize(os.stat(file_path).st_size),
                'count':None
            }
            fs.createItem(item)
            
    return redirect(url_for('main.source'))



@main.route('/sources/links/', methods=['POST'])
def link_online_data():
    """
    Create new link resource, 
    fetch online data using the URL and store as a file.
    """
    source = request.form['data_source']
    format = request.form['data_format']
    
    if net.checkURL(source):
        # Read URL from file and fetch online data
        data = net.fetchOnlineData(source)
        
        if data:
            name = source.rpartition('/')[2] # extracts left part of URL
            name = name.split('?',1)[0] # removes parameters (if any)
            name = name.split('.',1)[0] # removes extensions (if any)
            name = re.sub('\W','',name) # removes special characters
            name = name + '.' + format # add file extension
            
            path = os.path.join(config.USER_FOLDER, name)
            
            free = fs.findFreeName(path, 'path')
            name = free['name']
            path = free['source']
            
            # Create a temporary file to hold online data before loading them
            # in memory using a DataFrame
            fs.writeFile(data, path)
            size = fs.computeReadableSize(os.stat(path).st_size)
            
            # create item for resource file
            item = {
                'name':name,
                'type':'Online Data',
                'format':format,
                'source':source,
                'size':size,
                'count':None,
                # ------------------------
                'path':path
            }
            fs.createItem(item)
    else:
        print 'Warning: not a valid URL: ' + source
    
    return redirect(url_for('main.source'))


@main.route('/sources/databases/', methods=['POST'])
def connect_database():
    dbms = request.form['dbms']
    database = request.form['database']
    username = request.form['username']
    password = request.form['password']
    host = request.form['host']
    port = request.form['port']
    table = request.form['table']
    
    if dbms in config.ALLOWED_DATABASES:
        source = "jdbc:"+dbms.lower()+"://"+host+":"+port+"/"+database+"/"+table
        name = re.sub('\W','',table)
        
        item = {
            'name': name,
            'source': source,
            'type': 'Database',
            'format': 'table',
            'size': {'measure':'-', 'unit':''},
            'count': None,
            # ------------------------
            'dbms':dbms,
            'host':host,
            'port':port,
            'database':database,
            'username':username,
            'password':password,
            'table':table
        }
        fs.createItem(item)
    
    return redirect(url_for('main.source'))


###############################################################################

@main.route('/sources/<id>/_delete')
def delete(id):
    """
    Remove a user resource: remove entry item in source file.
    If the resource is a local file, the file is deleted as well.
    """
    id = int(id)
    item = fs.readItem(id)
    
    # deletes the local file
    if item['type'] == 'File':
        fs.deleteFile(os.path.join(config.UPLOAD_FOLDER, item['name']))
    elif item['type'] == 'Online Data':
        fs.deleteFile(os.path.join(config.USER_FOLDER, item['name']))
        
    # removes item
    fs.deleteItem(id)
    
    return redirect(url_for('main.source'))


@main.route('/sources/<id>/_load')
def load(id):
    """
    Load resource in memory as a new DataFrame.
    """
    id = int(id)
    item = fs.readItem(id)
    
    # creates DataFrame based on a resource item
    # and load as new DataGrid.
    success = spark.load(item)
    
    if success:
        return redirect(url_for('spark.display'))
    else:
        return redirect(url_for('main.source'))


@main.route('/sources/<id>/_join')
def join(id):
    """
    Load the file in memory as a new DataFrame and merge it with data
    present in the DataGrid using a common sub-schema (i.e. shared columns).
    This is equivalent to load(id) if the DataGrid is empty.
    """
    id = int(id)
    item = fs.readItem(id)
    
    # creates DataFrame based on a resource item
    # and merge with existing DataGrid.
    success = spark.join(item)
    
    if success:
        return redirect(url_for('spark.display'))
    else:
        return redirect(url_for('main.source'))


@main.route('/sources/<id>/_send_to_hdfs')
def send_to_hdfs(id):
    """
    Load the given local file in memory as a new DataFrame 
    and store it back to HDFS.
    """
    id = int(id)
    item = fs.readItem(id)
    
    # create a hard copy
    new_item = dict(item) 
    new_name = item['name'].split('.',1)[0] + '.parquet'
    new_path = os.path.join(config.HDFS_DOCS_URL, new_name)
    
    free = fs.findFreeName(new_path)
    
    # modifies item properties to match HDFS file description
    new_item['name'] = free['name']
    new_item['type'] = 'File (HDFS)'
    new_item['format'] = 'binary'
    new_item['source'] = free['source']
    del new_item['id']
    
    if 'path' in new_item:
        del new_item['path']
    
    # loads resource to HDFS
    success = spark.send_to_hdfs(item, new_item)
    
    if success:
        fs.createItem(new_item)
        
    return redirect(url_for('main.source'))

