from app import config

from os import remove
from os.path import exists, join

import mimetypes
import json


###############################################################################

def writeFile(data, file_path):
    """
    Add data to the given file.
    """
    try:
        f = open(file_path, 'a+')
        for row in data:
            f.write(row)
        f.close()
    except:
        print 'Error: could not write file: ' + file_path


def deleteFile(file_path):
    """
    Delete a given file or directory from the local file system.
    """
    try:
        if exists(file_path):
            remove(file_path)
    except:
        print 'Error: could not delete file: ' + file_path
    

def findFreeName(path, field='source'):
    """
    Look for the next free file name.
    """
    items = readAllItems()
    paths = [s[field] for s in items if field in s]
    
    i = 1
    new_path = path
    
    while(new_path in paths):
        p = path.rpartition('.')
        new_path = p[0] + str(i) + '.' + p[2]
        i = i + 1
    
    new_name = new_path.rpartition('/')[2]
    
    return {'name':new_name, 'source':new_path}


###############################################################################

def readAllItems(file_path=config.USER_SOURCE_FILE):
    """
    Open a file in read-only and load its content as JSON format.
    Note: The file should contain a list of dictionaries.
    """
    sources = []
    try:
        f = open(file_path, 'r')
        sources = json.load(f)
        f.close()
    except:
        print 'Error: could not read file: ' + file_path
        
    return sources


def createItem(item, file_path=config.USER_SOURCE_FILE):
    """
    Create new resource item and add it into the source file.
    Item is a dictionary that should at least contain the following fields:
    'name', 'type', 'format', and 'source'.
    """
    sources = readAllItems(file_path)
    
    if sources:
        # computes next free id
        free_id = max([s['id'] for s in sources]) + 1
        item['id'] = free_id
        
        writeItem(item, file_path)


def readItem(id, file_path=config.USER_SOURCE_FILE):
    """
    Return the corresponding resource (a dictionary) given its ID.
    """
    sources = []
    
    # opens file in read-only
    try:
        f = open(file_path, 'r')
        sources = json.load(f)
        f.close()
    except:
        print 'Error: could not read file: ' + file_path
    
    if sources:
        # look for the corresponding resource
        for s in sources:
            if s['id'] == id:
                return s
    
    return None


def writeItem(item, file_path=config.USER_SOURCE_FILE):
    """
    Add the new resource item to the source file.
    """
    try:
        # reads input file
        f = open(file_path, 'r+')
        sources = json.load(f)
        sources.append(item)
        
        # replaces content with new list of resources
        f.seek(0)
        f.write(json.dumps(sources))
        f.close()
    except:
        print 'Error: could not write into file: ' + file_path
    

def deleteItem(id, file_path=config.USER_SOURCE_FILE):
    """
    Remove an item from a given file using the item ID.
    """
    sources = []
    
    try:
        f = open(file_path, 'r+')
        sources = json.load(f)
        f.close()
    except:
        print 'Error: could not read file: ' + file_path
    
    if sources:
        # removes entry from source file
        new_sources = []
        for s in sources:
            if s['id'] != id:
                new_sources.append(s)
        
        try:
             # replaces content of source file
            f = open(file_path, 'w')
            f.write(json.dumps(new_sources))
            f.close()
        except:
            print 'Error: could not write into file: ' + file_path

###############################################################################

def checkFileExtension(file_name):
    """
    Check if extension of uploaded file: always use this function to secure 
    a file name before storing it into the file system,
    i.e. "never trust user input".
    """
    return ('.' in file_name) and \
           (file_name.rpartition('.')[2] in config.ALLOWED_EXTENSIONS)


def computeReadableSize(size):
    """
    Compute human readable file size given a size in bytes.
    """
    suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    base = 1024
    k = 0
    while size >= base:
        size = size/base
        k = k+1
    
    if size > 1000:
        size = 1
        k = k + 1
        
    suffix = ''
    if k < len(suffixes):
        suffix = suffixes[k]
    
    return {'measure':size, 'unit':suffix}


def computeFormat(file_name):
    """
    Compute format given a file name and extension.
    """
    format = mimetypes.guess_type(file_name)[0]

    if format is None:
        format = 'binary'
    else:
        format = format.rpartition('/')[2]
        if format == 'plain':
            format = 'text'
            
    return format

