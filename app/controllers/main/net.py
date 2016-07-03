
from urllib2 import Request, urlopen, URLError, HTTPError
from urlparse import urlparse
import httplib


def fetchOnlineData(url):
    """
    Load online data from the cloud.
    """
    req = Request(url)
    res = None
    
    try:
        res = urlopen(req)
    except HTTPError as err:
        print 'The server could not fulfill the request.'
        print 'Error code: ', err.code
    except URLError as err:
        print 'We failed to reach a server.'
        print 'Reason: ', err.reason
    
    return res


def checkURL(url):
    """
    Check that the given URL is valid.
    """
    parseResult = urlparse(url)
    conn = httplib.HTTPConnection(parseResult.netloc)
    
    try:
        conn.request('HEAD', url) #parseResult.path
    except:
        print 'Warning: Could not verify URL: ' + url
        return False
    
    resp = conn.getresponse()
    
    return resp.status < 400 # True if no HTTP error returned
