#!/usr/bin/env python3

# Rolf Niepraschk, Rolf.Niepraschk@gmx.de, 2023-05-08

import os, sys, argparse, logging, json, requests
from requests.utils import quote
from requests.exceptions import HTTPError
from requests.exceptions import ConnectionError
from pathlib import Path

VERSION = '3.2.0';

DESCRIPTION = '''
Submits the content of one or more JSON files to CouchDB.
Filenames with wildcards must be escaped with quotes.
'''

# Kommandozeilen-Parser
parser = argparse.ArgumentParser(description=DESCRIPTION)
parser.add_argument(dest="json_files", type=str, \
  help='file pattern or "-" to read from standard input')
parser.add_argument('-v', '--verbose', action='store_true', \
  help='Detailed outputs')
parser.add_argument("-V", "--version", action="version", \
  version='Version: {}'.format(VERSION))
parser.add_argument('-r', '--protocol', type=str, default='http', \
  help='Protocol, default: http')
parser.add_argument('-H', '--host', type=str, default='127.0.0.1', \
  help='CouchDB host, default: 127.0.0.1 (localhost)')
parser.add_argument('-P', '--port', type=int, default=5984, \
  help='CouchDB port, default=5984')
parser.add_argument('-u', '--username', type=str, default=False, \
  help='Username, default=no username')
parser.add_argument('-p', '--passwd', type=str, default=False, \
  help='Password, default=no password')
parser.add_argument('-d', '--database', type=str, default=False, \
  help='Database (required!)')
parser.add_argument('-c', '--create_db', action='store_true', \
  help='Create database if necessary')  
parser.add_argument('-k', '--keep_rev', action='store_true', \
  help='Never suppress entry "_rev"')
  
args = parser.parse_args()

def help_exit(code = -1):
    parser.print_help()
    sys.exit(code)
    
def message(m):
    print('{}'.format(m))
  
def message_exit(m, code = -1):
    message('\n{}\n'.format(m))
    sys.exit(code)   

# Logger-Konfiguration
basename = Path(__file__).stem
if args.verbose:
    level = logging.DEBUG
    fmt = '\n{}[%(lineno)d]: %(message)s'.format(basename)
else:
    level = logging.NOTSET
    fmt = ''
logging.basicConfig(format=fmt)
logger = logging.getLogger(__name__)
logger.setLevel(level)

if not args.database:
# see the discussion here:
# https://stackoverflow.com/questions/24180527/argparse-required-arguments-listed-under-optional-arguments
    message('\nError: the argument "-d / --database" is required!\n')
    help_exit()    

logger.debug('args: {}'.format(json.dumps(vars(args), indent=2))) 
    
auth = ''
if args.username:
    u = quote(args.username)# url-encoded!
    if args.passwd:
        p = quote(args.passwd)# url-encoded!
        auth = '{}:{}@'.format(u, p)
    else:
        auth = '{}@'.format(u)

# URL for general CouchDB access
baseURL ='{}://{}{}:{}/'.format(args.protocol, auth, args.host, args.port)

try:
    response = requests.get(baseURL)# Test if CouchDB is accessible.
except ConnectionError as e:
    message_exit('Error: {}'.format(e))
    
if response.status_code == 200:
    # URL to access the specific database ('-d ...')
    baseURL ='{}://{}{}:{}/{}/'.format(args.protocol, auth, args.host, \
      args.port, args.database)
    logger.debug('baseURL: {}'.format(baseURL))
else:
    message_exit('Error: {}'.format(response.reason))

response = requests.get(baseURL) # Test if specific database is accessible.
if not response.status_code == 200: # No access
    if response.status_code == 404: # Object Not Found
        data = response.json() 
        logger.debug('{}'.format(data['reason']))
        if args.create_db == True: # Creation of the database allowed?
            logger.debug('Create "{}"'.format(args.database))
            response = requests.put(baseURL)
            logger.debug('Reason: {}'.format(response.reason))
            if not response.status_code == 201: # Created
                data = response.json()
                message_exit('Error: {}'.format(data['reason']))
        else:
            message_exit('Error: Database does not exist')

posted = 0

def getHEADER(url):
    response = requests.head(url)
    logger.debug('header: {}'.format(str(response)))
    h = False
    if response.status_code == 200: # OK
        h = response.headers
    return h
  
def getRev(url):
    rev = getHEADER(url)
    try:
        if 'ETag' in rev:
            rev = rev['ETag'].replace('"', '')
    except:
        rev = False
    return rev
    
def sendto_couchdb(d, name=False):
    global posted; global args
    if not '_id' in d:
        if name:
            basename = Path(name).stem
            d['_id'] = basename
        else:
            message_exit('Error: Missing _id entry')
    url = baseURL + quote(d['_id']);
    logger.debug('url: {}'.format(url))
    rev = getRev(url)
    logger.debug('rev: {}'.format(rev))
    if not rev == False: # Dokument existiert bereits
        d['_rev'] = rev
    else:
        if not args.keep_rev and '_rev' in d:
            del d['_rev']
    if '_attachments' in d:
        del d['_attachments']
    response = requests.put(url, json=d)
    data = response.json()
    if response.status_code == 201: # Created
        logger.debug('New revision: {}'.format(data['rev']))
        posted += 1
        message('{}. {} <-- {}'.format(posted, args.database, \
          name if name else '{...}'))
    else:
        logger.debug('Reason: {}'.format(data['reason']))

def conclusion():
    global posted; global args
    message('\n>>> {} document(s) sent to the database "{}" <<<\n' \
      .format(posted, args.database))

if args.json_files == '-': # Read content from standard input
    jstr = ''
    for line in sys.stdin:
        jstr += line
    try:    
        data = json.loads(jstr)
    except ValueError as e:
        message_exit('Error: invalid JSON')
    sendto_couchdb(data)
else: # Process real files
    message('\nSubmit file contents to the database\n')
    something_exists = False
    for fn in Path(".").glob(args.json_files):
        if os.path.isfile(fn):
            something_exists = True
            with open(fn , 'r', encoding="utf8") as f:
                jstr = f.read()
            try:
                data = json.loads(jstr)
            except ValueError as e:
                message_exit('Error: invalid JSON')
            sendto_couchdb(data, fn)
        else:
            message('File {} not found'.format(fn))
    if not something_exists:
        message('No files found!')
        help_exit()

conclusion()

sys.exit(0)

