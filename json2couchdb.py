#!/usr/bin/env python3

# Rolf Niepraschk, Rolf.Niepraschk@gmx.de, 2023-05-04

import os, sys, argparse, logging, json, requests
from requests.utils import quote
from requests.exceptions import HTTPError
from pathlib import Path

VERSION = '3.1.1';

DESCRIPTION = '''
Submits the content of one or more JSON files to CouchDB.
Filenames with wildcards must be escaped with quotes.
'''

# Kommandozeilen-Parser
parser = argparse.ArgumentParser(description=DESCRIPTION)
parser.add_argument(dest="json_files", type=str, help='file pattern')
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
parser.add_argument('-d', '--database', type=str, default='vl_db', \
  help='Database, default="vl_db"')
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

logger.debug('args: {}'.format(json.dumps(vars(args), indent=2))) 
    
auth = ''
if args.username:
    u = quote(args.username)# url-encoded!
    if args.passwd:
        p = quote(args.passwd)# url-encoded!
        auth = '{}:{}@'.format(u, p)
    else:
        auth = '{}@'.format(u)

# URL zum generellen CouchDB-Zugriff
baseURL ='{}://{}{}:{}/'.format(args.protocol, auth, args.host, args.port)
msg = ''
try:
    response = requests.get(baseURL)# Testen, ob CouchDB zugreifbar.
except HTTPError as e:
    msg = e.response.reason    
if not response.status_code == 200: # Wenn kein Zugriff, dann Abbruch
    message_exit('Error: {}'.format(response.reason))

# URL zum Zugriff auf die spezifische Datenbank ('-d ...')
baseURL ='{}://{}{}:{}/{}/'.format(args.protocol, auth, args.host, \
args.port, args.database)
logger.debug('baseURL: {}'.format(baseURL))

response = requests.get(baseURL) # Testen, ob DB zugreifbar.
if not response.status_code == 200: # Kein Zugriff
    if response.status_code == 404: # Object Not Found
        data = response.json() # CouchDB-Antwort
        logger.debug('{}'.format(data['reason']))
        if args.create_db == True: # Erzeugen der DB erlaubt?
            logger.debug('Create "{}"'.format(args.database))
            response = requests.put(baseURL)
            logger.debug('Reason: {}'.format(response.reason))
            if not response.status_code == 201: # Created
                data = response.json() # erzeugt Dictionary-Variable
                message_exit('Error: {}'.format(data['reason']))
                sys.exit(-1)
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
    
def sendto_couchdb(name, d):
    global posted; global args
    basename = Path(name).stem
    if not '_id' in d:
        d['_id'] = basename
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
    data = response.json() # erzeugt Dictionary-Variable
    if response.status_code == 201: # Created
        logger.debug('New revision: {}'.format(data['rev']))
        posted += 1
        message('{}. {} <-- {}'.format(posted, args.database, fn))
    else:
        logger.debug('Reason: {}'.format(data['reason']))

def conclusion():
    global posted; global args
    message('\n>>> {} documents sent to the database "{}" <<<\n' \
      .format(posted, args.database))

message('\nSubmit file contents to the database\n')
something_exists = False
for fn in Path(".").glob(args.json_files):
    if os.path.isfile(fn):
        something_exists = True
        with open(fn , 'r', encoding="utf8") as f:
            jstr = f.read()
        data = json.loads(jstr)
        sendto_couchdb(fn, data)
    else:
        message('File {} not found'.format(fn))

if something_exists == False:
    message('No files found!')
    help_exit()

conclusion()

sys.exit(0)

# TODO: als pipe ... (???)

