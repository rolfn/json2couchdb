# json2couchdb
Submits the content of one or more JSON files to CouchDB.

## Syntax

```
usage: json2couchdb.py [-h] [-v] [-V] [-r PROTOCOL] [-H HOST] [-P PORT]
                       [-u USERNAME] [-p PASSWD] [-d DATABASE] [-c] [-k]
                       json_files

Submits the content of one or more JSON files to CouchDB. Filenames with
wildcards must be escaped with quotes.

positional arguments:
  json_files            file pattern

optional arguments:
  -h, --help            show this help message and exit
  -v, --verbose         Detailed outputs
  -V, --version         show program's version number and exit
  -r PROTOCOL, --protocol PROTOCOL
                        Protocol, default: http
  -H HOST, --host HOST  CouchDB host, default: 127.0.0.1 (localhost)
  -P PORT, --port PORT  CouchDB port, default=5984
  -u USERNAME, --username USERNAME
                        Username, default=no username
  -p PASSWD, --passwd PASSWD
                        Password, default=no password
  -d DATABASE, --database DATABASE
                        Database (required!)
  -c, --create_db       Create database if necessary
  -k, --keep_rev        Never suppress entry "_rev"
```
