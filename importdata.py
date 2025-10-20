import sqlite3
import csv
import html.parser
import multiprocessing.pool
import urllib.request

def fetch(url):
    return urllib.request.urlopen(url).read().decode()

def get_csv_urls():
    base = 'https://tess.mit.edu/public/target_lists/'
    hrefs = []

    class parse_hrefs(html.parser.HTMLParser):
        def handle_starttag(self, tag, attrs):
            attrs = dict(attrs)
            if 'href' in attrs:
                hrefs.append(attrs['href'])

    p = parse_hrefs()
    p.feed(fetch(base + 'target_lists.html'))
    return [base + href for href in hrefs if href.endswith('.csv')]

def get_csvs(poolsize=50):
    pool = multiprocessing.pool.ThreadPool(poolsize)
    for data in pool.imap(lambda url: (url, fetch(url)), get_csv_urls()):
        yield data

def createdb():
    db = sqlite3.connect('all_targets.sqlite')
    db.execute('create table if not exists all_targets(TICID integer,Camera integer,CCD integer,Tmag real,RA real,Dec real)')
    return db

def import_csv_data(db, data):
    from io import StringIO
    data = list(csv.reader(StringIO(data)))
    while data[0][0].startswith('#'):
        del data[0]
    assert data[0][0] == 'TICID'
    del data[0]
    db.executemany('insert into all_targets values(?,?,?,?,?,?)', data)
    db.commit()
    
def main():
    db = createdb()
    for url, data in get_csvs():
        import_csv_data(db, data)

if __name__ == '__main__':
    import sys
    main(*sys.argv[1:])


