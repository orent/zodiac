import pyvo

svc = pyvo.dal.TAPService("https://mast.stsci.edu/vo-tap/api/v0.1/caom/")

def getvalue(x):
    dtype = getattr(x, 'dtype', None)
    if dtype and dtype.kind == 'i':
        return int(x)
    elif dtype and dtype.kind == 'f':
        return float(x)
    else:
        return x
    
def row2dict(row):
    return {key: getvalue(value) for key, value in row.items()}

adql = '''
    SELECT TOP {batch}
        objID,
        obs_id,
        access_url,
        access_estsize,
        target_name,
        s_ra,
        s_dec,
        t_min,
        t_max,
        t_exptime
    FROM obscore
    WHERE obs_collection = 'TESS'
    AND access_url LIKE '%_lc.fits'
    AND objID > {start}
    ORDER BY objID asc
'''

schema = '''
    create table if not exists lightcurves(
        objID integer primary key,
        obs_id text,
        access_url text,
        access_estsize integer,
        target_name text,
        s_ra real,
        s_dec real,
        t_min real,
        t_max real,
        t_exptime integer
    )
'''

def query(adql=adql, start=0, batch=100):
    while True:
        for row in svc.search(adql.format(start=start, batch=batch)):
    return tbl
