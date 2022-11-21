import subprocess
import os

workDir = '/home/opengauss/openGauss'
codeBase = f'{workDir}/openGauss-server'
os.environ['PGDATA'] = f'{workDir}/gaussdata'

stmt = '''
CREATE OR REPLACE FUNCTION pg_catalog.INTERVALTONUM(INTERVAL)
RETURNS NUMERIC
AS '$libdir/plpgsql','intervaltonum'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;
'''

subprocess.run(
    [ f"{codeBase}/dest/bin/gaussdb", "--single", "--localxid", "-F", "-O", "-c", "search_path=pg_catalog", "-c", "exit_on_error=true", "-j", "postgres" ],
    input=str.encode(stmt))
    