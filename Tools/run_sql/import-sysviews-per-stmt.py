import csv
import os
import subprocess

workDir = '/home/opengauss/openGauss'
codeBase = f'{workDir}/openGauss-server'
os.environ['PGDATA'] = f'{workDir}/gaussdata'

# log to csv file
with open('sysviews-results.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile, dialect='excel')
    writer.writerow(['line number', 'return code', 'stderr', 'stdout'])

    stmt = ''
    # input from sql file
    with open(f'{codeBase}/src/common/backend/catalog/system_views.sql') as f:
    # with open(f'tmp.sql') as f:
        for lnum, line in enumerate(f):
            if lnum % 100 == 0:
                print(lnum, end='...', flush=True)
            stmt += line

            # end of statement
            if line == '\n' or line == '':
                r = subprocess.run(
                    [ f"{codeBase}/dest/bin/gaussdb", "--single", "--localxid", "-F", "-O", "-c", "search_path=pg_catalog", "-c", "exit_on_error=true", "-j", "template1" ],
                     input=str.encode(stmt), stdout=subprocess.PIPE, stderr=subprocess.PIPE)

                # clean up messages
                err = r.stderr.strip()
                out = r.stdout.strip()

                # log output
                writer.writerow([lnum, r.returncode, err, out])
                # print(str(err))

                # next statement
                stmt = ''
