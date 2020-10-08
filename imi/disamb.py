from datetime import datetime
import json
import logging
import logging.config
import os
import re

from cleanco import prepare_terms, basename
import psycopg2
import psycopg2.extras


DB_USER = os.environ.get('DB_USER', 'postgres')
DB_PASS = os.environ.get('DB_PASS', '')
DB_HOST = os.environ.get('DB_HOST', '127.0.0.1')
DB_PORT = os.environ.get('DB_PORT', 5432)
DB_NAME = os.environ.get('DB_NAME', 'postgres')
DB_SCHEMA = os.environ.get('DB_SCHEMA', 'imi')
pg_config = {
    'user': DB_USER,
    'password': DB_PASS,
    'host': DB_HOST,
    'port': DB_PORT,
    'dbname': DB_NAME
}
stoplist = [r'the\b', r'\bltd\b', r'\binc\b', r'\bco\b', r'\bcorp\b', r'\bllc\b', r'\blp\b', r'\bbv\b', r'\bsrl\b',
            r'\bsa\b', r'\bab\b']
substitutions = json.load(open('nber_substitutions.json'))
stoplist = [i[1].strip().lower() for i in substitutions]
stoplist = set(stoplist)
# print(stoplist)
alphanumeric = re.compile(r'[a-zA-Z0-9& ]', re.UNICODE)
terms = prepare_terms()


def create_logger():
    now = datetime.now()
    date = now.strftime("%Y-%m-%d")
    log_file = 'disamb_' + str(date) + '.log'
    logging.config.fileConfig('log.ini', defaults={'logfilename': log_file}, disable_existing_loggers=False)
    return logging.getLogger(__name__)


def clean_name(name):
    # name = name.lower()
    name = ''.join(alphanumeric.findall(name)).strip()
    for s in substitutions:
        name = re.sub(r"\b%s\b" % s[0].strip().lower(), s[1].strip().lower(), name, flags=re.I)
    name_list = name.split(' ')
    name_list[-1] = re.sub('\\b|\\b'.join(stoplist), '', name_list[-1].lower(), flags=re.I)
    name = ' '.join(name_list)
    return name.lstrip().strip()


def process_participants():
    q = """
        SELECT distinct("name")
        FROM imi.participants
        where "type" not in ('Patient organisations')
        and "name" not ilike '%Univers%'
        and "name" not ilike '%Univerz%'
        and "name" not ilike '%College%'
        order by "name";
    """
    try:
        cnx = psycopg2.connect(**pg_config)
        cur = cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(f"SET SEARCH_PATH = {DB_SCHEMA}")
        cnx.commit()
        cur = cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(q)
        result = cur.fetchall()
        cnx.commit()
    except Exception as err:
        logger.error(err)
    finally:
        cur.close()
    if result is not None:
        for row in result:
            # print(row['name'].split(' ')[-1])
            # name = clean_name(row['name'])
            name = basename(row['name'], terms, prefix=False, middle=False, suffix=True)
            print(row['name'], "|", name)


if __name__ == '__main__':
    logger = create_logger()
    process_participants()
