from datetime import datetime
import json
import logging
import logging.config
import os
import re

from cleanco import prepare_terms, basename
import pandas as pd
import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine


DB_USER_IMI = os.environ.get('DB_USER_IMI', 'postgres')
DB_PASS_IMI = os.environ.get('DB_PASS_IMI', '')
DB_HOST_IMI = os.environ.get('DB_HOST_IMI', '127.0.0.1')
DB_NAME_IMI = os.environ.get('DB_NAME_IMI', 'imi')
DB_SCHEMA_IMI = os.environ.get('DB_SCHEMA_IMI', 'imi')
DB_PORT = os.environ.get('DB_PORT', 5432)
DB_USER_PATENTS = os.environ.get('DB_USER_PATENTS', 'postgres')
DB_PASS_PATENTS = os.environ.get('DB_PASS_PATENTS', '')
DB_HOST_PATENTS = os.environ.get('DB_HOST_PATENTS', '127.0.0.1')
DB_NAME_PATENTS = os.environ.get('DB_NAME_PATENTS', 'patentdata')
DB_SCHEMA_GRANTS = os.environ.get('DB_SCHEMA_GRANTS', 'grants')
DB_SCHEMA_APPLICATIONS = os.environ.get('DB_SCHEMA_APPLICATIONS', 'applications')
pg_config_imi = {
    'user': DB_USER_IMI,
    'password': DB_PASS_IMI,
    'host': DB_HOST_IMI,
    'port': DB_PORT,
    'dbname': DB_NAME_IMI
}
pg_config_patents = {
    'user': DB_USER_PATENTS,
    'password': DB_PASS_PATENTS,
    'host': DB_HOST_PATENTS,
    'port': DB_PORT,
    'dbname': DB_NAME_PATENTS
}
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


def save_data(dataset, data_type, source_type):
    df_dataset = pd.DataFrame()
    i = 0
    try:
        cnx = psycopg2.connect(**pg_config_patents)
        cur = cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(f"SET SEARCH_PATH = {data_type}")
        cnx.commit()
    except Exception as err:
        logger.error(err)
    for row in dataset:
        if row['name'] is None or len(row['name']) == 0:
            continue
        name = basename(row['name'], terms, prefix=False, middle=False, suffix=True)
        if data_type == 'applications':
            patentdata = find_applications(name, cnx)
        elif data_type == 'grants':
            patentdata = find_grants(name, cnx)
        if len(patentdata) > 0:
            for patent in patentdata:
                if patent['count'] > 0:
                    for gun in row['gans']:
                        df_dataset.at[i, "gans"] = gun
                        df_dataset.at[i, "participant_name"] = row['name']
                        df_dataset.at[i, "organization"] = patent['organization']
                        df_dataset.at[i, "year"] = patent['year']
                        df_dataset.at[i, "count"] = patent['count']
                        i += 1
    cnx.close()
    cols = ['gans', 'year', 'count']
    df_dataset[cols] = df_dataset[cols].astype(int)
    print(df_dataset)
    dataname = f"{source_type}_{data_type}"
    filename = f"{dataname}.csv"
    df_dataset.to_csv(filename, index=False)
    try:
        engine = create_engine(f"postgresql://{DB_USER_IMI}:{DB_PASS_IMI}@{DB_HOST_IMI}:5432/{DB_NAME_IMI}")
        df_dataset.to_sql(dataname, con=engine, schema=DB_SCHEMA_IMI, if_exists='replace', index=False)
    except Exception as err:
        logger.error(err)
        raise
    del df_dataset


def process_participants():
    q = """
        SELECT distinct("name"), array_agg(gan) as gans
        FROM imi.participants
        where "type" not in ('Patient organisations')
        and "name" not ilike '%Univers%'
        and "name" not ilike '%Univerz%'
        and "name" not ilike '%College%'
        group by "name"
        order by "name";
        """
    try:
        cnx = psycopg2.connect(**pg_config_imi)
        cur = cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(f"SET SEARCH_PATH = {DB_SCHEMA_IMI}")
        cnx.commit()
        cur = cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(q)
        result = cur.fetchall()
        cnx.commit()
    except Exception as err:
        logger.error(err)
    finally:
        cur.close()
        cnx.close()
    save_data(result, 'applications', 'participants')
    save_data(result, 'grants', 'participants')

    q = """
        SELECT distinct(trim((project_leader::text[])[2])) as name, array_agg(gan) as gans
        FROM imi.projects
        where project_leader != '{}'
        group by name
        order by name;
        """
    try:
        cnx = psycopg2.connect(**pg_config_imi)
        cur = cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(f"SET SEARCH_PATH = {DB_SCHEMA_IMI}")
        cnx.commit()
        cur = cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(q)
        result = cur.fetchall()
        cnx.commit()
    except Exception as err:
        logger.error(err)
    finally:
        cur.close()
        cnx.close()
    save_data(result, 'applications', 'leaders')
    save_data(result, 'grants', 'leaders')


def find_grants(company_name, cnx):
    company_name = f"{company_name}%"
    q = """
        SELECT ass.organization, DATE_PART('year', p.date)::INTEGER as year, count(p.id)
        FROM grants.assignee ass
        left join grants.patent_assignee pass on pass.assignee_id = ass.id
        left join grants.patent p on p.id = pass.patent_id
        where ass.organization ilike %s
        group by ass.organization, year
        order by ass.organization, year;
    """
    result = []
    try:
        cur = cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(q, (company_name,))
        result = cur.fetchall()
        cnx.commit()
    except Exception as err:
        logger.error(err)
    finally:
        cur.close()
    return result


def find_applications(company_name, cnx):
    if company_name == '':
        return []
    company_name = f"{company_name}%"
    q = """
        SELECT ass.organization, DATE_PART('year', a.date_filed)::INTEGER as year, count(a.id)
        FROM applications.assignee ass
        left join applications.application_assignee aass on aass.assignee_id = ass.id
        left join applications.application a on a.id = aass.application_id
        where ass.organization ilike %s
        group by ass.organization, year
        order by ass.organization, year;
    """
    result = []
    try:
        cur = cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(q, (company_name,))
        result = cur.fetchall()
        cnx.commit()
    except Exception as err:
        logger.error(err)
    finally:
        cur.close()
        # cnx.close()
    return result


if __name__ == '__main__':
    logger = create_logger()
    process_participants()
