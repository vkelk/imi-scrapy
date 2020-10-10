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
    df_apps = pd.DataFrame()
    i = 0
    try:
        cnx = psycopg2.connect(**pg_config_patents)
        cur = cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(f"SET SEARCH_PATH = {DB_SCHEMA_APPLICATIONS}")
        cnx.commit()
    except Exception as err:
        logger.error(err)
    for row in result:
        name = basename(row['name'], terms, prefix=False, middle=False, suffix=True)
        applications = find_applications(name, cnx)
        if len(applications) > 0:
            for application in applications:
                if application['count'] > 0:
                    df_apps.at[i, "participant_name"] = row['name']
                    df_apps.at[i, "organization"] = application['organization']
                    df_apps.at[i, "year"] = str(application['year'])
                    df_apps.at[i, "count"] = str(application['count'])
                    df_apps.at[i, "gans"] = ','.join([str(g) for g in row['gans']])
                    i +=1
    print(df_apps)
    df_apps.to_csv('applications.csv', index=False)
    del df_apps

    df_grants = pd.DataFrame()
    i = 0
    try:
        cnx = psycopg2.connect(**pg_config_patents)
        cur = cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(f"SET SEARCH_PATH = {DB_SCHEMA_GRANTS}")
        cnx.commit()
    except Exception as err:
        logger.error(err)
    for row in result:
        name = basename(row['name'], terms, prefix=False, middle=False, suffix=True)
        grants = find_grants(name, cnx)
        if len(grants) > 0:
            for grant in grants:
                if grant['count'] > 0:
                    df_grants.at[i, "participant_name"] = row['name']
                    df_grants.at[i, "organization"] = grant['organization']
                    df_grants.at[i, "year"] = str(grant['year'])
                    df_grants.at[i, "count"] = str(grant['count'])
                    df_grants.at[i, "gans"] = ','.join([str(g) for g in row['gans']])
                    i +=1
    print(df_grants)
    df_grants.to_csv('grants.csv', index=False)


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
        # cnx = psycopg2.connect(**pg_config_patents)
        # cur = cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        # cur.execute(f"SET SEARCH_PATH = {DB_SCHEMA_GRANTS}")
        # cnx.commit()
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
        # cnx = psycopg2.connect(**pg_config_patents)
        # cur = cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        # cur.execute(f"SET SEARCH_PATH = {DB_SCHEMA_APPLICATIONS}")
        # cnx.commit()
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
