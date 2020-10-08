from datetime import datetime
import logging
import os
import re

import psycopg2
import psycopg2.extras


DB_USER = os.environ.get('DB_USER', 'postgres')
DB_PASS = os.environ.get('DB_PASS', '')
DB_HOST = os.environ.get('DB_HOST', '127.0.0.1')
DB_PORT = os.environ.get('DB_PORT', 5432)
DB_NAME = os.environ.get('DB_NAME', 'postgres')
DB_SCHEMA = os.environ.get('DB_SCHEMA', 'imi')


def remove_html_tags(text):
    """Remove html tags from a string"""
    clean = re.compile('<.*?>')
    text = re.sub(clean, '', text)
    text = re.sub(re.compile(r'\n'), ' ', text)
    return text.strip()


class ImiPipeline:
    def process_item(self, item, spider):
        return item


class DatabasePipeline(object):

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.pg_config = {
            'user': DB_USER,
            'password': DB_PASS,
            'host': DB_HOST,
            'port': DB_PORT,
            'dbname': DB_NAME
        }
        try:
            self.cnx = psycopg2.connect(**self.pg_config)
            cur = self.cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(f"SET SEARCH_PATH = {DB_SCHEMA}")
            self.cnx.commit()
            # self.logger.info('Connected to database')
        except psycopg2.Error as err:
            self.logger.error(err)
        finally:
            cur.close()

    def process_project(self, item):
        q = "SELECT gan FROM imi.projects WHERE gan = %s;"
        try:
            cur = self.cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(q, (item['gan'], ))
            result = cur.fetchone()
            self.cnx.commit()
        except Exception as err:
            self.logger.error(err)
        finally:
            cur.close()
        if result is None:
            q = """INSERT INTO imi.projects
                (gan, project_name, start_date, end_date, call_id, status, "program", disease_area, products, tools, imi_funding, efpia_inkind,
                other, project_intro, project_website, twitter_handle, project_coordinator, project_leader, project_manager, url, summary)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING gan;
                """
            try:
                cur = self.cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute(q, (
                    item['gan'],
                    item['project_name'],
                    datetime.strptime(item['start_date'], '%d/%m/%Y'),
                    datetime.strptime(item['end_date'], '%d/%m/%Y'),
                    item['call_id'],
                    item['status'],
                    item['program'],
                    item['disease_area'],
                    item['products'],
                    item['tools'],
                    item['imi_funding'],
                    item['efpia_inkind'],
                    item['other'],
                    item['project_intro'],
                    item['project_website'],
                    item['twitter_handle'],
                    item['project_coordinator'],
                    item['project_leader'],
                    item['project_manager'],
                    item['url'],
                    item['summary']))
                last_row = cur.fetchone()
                self.cnx.commit()
                result = last_row['gan']
            except Exception as err:
                self.logger.error(err)
                self.cnx.rollback()
                result = None
            finally:
                cur.close()
            return True
        return False

    def process_fundings(self, item):
        for funding in item['fundings']:
            funding_list = funding.split('</td>')
            funding_list = [remove_html_tags(t) for t in funding_list if 'Total Cost' not in t]
            if len(funding_list) >= 2:
                funding_list[1] = funding_list[1].replace(' ', '')
            if len(funding_list) == 3:
                funding_list.pop()
            q = 'INSERT INTO imi.fundings ("name", funding, gan, raw_text) VALUES(%s, %s, %s, %s) RETURNING id;'
            try:
                cur = self.cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute(q, (funding_list[0], funding_list[1], item['gan'], funding))
                last_row = cur.fetchone()
                self.cnx.commit()
                result = last_row['id']
            except Exception as err:
                self.logger.error(err)
                self.cnx.rollback()
                result = None
            finally:
                cur.close()

    def process_participants(self, item):
        h5_elements = []
        org_names = []
        for entry in item['participants']:
            if entry.startswith('<h5>'):
                organization = remove_html_tags(entry)
                if '(SMEs)' in organization:
                    organization = 'SME'
                elif 'public bodies' in organization:
                    organization = 'Public Sector'
                h5_elements.append(organization)
            if entry.startswith('<ul>'):
                li_elements = entry.split('</li>')
                li_elements = [remove_html_tags(t) for t in li_elements]
                li_elements.pop()
                org_names.append(li_elements)
        zip_iterator = zip(h5_elements, org_names)
        collection = (list(zip_iterator))
        for i, group in enumerate(collection):
            org_type = group[0]
            org_names = group[1]
            for name_loc in org_names:
                name_loc_list = name_loc.split(',')
                if len(name_loc_list) == 3:
                    q = 'INSERT INTO imi.participants (gan, "name", city, country, "type") VALUES(%s, %s, %s, %s, %s) RETURNING id;'
                    values = (item['gan'], name_loc_list[0].strip(), name_loc_list[1].strip(), name_loc_list[2].strip(), org_type)
                elif len(name_loc_list) == 4:
                    q = 'INSERT INTO imi.participants (gan, "name", city, region, country, "type") VALUES(%s, %s, %s, %s, %s, %s) RETURNING id;'
                    values = (item['gan'], name_loc_list[0].strip(), name_loc_list[1].strip(), name_loc_list[2].strip(), name_loc_list[3].strip(), org_type)
                try:
                    cur = self.cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                    cur.execute(q, values)
                    last_row = cur.fetchone()
                    self.cnx.commit()
                    result = last_row['id']
                except Exception as err:
                    self.logger.error(err)
                    self.cnx.rollback()
                    result = None
                finally:
                    cur.close()

    def process_call(self, item):
        q = "SELECT id FROM imi.calls WHERE call_id = %s;"
        try:
            cur = self.cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(q, (item['call_id'], ))
            result = cur.fetchone()
            self.cnx.commit()
        except Exception as err:
            self.logger.error(err)
        finally:
            cur.close()
        if result is None:
            q = """INSERT INTO imi.calls
                (call_id, action_type, proposal_submitted, topics, indicative_budget, url) 
                VALUES(%s, %s, %s, %s, %s, %s) RETURNING id;
                """
            try:
                cur = self.cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute(q, (
                    item['call_id'],
                    item['action_type'],
                    item['proposal_submitted'],
                    item['topics'],
                    item['indicative_budget'],
                    item['url']))
                last_row = cur.fetchone()
                self.cnx.commit()
                result = last_row['id']
            except Exception as err:
                self.logger.error(err)
                self.cnx.rollback()
                result = None
            finally:
                cur.close()
            return result

    def process_item(self, item, spider):
        if spider.name == 'projects':
            new_project = self.process_project(item)
            if new_project:
                if len(item['fundings']) > 0:
                    self.process_fundings(item)
                if len(item['participants']) > 0:
                    self.process_participants(item)
        if spider.name == 'calls':
            self.process_call(item)
        return item
