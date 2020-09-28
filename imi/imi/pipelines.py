from datetime import datetime
import logging
import os

import psycopg2
import psycopg2.extras


DB_USER = os.environ.get('DB_USER', 'postgres')
DB_PASS = os.environ.get('DB_PASS', '')
DB_HOST = os.environ.get('DB_HOST', '127.0.0.1')
DB_PORT = os.environ.get('DB_PORT', 5432)
DB_NAME = os.environ.get('DB_NAME', 'postgres')
DB_SCHEMA = os.environ.get('DB_SCHEMA', 'imi')


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
        except psycopg2.Error as err:
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
            except psycopg2.Error as err:
                self.logger.error(err)
                self.cnx.rollback()
                result = None
            finally:
                cur.close()
            return True
        return False

    def process_fundings(self, item):
        for funding in item['fundings']:
            q = "INSERT INTO imi.fundings (gan, raw_text) VALUES(%s, %s) RETURNING id;"
            try:
                cur = self.cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute(q, (item['gan'], funding))
                last_row = cur.fetchone()
                self.cnx.commit()
                result = last_row['id']
            except psycopg2.Error as err:
                self.logger.error(err)
                self.cnx.rollback()
                result = None
            finally:
                cur.close()

    def process_participants(self, item):
        for participant in item['participants']:
            q = "INSERT INTO imi.participants (gan, raw_text) VALUES(%s, %s) RETURNING id;"
            try:
                cur = self.cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute(q, (item['gan'], participant))
                last_row = cur.fetchone()
                self.cnx.commit()
                result = last_row['id']
            except psycopg2.Error as err:
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
        except psycopg2.Error as err:
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
            except psycopg2.Error as err:
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
