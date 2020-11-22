import pandas as pd
import psycopg2
import sqlalchemy as db
from sqlalchemy import create_engine, Integer, String, DateTime

from src.python.utils.util import save_data_to_file
base = "../data/"

class Database:
    def __init__(self, connection):
        self.conn = connection

    def create_neighbourhood_table(self, table_name):
        cur = self.conn.cursor()
        query = f"""
        DROP TABLE {table_name};
        CREATE TABLE IF NOT EXISTS {table_name} (
        user_id integer NOT NULL,
        response_user_id integer NOT NULL,
        type character varying(50) NOT NULL,
        date timestamp without time zone NOT NULL
        )
         """
        cur.execute(query)
        self.conn.commit()
        cur.close()

    def create_user_mapping_table(self, table_name):
        cur = self.conn.cursor()
        query = f"""
        DROP TABLE {table_name};
        CREATE TABLE IF NOT EXISTS {table_name} (
        user_id integer NOT NULL,
        label character varying(50) NOT NULL,
        date timestamp without time zone NOT NULL
        )
        """
        cur.execute(query)
        self.conn.commit()
        cur.close()

    def create_counted_neighbours_table(self, table_name):
        cur = self.conn.cursor()
        query = f"""
        DROP TABLE {table_name};
        CREATE TABLE IF NOT EXISTS {table_name} (
        user_id integer NOT NULL,
        number_of_links integer NOT NULL,
        type character varying(50) NOT NULL,
        date timestamp without time zone NOT NULL
        )
        """
        cur.execute(query)
        self.conn.commit()
        cur.close()

    def create_counted_neighbours_table_new(self, table_name):
        cur = self.conn.cursor()
        query = f"""
        DROP TABLE {table_name};
        CREATE TABLE IF NOT EXISTS {table_name} (
        user_id integer NOT NULL,
        date timestamp without time zone NOT NULL,
        number_of_links_0 integer NOT NULL,
        number_of_links_1 integer NOT NULL,
        number_of_links_2 integer NOT NULL,
        number_of_links_3 integer NOT NULL,
        number_of_links_4 integer NOT NULL,
        number_of_links_5 integer NOT NULL,
        number_of_links_6 integer NOT NULL
        )
        """
        cur.execute(query)
        self.conn.commit()
        cur.close()

    def feed_neighbourhood_table(self, table_name):
        cur = self.conn.cursor()
        query = f"""
        WITH posts_neighbours AS (
            SELECT p.author_id as user_id, c.author_id as response_user_id, 'post_response' as type, c.date as date 
            FROM posts p join comments c on p.id = c.post_id 
            WHERE p.author_id <> c.author_id
        ),
        comments_neighbours AS (
        SELECT pc.author_id as user_id, c.author_id as response_user_id, 'comment_response' as type, c.date as date 
        FROM comments pc join comments c on pc.id = c.parentcomment_id 
        WHERE pc.author_id <> c.author_id
        )
        INSERT INTO {table_name}
        SELECT * FROM posts_neighbours UNION SELECT * FROM comments_neighbours
        """
        cur.execute(query)
        self.conn.commit()
        cur.close()

    def feed_user_mapping_table(self, engine, path, table_name):
        df = pd.read_csv(path)[["user_id","label","start_date"]]
        save_data_to_file("TEMP", "temp_file", df, False)
        cur = self.conn.cursor()
        f = open(f'{base}/TEMP/temp_file', 'r')
        cur.copy_from(f, table_name, sep=',')
        f.close()
        # do sprawdzenia czy dziala
        # df = pd.read_csv(path)[["user_id","label","start_date"]]
        # df.columns = ["user_id","label","date"]
        # df.to_sql(table_name, engine, if_exists='replace',index=False,
        #       dtype={"user_id": Integer(),"label": String(), "date": DateTime()})

    # odpowiedz na post w innym slocie czasowym zaliczy sie jako sasiedztwo w dla autora tego postu w tym slocie czasowym
    def feed_counted_neighbours_table(self, table_name, user_mapping_table):
        cur = self.conn.cursor()
        query = f"""
        WITH comments_labeled AS (
            SELECT c.author_id,
                   m.label as author_response_label,
                   c.parentcomment_id as parentcomment_id,
                   c.post_id as post_id,
                   c.date as comment_date,
                   m.date as mapping_date
            FROM comments c JOIN {user_mapping_table} m 
            ON c.author_id = m.user_id AND m.date <= c.date AND c.date < m.date + interval '28 day'
        ),

        posts_neighbours AS (
            SELECT DISTINCT p.author_id as user_id,
                   c.author_id as response_user_id,
                   'post_response' as type,
                   author_response_label,
                   c.mapping_date as date 
            FROM posts p join comments_labeled c on p.id = c.post_id 
            WHERE p.author_id <> c.author_id AND c.mapping_date <= c.comment_date AND c.comment_date < c.mapping_date + interval '28 day'
        ),
        comments_neighbours AS (
        SELECT DISTINCT pc.author_id as user_id,
               c.author_id as response_user_id,
               'comment_response' as type,
               author_response_label,
               c.mapping_date as date 
        FROM comments pc join comments_labeled c on pc.id = c.parentcomment_id 
        WHERE pc.author_id <> c.author_id AND c.mapping_date <= c.comment_date AND c.comment_date < c.mapping_date + interval '28 day'
        ),

        comment_mapped_table AS (
            SELECT * FROM posts_neighbours UNION ALL SELECT * FROM comments_neighbours
        )
    
        INSERT INTO {table_name}
        SELECT mt.user_id as user_id, count(*) as number_of_links, mt.author_response_label as type, mt.date AS date 
        FROM comment_mapped_table mt 
        GROUP BY mt.user_id, mt.author_response_label, mt.date
        """
        cur.execute(query)
        self.conn.commit()
        cur.close()

    def feed_counted_neighbours_table_new(self, table_name, user_mapping_table):
        cur = self.conn.cursor()
        query = f"""
    WITH comments_labeled AS (
            SELECT c.author_id,
                   m.label as author_response_label,
                   c.parentcomment_id as parentcomment_id,
                   c.post_id as post_id,
                   c.date as comment_date,
                   m.date as mapping_date
            FROM comments c JOIN {user_mapping_table} m 
            ON c.author_id = m.user_id AND m.date <= c.date AND c.date < m.date + interval '28 day'
        ),

        posts_neighbours AS (
            SELECT DISTINCT p.author_id as user_id,
                   c.author_id as response_user_id,
                   'post_response' as type,
                   author_response_label,
                   c.mapping_date as date 
            FROM posts p join comments_labeled c on p.id = c.post_id 
            WHERE p.author_id <> c.author_id AND c.mapping_date <= c.comment_date AND c.comment_date < c.mapping_date + interval '28 day'
        ),
        comments_neighbours AS (
        SELECT DISTINCT pc.author_id as user_id,
               c.author_id as response_user_id,
               'comment_response' as type,
               author_response_label,
               c.mapping_date as date 
        FROM comments pc join comments_labeled c on pc.id = c.parentcomment_id 
        WHERE pc.author_id <> c.author_id AND c.mapping_date <= c.comment_date AND c.comment_date < c.mapping_date + interval '28 day'
        ),

        comment_mapped_table AS (
            SELECT * FROM posts_neighbours UNION ALL SELECT * FROM comments_neighbours
        ),
        
        aggregated AS (
        SELECT mt.user_id as user_id, count(*) as number_of_links, mt.author_response_label as type, mt.date AS date 
        FROM comment_mapped_table mt 
        GROUP BY mt.user_id, mt.author_response_label, mt.date
        ),
        divided AS (SELECT 
         user_id,
         date,
         CASE WHEN type = '0' THEN number_of_links ELSE 0 END AS number_of_links_0,
         CASE WHEN type = '1' THEN number_of_links ELSE 0 END AS number_of_links_1,
         CASE WHEN type = '2' THEN number_of_links ELSE 0 END AS number_of_links_2,
         CASE WHEN type = '3' THEN number_of_links ELSE 0 END AS number_of_links_3,
         CASE WHEN type = '4' THEN number_of_links ELSE 0 END AS number_of_links_4,
         CASE WHEN type = '5' THEN number_of_links ELSE 0 END AS number_of_links_5,
         CASE WHEN type = '6' THEN number_of_links ELSE 0 END AS number_of_links_6
        FROM aggregated)
       
        
        INSERT INTO {table_name}
         SELECT 
            user_id,
            date,
            SUM(number_of_links_0) AS number_of_links_0,
            SUM(number_of_links_1) AS number_of_links_1,
            SUM(number_of_links_2) AS number_of_links_2,
            SUM(number_of_links_3) AS number_of_links_3,
            SUM(number_of_links_4) AS number_of_links_4,
            SUM(number_of_links_5) AS number_of_links_5,
            SUM(number_of_links_6) AS number_of_links_6
        FROM divided
        GROUP BY user_id, date
        """
        cur.execute(query)
        self.conn.commit()
        cur.close()


    def execute_query(self, query):
        with self.engine.connect() as connection:
            return connection.execute(query)

    # def execute_query_for_result(self, query, fetchone=False):
    #     cursor = self.connection.cursor()
    #     cursor.execute(query)
    #     if fetchone:
    #         result = cursor.fetchone()
    #     else:
    #         result = cursor.fetchall()
    #     cursor.close()
    #     return result


# def get_posts_table_name(self):
#     return self.config.get('tables', 'posts')
#
# def get_comments_table_name(self):
#     return self.config.get('tables', 'comments')
#
# def get_post_sentiment_table_name(self):
#     return self.config.get('tables', 'post_sentiment')
#
# def get_comment_sentiment_table_name(self):
#     return self.config.get('tables', 'comment_sentiment')
#
# def get_user_sentiment_table_name(self):
#     return self.config.get('tables', 'user_sentiment')
#
# def get_authors_table_name(self):
#     return self.config.get('tables', 'authors')
#
# def get_user_features_table_name(self):
#     return self.config.get('tables', 'user_features')


if __name__ == '__main__':
    engine = db.create_engine('postgres://postgres@localhost:54320/my_database')
    connection = engine.connect()
    metadata = db.MetaData()
    census = db.Table('authors', metadata, autoload=True, autoload_with=engine)
    print(census.columns.keys())

