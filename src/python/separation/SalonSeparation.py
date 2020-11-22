from src.python.database.Database import Database
from src.python.database.DbUtils import connection_SALON
from src.python.utils.util import save_data_to_file, save_data_to_file_with_index
import matplotlib.pyplot as plt
import pandas as pd

base = "../data/"


def prepare_separation_final_table(db):
    conn = db.conn
    cur = conn.cursor()
    query = f""" 
    DROP TABLE SeparationFinal;

    CREATE TABLE SeparationFinal(
    claster varchar(50),
    number_of_posts decimal,
    number_of_comments decimal,
    frequency_of_posts_stddev decimal,
    frequency_of_comments_q3 decimal,
    frequency_of_comments_stddev decimal,
    number_of_received_responses_to_post_max decimal,
    number_of_received_responses_to_post_q3 decimal,
    number_of_received_responses_under_comments_q3 decimal,
    number_of_received_responses_under_comments_max decimal,
    number_of_own_post_responses_q3 decimal,
    number_of_words_in_posts_q3 decimal,
    number_of_words_in_comments_q3 decimal,
    number_of_words_in_responses_to_posts_q3 decimal,
    number_of_words_in_own_post_responses_q3 decimal
    );
        """
    cur.execute(query)
    conn.commit()
    cur.close()




def prepare_separation_table(db):
    conn = db.conn
    cur = conn.cursor()
    query = f""" 
    DROP TABLE Separation;

    CREATE TABLE Separation(
    user_id int,
    date date,
    number_of_posts decimal,
    number_of_comments decimal,
    frequency_of_posts_stddev decimal,
    frequency_of_comments_q3 decimal,
    frequency_of_comments_stddev decimal,
    number_of_received_responses_to_post_max decimal,
    number_of_received_responses_to_post_q3 decimal,
    number_of_received_responses_under_comments_q3 decimal,
    number_of_received_responses_under_comments_max decimal,
    number_of_own_post_responses_q3 decimal,
    number_of_words_in_posts_q3 decimal,
    number_of_words_in_comments_q3 decimal,
    number_of_words_in_responses_to_posts_q3 decimal,
    number_of_words_in_own_post_responses_q3 decimal
    );
        """
    cur.execute(query)
    conn.commit()
    cur.close()


def get_cluster_different_roles_statistics(db, all_data, user_mapping, cluster, featur_cluster):
    conn = db.conn
    cur = conn.cursor()
    query = f"""
    DROP TABLE SeparationRange;

    CREATE TABLE SeparationRange(
    col_min_number_of_posts decimal,
    col_max_number_of_posts decimal,
    col_min_number_of_comments decimal,
    col_max_number_of_comments decimal,
    col_min_frequency_of_posts_stddev decimal,
    col_max_frequency_of_posts_stddev decimal,
    col_min_frequency_of_comments_q3 decimal,
    col_max_frequency_of_comments_q3 decimal,
    col_min_frequency_of_comments_stddev decimal,
    col_max_frequency_of_comments_stddev decimal,
    col_min_number_of_received_responses_to_post_max decimal,
    col_max_number_of_received_responses_to_post_max decimal,
    col_min_number_of_received_responses_to_post_q3 decimal,
    col_max_number_of_received_responses_to_post_q3 decimal,
    col_min_number_of_received_responses_under_comments_q3 decimal,
    col_max_number_of_received_responses_under_comments_q3 decimal,
    col_min_number_of_received_responses_under_comments_max decimal,
    col_max_number_of_received_responses_under_comments_max decimal,
    col_min_number_of_own_post_responses_q3 decimal,
    col_max_number_of_own_post_responses_q3 decimal,
    col_min_number_of_words_in_posts_q3 decimal,
    col_max_number_of_words_in_posts_q3 decimal,
    col_min_number_of_words_in_comments_q3 decimal,
    col_max_number_of_words_in_comments_q3 decimal,
    col_min_number_of_words_in_responses_to_posts_q3 decimal,
    col_max_number_of_words_in_responses_to_posts_q3 decimal,
    col_min_number_of_words_in_own_post_responses_q3 decimal,
    col_max_number_of_words_in_own_post_responses_q3 decimal
    );

    do $$
    declare

    declare min_number_of_posts decimal;
    declare min_number_of_comments decimal;
    declare min_frequency_of_posts_stddev decimal;
    declare min_frequency_of_comments_q3 decimal;
    declare min_frequency_of_comments_stddev decimal;
    declare min_number_of_received_responses_to_post_max decimal;
    declare min_number_of_received_responses_to_post_q3 decimal;
    declare min_number_of_received_responses_under_comments_q3 decimal;
    declare min_number_of_received_responses_under_comments_max decimal;
    declare min_number_of_own_post_responses_q3 decimal;
    declare min_number_of_words_in_posts_q3 decimal;
    declare min_number_of_words_in_comments_q3 decimal;
    declare min_number_of_words_in_responses_to_posts_q3 decimal;
    declare min_number_of_words_in_own_post_responses_q3 decimal;

    declare max_number_of_posts decimal;
    declare max_number_of_comments decimal;
    declare max_frequency_of_posts_stddev decimal;
    declare max_frequency_of_comments_q3 decimal;
    declare max_frequency_of_comments_stddev decimal;
    declare max_number_of_received_responses_to_post_max decimal;
    declare max_number_of_received_responses_to_post_q3 decimal;
    declare max_number_of_received_responses_under_comments_q3 decimal;
    declare max_number_of_received_responses_under_comments_max decimal;
    declare max_number_of_own_post_responses_q3 decimal;
    declare max_number_of_words_in_posts_q3 decimal;
    declare max_number_of_words_in_comments_q3 decimal;
    declare max_number_of_words_in_responses_to_posts_q3 decimal;
    declare max_number_of_words_in_own_post_responses_q3 decimal;

    begin

    INSERT INTO SeparationRange 
    WITH enriched AS (
        SELECT a.user_id AS user_id, 
           a.date AS date,
           u.label AS label,
               number_of_posts,
    number_of_comments,
    frequency_of_posts_stddev,
    frequency_of_comments_q3,
    frequency_of_comments_stddev,
    number_of_received_responses_to_post_max,
    number_of_received_responses_to_post_q3,
    number_of_received_responses_under_comments_q3,
    number_of_received_responses_under_comments_max,
    number_of_own_post_responses_q3,
    number_of_words_in_posts_q3,
    number_of_words_in_comments_q3,
    number_of_words_in_responses_to_posts_q3,
    number_of_words_in_own_post_responses_q3
    FROM {all_data} a
    JOIN {user_mapping} u ON a.user_id = u.user_id AND a.date = u.date
    )
    SELECT 

                min(number_of_posts),
                max(number_of_posts),
                min(number_of_comments),
                max(number_of_comments),
                min(frequency_of_posts_stddev),
                max(frequency_of_posts_stddev),
                min(frequency_of_comments_q3),
                max(frequency_of_comments_q3),
                min(frequency_of_comments_stddev),
                max(frequency_of_comments_stddev),
                min(number_of_received_responses_to_post_max),
                max(number_of_received_responses_to_post_max),
                min(number_of_received_responses_to_post_q3),
                max(number_of_received_responses_to_post_q3),
                min(number_of_received_responses_under_comments_q3),
                max(number_of_received_responses_under_comments_q3),
                min(number_of_received_responses_under_comments_max),
                max(number_of_received_responses_under_comments_max),
                min(number_of_own_post_responses_q3),
                max(number_of_own_post_responses_q3),
                min(number_of_words_in_posts_q3),
                max(number_of_words_in_posts_q3),
                min(number_of_words_in_comments_q3),
                max(number_of_words_in_comments_q3),
                min(number_of_words_in_responses_to_posts_q3),
                max(number_of_words_in_responses_to_posts_q3),
                min(number_of_words_in_own_post_responses_q3),
                max(number_of_words_in_own_post_responses_q3)
        
    FROM enriched
    WHERE label = '{featur_cluster}';

    min_number_of_posts := (SELECT col_min_number_of_posts FROM SeparationRange);
    min_number_of_comments := (SELECT col_min_number_of_comments FROM SeparationRange);
    min_frequency_of_posts_stddev := (SELECT col_min_frequency_of_posts_stddev FROM SeparationRange);
    min_frequency_of_comments_q3 := (SELECT col_min_frequency_of_comments_q3 FROM SeparationRange);
    min_frequency_of_comments_stddev := (SELECT col_min_frequency_of_comments_stddev FROM SeparationRange);
    min_number_of_received_responses_to_post_max := (SELECT col_min_number_of_received_responses_to_post_max FROM SeparationRange);
    min_number_of_received_responses_to_post_q3 := (SELECT col_min_number_of_received_responses_to_post_q3 FROM SeparationRange);
    min_number_of_received_responses_under_comments_q3 := (SELECT col_min_number_of_received_responses_under_comments_q3 FROM SeparationRange);
    min_number_of_received_responses_under_comments_max := (SELECT col_min_number_of_received_responses_under_comments_max FROM SeparationRange);
    min_number_of_own_post_responses_q3 := (SELECT col_min_number_of_own_post_responses_q3 FROM SeparationRange);
    min_number_of_words_in_posts_q3 := (SELECT col_min_number_of_words_in_posts_q3 FROM SeparationRange);
    min_number_of_words_in_comments_q3 := (SELECT col_min_number_of_words_in_comments_q3 FROM SeparationRange);
    min_number_of_words_in_responses_to_posts_q3 := (SELECT col_min_number_of_words_in_responses_to_posts_q3 FROM SeparationRange);
    min_number_of_words_in_own_post_responses_q3 := (SELECT col_min_number_of_words_in_own_post_responses_q3 FROM SeparationRange);

    max_number_of_posts := (SELECT col_max_number_of_posts FROM SeparationRange);
    max_number_of_comments := (SELECT col_max_number_of_comments FROM SeparationRange);
    max_frequency_of_posts_stddev := (SELECT col_max_frequency_of_posts_stddev FROM SeparationRange);
    max_frequency_of_comments_q3 := (SELECT col_max_frequency_of_comments_q3 FROM SeparationRange);
    max_frequency_of_comments_stddev := (SELECT col_max_frequency_of_comments_stddev FROM SeparationRange);
    max_number_of_received_responses_to_post_max := (SELECT col_max_number_of_received_responses_to_post_max FROM SeparationRange);
    max_number_of_received_responses_to_post_q3 := (SELECT col_max_number_of_received_responses_to_post_q3 FROM SeparationRange);
    max_number_of_received_responses_under_comments_q3 := (SELECT col_max_number_of_received_responses_under_comments_q3 FROM SeparationRange);
    max_number_of_received_responses_under_comments_max := (SELECT col_max_number_of_received_responses_under_comments_max FROM SeparationRange);
    max_number_of_own_post_responses_q3 := (SELECT col_max_number_of_own_post_responses_q3 FROM SeparationRange);
    max_number_of_words_in_posts_q3 := (SELECT col_max_number_of_words_in_posts_q3 FROM SeparationRange);
    max_number_of_words_in_comments_q3 := (SELECT col_max_number_of_words_in_comments_q3 FROM SeparationRange);
    max_number_of_words_in_responses_to_posts_q3 := (SELECT col_max_number_of_words_in_responses_to_posts_q3 FROM SeparationRange);
    max_number_of_words_in_own_post_responses_q3 := (SELECT col_max_number_of_words_in_own_post_responses_q3 FROM SeparationRange);

    INSERT INTO Separation
    WITH enriched AS (
        SELECT a.user_id AS user_id, 
           a.date AS date,
           u.label AS label,
            number_of_posts,
            number_of_comments,
            frequency_of_posts_stddev,
            frequency_of_comments_q3,
            frequency_of_comments_stddev,
            number_of_received_responses_to_post_max,
            number_of_received_responses_to_post_q3,
            number_of_received_responses_under_comments_q3,
            number_of_received_responses_under_comments_max,
            number_of_own_post_responses_q3,
            number_of_words_in_posts_q3,
            number_of_words_in_comments_q3,
            number_of_words_in_responses_to_posts_q3,
            number_of_words_in_own_post_responses_q3
    FROM all_hp_data a
    JOIN user_mapping u ON a.user_id = u.user_id AND a.date = u.date
    WHERE u.label = '{cluster}'
    )
    SELECT
    user_id,
    date,
            CASE WHEN min_number_of_posts < number_of_posts AND number_of_posts < max_number_of_posts THEN 1 ELSE 0 END AS number_of_posts, 
            CASE WHEN min_number_of_comments < number_of_comments AND number_of_comments < max_number_of_comments THEN 1 ELSE 0 END AS number_of_comments, 
            CASE WHEN min_frequency_of_posts_stddev < frequency_of_posts_stddev AND frequency_of_posts_stddev < max_frequency_of_posts_stddev THEN 1 ELSE 0 END AS frequency_of_posts_stddev, 
            CASE WHEN min_frequency_of_comments_q3 < frequency_of_comments_q3 AND frequency_of_comments_q3 < max_frequency_of_comments_q3 THEN 1 ELSE 0 END AS frequency_of_comments_q3, 
            CASE WHEN min_frequency_of_comments_stddev < frequency_of_comments_stddev AND frequency_of_comments_stddev < max_frequency_of_comments_stddev THEN 1 ELSE 0 END AS frequency_of_comments_stddev, 
            CASE WHEN min_number_of_received_responses_to_post_max < number_of_received_responses_to_post_max AND number_of_received_responses_to_post_max < max_number_of_received_responses_to_post_max THEN 1 ELSE 0 END AS number_of_received_responses_to_post_max, 
            CASE WHEN min_number_of_received_responses_to_post_q3 < number_of_received_responses_to_post_q3 AND number_of_received_responses_to_post_q3 < max_number_of_received_responses_to_post_q3 THEN 1 ELSE 0 END AS number_of_received_responses_to_post_q3, 
            CASE WHEN min_number_of_received_responses_under_comments_q3 < number_of_received_responses_under_comments_q3 AND number_of_received_responses_under_comments_q3 < max_number_of_received_responses_under_comments_q3 THEN 1 ELSE 0 END AS number_of_received_responses_under_comments_q3, 
            CASE WHEN min_number_of_received_responses_under_comments_max < number_of_received_responses_under_comments_max AND number_of_received_responses_under_comments_max < max_number_of_received_responses_under_comments_max THEN 1 ELSE 0 END AS number_of_received_responses_under_comments_max, 
            CASE WHEN min_number_of_own_post_responses_q3 < number_of_own_post_responses_q3 AND number_of_own_post_responses_q3 < max_number_of_own_post_responses_q3 THEN 1 ELSE 0 END AS number_of_own_post_responses_q3, 
            CASE WHEN min_number_of_words_in_posts_q3 < number_of_words_in_posts_q3 AND number_of_words_in_posts_q3 < max_number_of_words_in_posts_q3 THEN 1 ELSE 0 END AS number_of_words_in_posts_q3, 
            CASE WHEN min_number_of_words_in_comments_q3 < number_of_words_in_comments_q3 AND number_of_words_in_comments_q3 < max_number_of_words_in_comments_q3 THEN 1 ELSE 0 END AS number_of_words_in_comments_q3, 
            CASE WHEN min_number_of_words_in_responses_to_posts_q3 < number_of_words_in_responses_to_posts_q3 AND number_of_words_in_responses_to_posts_q3 < max_number_of_words_in_responses_to_posts_q3 THEN 1 ELSE 0 END AS number_of_words_in_responses_to_posts_q3, 
            CASE WHEN min_number_of_words_in_own_post_responses_q3 < number_of_words_in_own_post_responses_q3 AND number_of_words_in_own_post_responses_q3 < max_number_of_words_in_own_post_responses_q3 THEN 1 ELSE 0 END AS number_of_words_in_own_post_responses_q3 
    FROM enriched;
    
    end$$;
    """
    cur.execute(query)
    conn.commit()
    cur.close()


def fill_separation_final_table(db, cluster):
    conn = db.conn
    cur = conn.cursor()
    query = f"""
    INSERT INTO SeparationFinal
    WITH counted AS (
    SELECT 
    SUM(number_of_posts) AS number_of_posts,
    SUM(number_of_comments) AS number_of_comments,
    SUM(frequency_of_posts_stddev) AS frequency_of_posts_stddev,
    SUM(frequency_of_comments_q3) AS frequency_of_comments_q3,
    SUM(frequency_of_comments_stddev) AS frequency_of_comments_stddev,
    SUM(number_of_received_responses_to_post_max) AS number_of_received_responses_to_post_max,
    SUM(number_of_received_responses_to_post_q3) AS number_of_received_responses_to_post_q3,
    SUM(number_of_received_responses_under_comments_q3) AS number_of_received_responses_under_comments_q3,
    SUM(number_of_received_responses_under_comments_max) AS number_of_received_responses_under_comments_max,
    SUM(number_of_own_post_responses_q3) AS number_of_own_post_responses_q3,
    SUM(number_of_words_in_posts_q3) AS number_of_words_in_posts_q3,
    SUM(number_of_words_in_comments_q3) AS number_of_words_in_comments_q3,
    SUM(number_of_words_in_responses_to_posts_q3) AS number_of_words_in_responses_to_posts_q3,
    SUM(number_of_words_in_own_post_responses_q3) AS number_of_words_in_own_post_responses_q3
    FROM separation
    GROUP BY user_id, date
    ) 
    SELECT 
    {cluster},
    AVG(number_of_posts) AS number_of_posts,
    AVG(number_of_comments) AS number_of_comments,
    AVG(frequency_of_posts_stddev) AS frequency_of_posts_stddev,
    AVG(frequency_of_comments_q3) AS frequency_of_comments_q3,
    AVG(frequency_of_comments_stddev) AS frequency_of_comments_stddev,
    AVG(number_of_received_responses_to_post_max) AS number_of_received_responses_to_post_max,
    AVG(number_of_received_responses_to_post_q3) AS number_of_received_responses_to_post_q3,
    AVG(number_of_received_responses_under_comments_q3) AS number_of_received_responses_under_comments_q3,
    AVG(number_of_received_responses_under_comments_max) AS number_of_received_responses_under_comments_max,
    AVG(number_of_own_post_responses_q3) AS number_of_own_post_responses_q3,
    AVG(number_of_words_in_posts_q3) AS number_of_words_in_posts_q3,
    AVG(number_of_words_in_comments_q3) AS number_of_words_in_comments_q3,
    AVG(number_of_words_in_responses_to_posts_q3) AS number_of_words_in_responses_to_posts_q3,
    AVG(number_of_words_in_own_post_responses_q3) AS number_of_words_in_own_post_responses_q3
    FROM counted;
        """
    cur.execute(query)
    conn.commit()
    cur.close()


def get_data(db):
    conn = db.conn
    cur = conn.cursor()
    query = f"""
        SELECT * FROM separationfinal;
    """
    cur.execute(query)
    res = cur.fetchall()
    conn.commit()
    cur.close()
    data = []
    data.extend(res)
    df = pd.DataFrame(data, columns=['cluster',
                                     'number_of_posts',
                                     'number_of_comments',
                                     'frequency_of_posts_stddev',
                                     'frequency_of_comments_q3',
                                     'frequency_of_comments_stddev',
                                     'number_of_received_responses_to_post_max',
                                     'number_of_received_responses_to_post_q3',
                                     'number_of_received_responses_under_comments_q3',
                                     'number_of_received_responses_under_comments_max',
                                     'number_of_own_post_responses_q3',
                                     'number_of_words_in_posts_q3',
                                     'number_of_words_in_comments_q3',
                                     'number_of_words_in_responses_to_posts_q3',
                                     'number_of_words_in_own_post_responses_q3'])

    save_data_to_file_with_index("Salon_separation", "separation", df.T)


def salon_separation():
    db = Database(connection_SALON)
    prepare_separation_final_table(db)

    prepare_separation_table(db)
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='0',
                                           featur_cluster='1')
    print("1")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='0',
                                           featur_cluster='2')
    print("2")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='0',
                                           featur_cluster='3')
    print("3")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='0',
                                           featur_cluster='4')
    print("4")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='0',
                                           featur_cluster='5')
    print("5")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='0',
                                           featur_cluster='6')
    print("6")
    fill_separation_final_table(db, "0")
    print("done")

    prepare_separation_table(db)
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='1',
                                           featur_cluster='0')
    print("1")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='1',
                                           featur_cluster='2')
    print("2")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='1',
                                           featur_cluster='3')
    print("3")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='1',
                                           featur_cluster='4')
    print("4")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='1',
                                           featur_cluster='5')
    print("5")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='1',
                                           featur_cluster='6')
    print("6")
    fill_separation_final_table(db, "1")
    print("done")

    prepare_separation_table(db)
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='2',
                                           featur_cluster='0')
    print("1")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='2',
                                           featur_cluster='1')
    print("2")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='2',
                                           featur_cluster='3')
    print("3")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='2',
                                           featur_cluster='4')
    print("4")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='2',
                                           featur_cluster='5')
    print("5")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='2',
                                           featur_cluster='6')
    print("6")
    fill_separation_final_table(db, "2")
    print("done")

    prepare_separation_table(db)
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='3',
                                           featur_cluster='0')
    print("1")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='3',
                                           featur_cluster='1')
    print("2")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='3',
                                           featur_cluster='2')
    print("3")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='3',
                                           featur_cluster='4')
    print("4")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='3',
                                           featur_cluster='5')
    print("5")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='3',
                                           featur_cluster='6')
    print("6")
    fill_separation_final_table(db, "3")
    print("done")

    prepare_separation_table(db)
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='4',
                                           featur_cluster='0')
    print("1")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='4',
                                           featur_cluster='1')
    print("2")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='4',
                                           featur_cluster='2')
    print("3")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='4',
                                           featur_cluster='3')
    print("4")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='4',
                                           featur_cluster='5')
    print("5")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='4',
                                           featur_cluster='6')
    print("6")
    fill_separation_final_table(db, "4")
    print("done")

    prepare_separation_table(db)
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='5',
                                           featur_cluster='0')
    print("1")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='5',
                                           featur_cluster='1')
    print("2")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='5',
                                           featur_cluster='2')
    print("3")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='5',
                                           featur_cluster='3')
    print("4")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='5',
                                           featur_cluster='4')
    print("5")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='5',
                                           featur_cluster='6')
    print("6")
    fill_separation_final_table(db, "5")
    print("done")

    prepare_separation_table(db)
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='6',
                                           featur_cluster='0')
    print("1")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='6',
                                           featur_cluster='1')
    print("2")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='6',
                                           featur_cluster='2')
    print("3")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='6',
                                           featur_cluster='3')
    print("4")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='6',
                                           featur_cluster='4')
    print("5")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='6',
                                           featur_cluster='5')
    print("6")
    fill_separation_final_table(db, "6")
    print("done final")

    get_data(db)


if __name__ == '__main__':
    salon_separation()
