from src.python.database.Database import Database
from src.python.database.DbUtils import connection_HP, engine_HP
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
    posts_activity_time decimal,
    frequency_of_posts_avg decimal,
    frequency_of_posts_stddev decimal,
    frequency_of_comments_q3 decimal,
    number_of_received_responses_to_post_max decimal,
    number_of_received_responses_to_post_stddev decimal,
    number_of_received_responses_to_post_avg decimal,
    number_of_received_responses_to_post_q3 decimal,
    number_of_received_responses_under_comments_q3 decimal,
    number_of_received_responses_under_comments_max decimal,
    number_of_own_post_responses_q3 decimal,
    number_of_words_in_posts_q3 decimal,
    number_of_words_in_comments_median decimal,
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
    posts_activity_time decimal,
    frequency_of_posts_avg decimal,
    frequency_of_posts_stddev decimal,
    frequency_of_comments_q3 decimal,
    number_of_received_responses_to_post_max decimal,
    number_of_received_responses_to_post_stddev decimal,
    number_of_received_responses_to_post_avg decimal,
    number_of_received_responses_to_post_q3 decimal,
    number_of_received_responses_under_comments_q3 decimal,
    number_of_received_responses_under_comments_max decimal,
    number_of_own_post_responses_q3 decimal,
    number_of_words_in_posts_q3 decimal,
    number_of_words_in_comments_median decimal,
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
    col_min_posts_activity_time decimal,
    col_max_posts_activity_time decimal,
    col_min_frequency_of_posts_avg decimal,
    col_max_frequency_of_posts_avg decimal,
    col_min_frequency_of_posts_stddev decimal,
    col_max_frequency_of_posts_stddev decimal,
    col_min_frequency_of_comments_q3 decimal,
    col_max_frequency_of_comments_q3 decimal,
    col_min_number_of_received_responses_to_post_max decimal,
    col_max_number_of_received_responses_to_post_max decimal,
    col_min_number_of_received_responses_to_post_stddev decimal,
    col_max_number_of_received_responses_to_post_stddev decimal,
    col_min_number_of_received_responses_to_post_avg decimal,
    col_max_number_of_received_responses_to_post_avg decimal,
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
    col_min_number_of_words_in_comments_median decimal,
    col_max_number_of_words_in_comments_median decimal,
    col_min_number_of_words_in_responses_to_posts_q3 decimal,
    col_max_number_of_words_in_responses_to_posts_q3 decimal,
    col_min_number_of_words_in_own_post_responses_q3 decimal,
    col_max_number_of_words_in_own_post_responses_q3 decimal
    );

    do $$
    declare
        declare min_posts_activity_time decimal;
        declare min_frequency_of_posts_avg decimal;
        declare min_frequency_of_posts_stddev decimal;
        declare min_frequency_of_comments_q3 decimal;
        declare min_number_of_received_responses_to_post_max decimal;
        declare min_number_of_received_responses_to_post_stddev decimal;
        declare min_number_of_received_responses_to_post_avg decimal;
        declare min_number_of_received_responses_to_post_q3 decimal;
        declare min_number_of_received_responses_under_comments_q3 decimal;
        declare min_number_of_received_responses_under_comments_max decimal;
        declare min_number_of_own_post_responses_q3 decimal;
        declare min_number_of_words_in_posts_q3 decimal;
        declare min_number_of_words_in_comments_median decimal;
        declare min_number_of_words_in_responses_to_posts_q3 decimal;
        declare min_number_of_words_in_own_post_responses_q3 decimal;
        
        declare max_posts_activity_time decimal;
        declare max_frequency_of_posts_avg decimal;
        declare max_frequency_of_posts_stddev decimal;
        declare max_frequency_of_comments_q3 decimal;
        declare max_number_of_received_responses_to_post_max decimal;
        declare max_number_of_received_responses_to_post_stddev decimal;
        declare max_number_of_received_responses_to_post_avg decimal;
        declare max_number_of_received_responses_to_post_q3 decimal;
        declare max_number_of_received_responses_under_comments_q3 decimal;
        declare max_number_of_received_responses_under_comments_max decimal;
        declare max_number_of_own_post_responses_q3 decimal;
        declare max_number_of_words_in_posts_q3 decimal;
        declare max_number_of_words_in_comments_median decimal;
        declare max_number_of_words_in_responses_to_posts_q3 decimal;
        declare max_number_of_words_in_own_post_responses_q3 decimal;
    begin

    INSERT INTO SeparationRange 
    WITH enriched AS (
        SELECT a.user_id AS user_id, 
           a.date AS date,
           u.label AS label,
           posts_activity_time, 
           frequency_of_posts_avg, 
           frequency_of_posts_stddev, 
           frequency_of_comments_q3,
           number_of_received_responses_to_post_max,
           number_of_received_responses_to_post_stddev,
           number_of_received_responses_to_post_avg,
           number_of_received_responses_to_post_q3,
           number_of_received_responses_under_comments_q3,
           number_of_received_responses_under_comments_max,
           number_of_own_post_responses_q3,
           number_of_words_in_posts_q3,
           number_of_words_in_comments_median,
           number_of_words_in_responses_to_posts_q3,
           number_of_words_in_own_post_responses_q3
    FROM {all_data} a
    JOIN {user_mapping} u ON a.user_id = u.user_id AND a.date = u.date
    )
    SELECT 
           min(posts_activity_time), 
           max(posts_activity_time), 
           min(frequency_of_posts_avg), 
           max(frequency_of_posts_avg), 
           min(frequency_of_posts_stddev), 
           max(frequency_of_posts_stddev), 
           min(frequency_of_comments_q3),
           max(frequency_of_comments_q3),
           min(number_of_received_responses_to_post_max),
           max(number_of_received_responses_to_post_max),
           min(number_of_received_responses_to_post_stddev),
           max(number_of_received_responses_to_post_stddev),
           min(number_of_received_responses_to_post_avg),
           max(number_of_received_responses_to_post_avg),
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
           min(number_of_words_in_comments_median),
           max(number_of_words_in_comments_median),
           min(number_of_words_in_responses_to_posts_q3),
           max(number_of_words_in_responses_to_posts_q3),
           min(number_of_words_in_own_post_responses_q3),
           max(number_of_words_in_own_post_responses_q3)
    FROM enriched
    WHERE label = '{featur_cluster}';
    
     min_posts_activity_time := (SELECT col_min_posts_activity_time FROM SeparationRange);
     min_frequency_of_posts_avg := (SELECT col_min_frequency_of_posts_avg FROM SeparationRange);
     min_frequency_of_posts_stddev := (SELECT col_min_frequency_of_posts_stddev FROM SeparationRange);
     min_frequency_of_comments_q3 := (SELECT col_min_frequency_of_comments_q3 FROM SeparationRange);
     min_number_of_received_responses_to_post_max := (SELECT col_min_number_of_received_responses_to_post_max FROM SeparationRange);
     min_number_of_received_responses_to_post_stddev := (SELECT col_min_number_of_received_responses_to_post_stddev FROM SeparationRange);
     min_number_of_received_responses_to_post_avg := (SELECT col_min_number_of_received_responses_to_post_avg FROM SeparationRange);
     min_number_of_received_responses_to_post_q3 := (SELECT col_min_number_of_received_responses_to_post_q3 FROM SeparationRange);
     min_number_of_received_responses_under_comments_q3 := (SELECT col_min_number_of_received_responses_under_comments_q3 FROM SeparationRange);
     min_number_of_received_responses_under_comments_max := (SELECT col_min_number_of_received_responses_under_comments_max FROM SeparationRange);
     min_number_of_own_post_responses_q3 := (SELECT col_min_number_of_own_post_responses_q3 FROM SeparationRange);
     min_number_of_words_in_posts_q3 := (SELECT col_min_number_of_words_in_posts_q3 FROM SeparationRange);
     min_number_of_words_in_comments_median := (SELECT col_min_number_of_words_in_comments_median FROM SeparationRange);
     min_number_of_words_in_responses_to_posts_q3 := (SELECT col_min_number_of_words_in_responses_to_posts_q3 FROM SeparationRange);
     min_number_of_words_in_own_post_responses_q3 := (SELECT col_min_number_of_words_in_own_post_responses_q3 FROM SeparationRange);
    
     max_posts_activity_time := (SELECT col_max_posts_activity_time FROM SeparationRange);
     max_frequency_of_posts_avg := (SELECT col_max_frequency_of_posts_avg FROM SeparationRange);
     max_frequency_of_posts_stddev := (SELECT col_max_frequency_of_posts_stddev FROM SeparationRange);
     max_frequency_of_comments_q3 := (SELECT col_max_frequency_of_comments_q3 FROM SeparationRange);
     max_number_of_received_responses_to_post_max := (SELECT col_max_number_of_received_responses_to_post_max FROM SeparationRange);
     max_number_of_received_responses_to_post_stddev := (SELECT col_max_number_of_received_responses_to_post_stddev FROM SeparationRange);
     max_number_of_received_responses_to_post_avg := (SELECT col_max_number_of_received_responses_to_post_avg FROM SeparationRange);
     max_number_of_received_responses_to_post_q3 := (SELECT col_max_number_of_received_responses_to_post_q3 FROM SeparationRange);
     max_number_of_received_responses_under_comments_q3 := (SELECT col_max_number_of_received_responses_under_comments_q3 FROM SeparationRange);
     max_number_of_received_responses_under_comments_max := (SELECT col_max_number_of_received_responses_under_comments_max FROM SeparationRange);
     max_number_of_own_post_responses_q3 := (SELECT col_max_number_of_own_post_responses_q3 FROM SeparationRange);
     max_number_of_words_in_posts_q3 := (SELECT col_max_number_of_words_in_posts_q3 FROM SeparationRange);
     max_number_of_words_in_comments_median := (SELECT col_max_number_of_words_in_comments_median FROM SeparationRange);
     max_number_of_words_in_responses_to_posts_q3 := (SELECT col_max_number_of_words_in_responses_to_posts_q3 FROM SeparationRange);
     max_number_of_words_in_own_post_responses_q3 := (SELECT col_max_number_of_words_in_own_post_responses_q3 FROM SeparationRange);

    INSERT INTO Separation
    WITH enriched AS (
        SELECT a.user_id AS user_id, 
           a.date AS date,
           u.label AS label,
           posts_activity_time, 
           frequency_of_posts_avg, 
           frequency_of_posts_stddev, 
           frequency_of_comments_q3,
           number_of_received_responses_to_post_max,
           number_of_received_responses_to_post_stddev,
           number_of_received_responses_to_post_avg,
           number_of_received_responses_to_post_q3,
           number_of_received_responses_under_comments_q3,
           number_of_received_responses_under_comments_max,
           number_of_own_post_responses_q3,
           number_of_words_in_posts_q3,
           number_of_words_in_comments_median,
           number_of_words_in_responses_to_posts_q3,
           number_of_words_in_own_post_responses_q3
    FROM all_hp_data a
    JOIN user_mapping_hp u ON a.user_id = u.user_id AND a.date = u.date
    WHERE u.label = '{cluster}'
    )
    SELECT
    user_id,
    date,
    CASE WHEN min_posts_activity_time < posts_activity_time AND posts_activity_time <  max_posts_activity_time THEN 1 ELSE 0 END AS posts_activity_time,
    CASE WHEN min_frequency_of_posts_avg < frequency_of_posts_avg AND frequency_of_posts_avg <  max_frequency_of_posts_avg THEN 1 ELSE 0 END AS frequency_of_posts_avg,
    CASE WHEN min_frequency_of_posts_stddev < frequency_of_posts_stddev AND frequency_of_posts_stddev <  max_frequency_of_posts_stddev THEN 1 ELSE 0 END AS frequency_of_posts_stddev,
    CASE WHEN min_frequency_of_comments_q3 < frequency_of_comments_q3  AND frequency_of_comments_q3 <  max_frequency_of_comments_q3 THEN 1 ELSE 0 END AS frequency_of_comments_q3,
    CASE WHEN min_number_of_received_responses_to_post_max < number_of_received_responses_to_post_max AND number_of_received_responses_to_post_max <  max_number_of_received_responses_to_post_max THEN 1 ELSE 0 END AS number_of_received_responses_to_post_max,
    CASE WHEN min_number_of_received_responses_to_post_stddev < number_of_received_responses_to_post_stddev AND number_of_received_responses_to_post_stddev <  max_number_of_received_responses_to_post_stddev THEN 1 ELSE 0 END AS number_of_received_responses_to_post_stddev,
    CASE WHEN min_number_of_received_responses_to_post_avg < number_of_received_responses_to_post_avg AND number_of_received_responses_to_post_avg <  max_number_of_received_responses_to_post_avg THEN 1 ELSE 0 END AS number_of_received_responses_to_post_avg,
    CASE WHEN min_number_of_received_responses_to_post_q3 < number_of_received_responses_to_post_q3 AND number_of_received_responses_to_post_q3 <  max_number_of_received_responses_to_post_q3 THEN 1 ELSE 0 END AS number_of_received_responses_to_post_q3,
    CASE WHEN min_number_of_received_responses_under_comments_q3 < number_of_received_responses_under_comments_q3 AND number_of_received_responses_under_comments_q3 <  max_number_of_received_responses_under_comments_q3 THEN 1 ELSE 0 END AS number_of_received_responses_under_comments_q3,
    CASE WHEN min_number_of_received_responses_under_comments_max < number_of_received_responses_under_comments_max AND number_of_received_responses_under_comments_max <  max_number_of_received_responses_under_comments_max THEN 1 ELSE 0 END AS number_of_received_responses_under_comments_max,
    CASE WHEN min_number_of_words_in_posts_q3 < number_of_words_in_posts_q3 AND number_of_words_in_posts_q3 <  max_number_of_words_in_posts_q3 THEN 1 ELSE 0 END AS number_of_words_in_posts_q3,
    CASE WHEN min_number_of_own_post_responses_q3 < number_of_own_post_responses_q3 AND number_of_own_post_responses_q3 <  max_number_of_own_post_responses_q3 THEN 1 ELSE 0 END AS number_of_own_post_responses_q3,
    CASE WHEN min_number_of_words_in_comments_median < number_of_words_in_comments_median AND number_of_words_in_comments_median <  max_number_of_words_in_comments_median THEN 1 ELSE 0 END AS number_of_words_in_comments_median,
    CASE WHEN min_number_of_words_in_responses_to_posts_q3 < number_of_words_in_responses_to_posts_q3 AND number_of_words_in_responses_to_posts_q3 <  max_number_of_words_in_responses_to_posts_q3 THEN 1 ELSE 0 END AS number_of_words_in_responses_to_posts_q3,
    CASE WHEN min_number_of_words_in_own_post_responses_q3 < number_of_words_in_own_post_responses_q3 AND number_of_words_in_own_post_responses_q3 <  max_number_of_words_in_own_post_responses_q3 THEN 1 ELSE 0 END AS number_of_words_in_own_post_responses_q3
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
    SUM(posts_activity_time) AS posts_activity_time,
    SUM(frequency_of_posts_avg) AS frequency_of_posts_avg,
    SUM(frequency_of_posts_stddev) AS frequency_of_posts_stddev,
    SUM(frequency_of_comments_q3) AS frequency_of_comments_q3,
    SUM(number_of_received_responses_to_post_max) AS number_of_received_responses_to_post_max,
    SUM(number_of_received_responses_to_post_stddev) AS number_of_received_responses_to_post_stddev,
    SUM(number_of_received_responses_to_post_avg) AS number_of_received_responses_to_post_avg,
    SUM(number_of_received_responses_to_post_q3) AS number_of_received_responses_to_post_q3,
    SUM(number_of_received_responses_under_comments_q3) AS number_of_received_responses_under_comments_q3,
    SUM(number_of_received_responses_under_comments_max) AS number_of_received_responses_under_comments_max,
    SUM(number_of_own_post_responses_q3) AS number_of_own_post_responses_q3,
    SUM(number_of_words_in_posts_q3) AS number_of_words_in_posts_q3,
    SUM(number_of_words_in_comments_median) AS number_of_words_in_comments_median,
    SUM(number_of_words_in_responses_to_posts_q3) AS number_of_words_in_responses_to_posts_q3,
    SUM(number_of_words_in_own_post_responses_q3) AS number_of_words_in_own_post_responses_q3
    FROM separation
    GROUP BY user_id, date
    ) 
    SELECT 
    {cluster},
    AVG(posts_activity_time),
    AVG(frequency_of_posts_avg),
    AVG(frequency_of_posts_stddev),
    AVG(frequency_of_comments_q3),
    AVG(number_of_received_responses_to_post_max),
    AVG(number_of_received_responses_to_post_stddev),
    AVG(number_of_received_responses_to_post_avg),
    AVG(number_of_received_responses_to_post_q3),
    AVG(number_of_received_responses_under_comments_q3),
    AVG(number_of_received_responses_under_comments_max),
    AVG(number_of_own_post_responses_q3),
    AVG(number_of_words_in_posts_q3),
    AVG(number_of_words_in_comments_median),
    AVG(number_of_words_in_responses_to_posts_q3),
    AVG(number_of_words_in_own_post_responses_q3)
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
    df = pd.DataFrame(data, columns=["cluster",
                                     "posts_activity_time",
                                     "frequency_of_posts_avg",
                                     "frequency_of_posts_stddev",
                                     "frequency_of_comments_q3",
                                     "number_of_received_responses_to_post_max",
                                     "number_of_received_responses_to_post_stddev",
                                     "number_of_received_responses_to_post_avg",
                                     "number_of_received_responses_to_post_q3",
                                     "number_of_received_responses_under_comments_q3",
                                     "number_of_received_responses_under_comments_max",
                                     "number_of_own_post_responses_q3",
                                     "number_of_words_in_posts_q3",
                                     "number_of_words_in_comments_median",
                                     "number_of_words_in_responses_to_posts_q3",
                                     "number_of_words_in_own_post_responses_q3"])


    save_data_to_file_with_index("HP_separation", "separation", df.T)


def HP_separation():
    db = Database(connection_HP)
    prepare_separation_final_table(db)

    prepare_separation_table(db)
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='0',
                                           featur_cluster='1')
    print("1")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='0',
                                           featur_cluster='2')
    print("2")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='0',
                                           featur_cluster='3')
    print("3")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='0',
                                           featur_cluster='4')
    print("4")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='0',
                                           featur_cluster='5')
    print("5")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='0',
                                           featur_cluster='6')
    print("6")
    fill_separation_final_table(db, "0")
    print("done")

    prepare_separation_table(db)
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='1',
                                           featur_cluster='0')
    print("1")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='1',
                                           featur_cluster='2')
    print("2")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='1',
                                           featur_cluster='3')
    print("3")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='1',
                                           featur_cluster='4')
    print("4")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='1',
                                           featur_cluster='5')
    print("5")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='1',
                                           featur_cluster='6')
    print("6")
    fill_separation_final_table(db, "1")
    print("done")

    prepare_separation_table(db)
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='2',
                                           featur_cluster='0')
    print("1")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='2',
                                           featur_cluster='1')
    print("2")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='2',
                                           featur_cluster='3')
    print("3")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='2',
                                           featur_cluster='4')
    print("4")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='2',
                                           featur_cluster='5')
    print("5")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='2',
                                           featur_cluster='6')
    print("6")
    fill_separation_final_table(db, "2")
    print("done")

    prepare_separation_table(db)
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='3',
                                           featur_cluster='0')
    print("1")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='3',
                                           featur_cluster='1')
    print("2")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='3',
                                           featur_cluster='2')
    print("3")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='3',
                                           featur_cluster='4')
    print("4")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='3',
                                           featur_cluster='5')
    print("5")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='3',
                                           featur_cluster='6')
    print("6")
    fill_separation_final_table(db, "3")
    print("done")

    prepare_separation_table(db)
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='4',
                                           featur_cluster='0')
    print("1")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='4',
                                           featur_cluster='1')
    print("2")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='4',
                                           featur_cluster='2')
    print("3")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='4',
                                           featur_cluster='3')
    print("4")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='4',
                                           featur_cluster='5')
    print("5")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='4',
                                           featur_cluster='6')
    print("6")
    fill_separation_final_table(db, "4")
    print("done")

    prepare_separation_table(db)
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='5',
                                           featur_cluster='0')
    print("1")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='5',
                                           featur_cluster='1')
    print("2")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='5',
                                           featur_cluster='2')
    print("3")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='5',
                                           featur_cluster='3')
    print("4")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='5',
                                           featur_cluster='4')
    print("5")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='5',
                                           featur_cluster='6')
    print("6")
    fill_separation_final_table(db, "5")
    print("done")

    prepare_separation_table(db)
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='6',
                                           featur_cluster='0')
    print("1")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='6',
                                           featur_cluster='1')
    print("2")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='6',
                                           featur_cluster='2')
    print("3")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='6',
                                           featur_cluster='3')
    print("4")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='6',
                                           featur_cluster='4')
    print("5")
    get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping_hp", cluster='6',
                                           featur_cluster='5')
    print("6")
    fill_separation_final_table(db, "6")
    print("done final")

    get_data(db)


if __name__ == '__main__':
    HP_separation()
