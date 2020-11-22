from src.python.database.Database import Database
from src.python.database.DbUtils import connection_SALON
from src.python.utils.util import save_data_to_file, save_data_to_file_with_index
import matplotlib.pyplot as plt
import pandas as pd

base = "../data/"


def prepare_separation_neighbours_table(db):
    conn = db.conn
    cur = conn.cursor()
    query = f""" 
    DROP TABLE neighbour_separation;

    CREATE TABLE neighbour_separation(
    role_type varchar(50),
    type_0_neighbourhood decimal,
    type_1_neighbourhood decimal,
    type_2_neighbourhood decimal,
    type_3_neighbourhood decimal,
    type_4_neighbourhood decimal,
    type_5_neighbourhood decimal,
    type_6_neighbourhood decimal
    );
        """
    cur.execute(query)
    conn.commit()
    cur.close()

def prepare_separation_neighbours_table_TEMP(db):
    conn = db.conn
    cur = conn.cursor()
    query = f""" 
    DROP TABLE neighbour_separation_temp;

    CREATE TABLE neighbour_separation_temp(
    user_id int,
    date date,
   -- neighbour_type varchar(50),
    type_0_neighbourhood decimal,
    type_1_neighbourhood decimal,
    type_2_neighbourhood decimal,
    type_3_neighbourhood decimal,
    type_4_neighbourhood decimal,
    type_5_neighbourhood decimal,
    type_6_neighbourhood decimal
    );
        """
    cur.execute(query)
    conn.commit()
    cur.close()


def get_separation_for_role_and_neighbourhood(db, user_mapping, role, other_role):
    conn = db.conn
    cur = conn.cursor()
    sql = f"""
    INSERT INTO neighbour_separation_temp
    WITH enriched AS (
    SELECT
        m.user_id,
        m.date,
        c.type AS neighbour_type,
        c.number_of_links AS number_of_links
    FROM counted_neighbours c
    JOIN {user_mapping} m USING(user_id, date) WHERE m.label = '{role}'
    ),
    flatten AS 
    (SELECT
    user_id,
    date,
    CASE WHEN neighbour_type = '0' THEN number_of_links ELSE 0 END AS number_of_links_with_type_0,
    CASE WHEN neighbour_type = '1' THEN number_of_links ELSE 0 END AS number_of_links_with_type_1,
    CASE WHEN neighbour_type = '2' THEN number_of_links ELSE 0 END AS number_of_links_with_type_2,
    CASE WHEN neighbour_type = '3' THEN number_of_links ELSE 0 END AS number_of_links_with_type_3,
    CASE WHEN neighbour_type = '4' THEN number_of_links ELSE 0 END AS number_of_links_with_type_4,
    CASE WHEN neighbour_type = '5' THEN number_of_links ELSE 0 END AS number_of_links_with_type_5,
    CASE WHEN neighbour_type = '6' THEN number_of_links ELSE 0 END AS number_of_links_with_type_6
    FROM enriched),
    neighbourhood_of_users AS (SELECT
        user_id,
        date,
        sum(number_of_links_with_type_0) AS number_of_links_with_type_0,
        sum(number_of_links_with_type_1) AS number_of_links_with_type_1,
        sum(number_of_links_with_type_2) AS number_of_links_with_type_2,
        sum(number_of_links_with_type_3) AS number_of_links_with_type_3,
        sum(number_of_links_with_type_4) AS number_of_links_with_type_4,
        sum(number_of_links_with_type_5) AS number_of_links_with_type_5,
        sum(number_of_links_with_type_6) AS number_of_links_with_type_6
    FROM flatten GROUP BY user_id, date),
    

    enriched_other_role AS (
    SELECT 
        m.user_id,
        m.date,  
        c.type AS neighbour_type, 
        c.number_of_links 
    FROM counted_neighbours c
    JOIN {user_mapping} m USING(user_id, date)
    WHERE m.label = '{other_role}'
    ),
    max_min_neighbourhood AS (SELECT 
        neighbour_type, 
        date, 
        max(number_of_links) as max_num, 
        min(number_of_links) as min_num 
    FROM enriched_other_role
    GROUP BY neighbour_type, date),
    mins AS (SELECT 
        date,
        CASE WHEN neighbour_type = '0' THEN min_num ELSE 0 END AS min_number_of_links_with_type_0,
        CASE WHEN neighbour_type = '1' THEN min_num ELSE 0 END AS min_number_of_links_with_type_1,
        CASE WHEN neighbour_type = '2' THEN min_num ELSE 0 END AS min_number_of_links_with_type_2,
        CASE WHEN neighbour_type = '3' THEN min_num ELSE 0 END AS min_number_of_links_with_type_3,
        CASE WHEN neighbour_type = '4' THEN min_num ELSE 0 END AS min_number_of_links_with_type_4,
        CASE WHEN neighbour_type = '5' THEN min_num ELSE 0 END AS min_number_of_links_with_type_5,
        CASE WHEN neighbour_type = '6' THEN min_num ELSE 0 END AS min_number_of_links_with_type_6
        FROM max_min_neighbourhood
        ),
    maxes AS (
    SELECT 
        date,
        CASE WHEN neighbour_type = '0' THEN max_num ELSE 0 END AS max_number_of_links_with_type_0,
        CASE WHEN neighbour_type = '1' THEN max_num ELSE 0 END AS max_number_of_links_with_type_1,
        CASE WHEN neighbour_type = '2' THEN max_num ELSE 0 END AS max_number_of_links_with_type_2,
        CASE WHEN neighbour_type = '3' THEN max_num ELSE 0 END AS max_number_of_links_with_type_3,
        CASE WHEN neighbour_type = '4' THEN max_num ELSE 0 END AS max_number_of_links_with_type_4,
        CASE WHEN neighbour_type = '5' THEN max_num ELSE 0 END AS max_number_of_links_with_type_5,
        CASE WHEN neighbour_type = '6' THEN max_num ELSE 0 END AS max_number_of_links_with_type_6
        FROM max_min_neighbourhood
    ),
    min_max_grouped AS (SELECT date,
             sum(min_number_of_links_with_type_0) AS min_number_of_links_with_type_0,
             sum(max_number_of_links_with_type_0) AS max_number_of_links_with_type_0,
             sum(min_number_of_links_with_type_1) AS min_number_of_links_with_type_1,
             sum(max_number_of_links_with_type_1) AS max_number_of_links_with_type_1,
             sum(min_number_of_links_with_type_2) AS min_number_of_links_with_type_2,
             sum(max_number_of_links_with_type_2) AS max_number_of_links_with_type_2,
             sum(min_number_of_links_with_type_3) AS min_number_of_links_with_type_3,
             sum(max_number_of_links_with_type_3) AS max_number_of_links_with_type_3,
             sum(min_number_of_links_with_type_4) AS min_number_of_links_with_type_4,
             sum(max_number_of_links_with_type_4) AS max_number_of_links_with_type_4,
             sum(min_number_of_links_with_type_5) AS min_number_of_links_with_type_5,
             sum(max_number_of_links_with_type_5) AS max_number_of_links_with_type_5,
             sum(min_number_of_links_with_type_6) AS min_number_of_links_with_type_6,
             sum(max_number_of_links_with_type_6) AS max_number_of_links_with_type_6
      FROM maxes JOIN mins USING (date)
      GROUP BY date),
      enriched_with_min_max AS (
      SELECT 
      *
      FROM neighbourhood_of_users n
      JOIN min_max_grouped m USING (date))
      SELECT 
      user_id, 
      date,
     -- '{role} - {other_role}' AS u,
      CASE WHEN min_number_of_links_with_type_0 <= number_of_links_with_type_0 AND number_of_links_with_type_0 <= max_number_of_links_with_type_0 THEN 1 ELSE 0 END AS type_0_neighbourhood,
      CASE WHEN min_number_of_links_with_type_1 <= number_of_links_with_type_1 AND number_of_links_with_type_1 <= max_number_of_links_with_type_1 THEN 1 ELSE 0 END AS type_1_neighbourhood,
      CASE WHEN min_number_of_links_with_type_2 <= number_of_links_with_type_2 AND number_of_links_with_type_2 <= max_number_of_links_with_type_2 THEN 1 ELSE 0 END AS type_2_neighbourhood,
      CASE WHEN min_number_of_links_with_type_3 <= number_of_links_with_type_3 AND number_of_links_with_type_3 <= max_number_of_links_with_type_3 THEN 1 ELSE 0 END AS type_3_neighbourhood,
      CASE WHEN min_number_of_links_with_type_4 <= number_of_links_with_type_4 AND number_of_links_with_type_4 <= max_number_of_links_with_type_4 THEN 1 ELSE 0 END AS type_4_neighbourhood,
      CASE WHEN min_number_of_links_with_type_5 <= number_of_links_with_type_5 AND number_of_links_with_type_5 <= max_number_of_links_with_type_5 THEN 1 ELSE 0 END AS type_5_neighbourhood,
      CASE WHEN min_number_of_links_with_type_6 <= number_of_links_with_type_6 AND number_of_links_with_type_6 <= max_number_of_links_with_type_6 THEN 1 ELSE 0 END AS type_6_neighbourhood
      FROM enriched_with_min_max

    """
    cur.execute(sql)
    conn.commit()
    cur.close()

def fill_neighbour_separation(db, role):
    conn = db.conn
    cur = conn.cursor()
    sql = f"""
    INSERT INTO neighbour_separation
    WITH all_sums AS (SELECT 
      user_id,
      date,
      sum(type_0_neighbourhood) AS type_0_neighbourhood,
      sum(type_1_neighbourhood) AS type_1_neighbourhood,
      sum(type_2_neighbourhood) AS type_2_neighbourhood,
      sum(type_3_neighbourhood) AS type_3_neighbourhood,
      sum(type_4_neighbourhood) AS type_4_neighbourhood,
      sum(type_5_neighbourhood) AS type_5_neighbourhood,
      sum(type_6_neighbourhood) AS type_6_neighbourhood
      FROM neighbour_separation_temp
      GROUP BY user_id, date)
      SELECT
      '{role}' AS role_type,
      avg(type_0_neighbourhood) AS type_0_neighbourhood,
      avg(type_1_neighbourhood) AS type_1_neighbourhood,
      avg(type_2_neighbourhood) AS type_2_neighbourhood,
      avg(type_3_neighbourhood) AS type_3_neighbourhood,
      avg(type_4_neighbourhood) AS type_4_neighbourhood,
      avg(type_5_neighbourhood) AS type_5_neighbourhood,
      avg(type_6_neighbourhood) AS type_6_neighbourhood
      FROM all_sums"""
    cur.execute(sql)
    conn.commit()
    cur.close()

def get_data(db):
    conn = db.conn
    cur = conn.cursor()
    query = f"""
        SELECT * FROM neighbour_separation;
    """
    cur.execute(query)
    res = cur.fetchall()
    conn.commit()
    cur.close()
    data = []
    data.extend(res)
    df = pd.DataFrame(data, columns=['role_type',
                                     'type_0_neighbourhood',
                                     'type_1_neighbourhood',
                                     'type_2_neighbourhood',
                                     'type_3_neighbourhood',
                                     'type_4_neighbourhood',
                                     'type_5_neighbourhood',
                                     'type_6_neighbourhood'])

    save_data_to_file_with_index("Salon_separation_neighbourhood", "separation", df.T)


def salon_separation_neighbourhood():
    db = Database(connection_SALON)
    prepare_separation_neighbours_table(db)

    prepare_separation_neighbours_table_TEMP(db)
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="0", other_role="1")
    print("1")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="0", other_role="2")
    print("2")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="0", other_role="3")
    print("3")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="0", other_role="4")
    print("4")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="0", other_role="5")
    print("5")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="0", other_role="6")
    print("6")
    fill_neighbour_separation(db, "0")



    prepare_separation_neighbours_table_TEMP(db)
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="1", other_role="0")
    print("1")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="1", other_role="2")
    print("2")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="1", other_role="3")
    print("3")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="1", other_role="4")
    print("4")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="1", other_role="5")
    print("5")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="1", other_role="6")
    print("6")
    fill_neighbour_separation(db, "1")

    prepare_separation_neighbours_table_TEMP(db)
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="2", other_role="1")
    print("1")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="2", other_role="0")
    print("2")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="2", other_role="3")
    print("3")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="2", other_role="4")
    print("4")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="2", other_role="5")
    print("5")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="2", other_role="6")
    print("6")
    fill_neighbour_separation(db, "2")

    prepare_separation_neighbours_table_TEMP(db)
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="3", other_role="1")
    print("1")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="3", other_role="2")
    print("2")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="3", other_role="0")
    print("3")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="3", other_role="4")
    print("4")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="3", other_role="5")
    print("5")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="3", other_role="6")
    print("6")
    fill_neighbour_separation(db, "3")

    prepare_separation_neighbours_table_TEMP(db)
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="4", other_role="1")
    print("1")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="4", other_role="2")
    print("2")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="4", other_role="3")
    print("3")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="4", other_role="0")
    print("4")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="4", other_role="5")
    print("5")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="4", other_role="6")
    print("6")
    fill_neighbour_separation(db, "4")



    prepare_separation_neighbours_table_TEMP(db)
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="5", other_role="1")
    print("1")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="5", other_role="2")
    print("2")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="5", other_role="3")
    print("3")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="5", other_role="4")
    print("4")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="5", other_role="0")
    print("5")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="5", other_role="6")
    print("6")
    fill_neighbour_separation(db, "5")



    prepare_separation_neighbours_table_TEMP(db)
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="6", other_role="1")
    print("1")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="6", other_role="2")
    print("2")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="6", other_role="3")
    print("3")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="6", other_role="4")
    print("4")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="6", other_role="5")
    print("5")
    get_separation_for_role_and_neighbourhood(db, user_mapping="user_mapping", role="6", other_role="0")
    print("6")
    fill_neighbour_separation(db, "6")


    get_data(db)


if __name__ == '__main__':
    salon_separation_neighbourhood()

    # sql = """
    #
    #       all_sums AS (SELECT
    #   user_id,
    #   date,
    #   sum(type_0_neighbourhood) AS type_0_neighbourhood,
    #   sum(type_1_neighbourhood) AS type_1_neighbourhood,
    #   sum(type_2_neighbourhood) AS type_2_neighbourhood,
    #   sum(type_3_neighbourhood) AS type_3_neighbourhood,
    #   sum(type_4_neighbourhood) AS type_4_neighbourhood,
    #   sum(type_5_neighbourhood) AS type_5_neighbourhood,
    #   sum(type_6_neighbourhood) AS type_6_neighbourhood
    #   FROM separation
    #   GROUP BY user_id, date)
    #   SELECT
    #   '{role}' AS role_type,
    #   avg(type_0_neighbourhood) AS type_0_neighbourhood,
    #   avg(type_1_neighbourhood) AS type_1_neighbourhood,
    #   avg(type_2_neighbourhood) AS type_2_neighbourhood,
    #   avg(type_3_neighbourhood) AS type_3_neighbourhood,
    #   avg(type_4_neighbourhood) AS type_4_neighbourhood,
    #   avg(type_5_neighbourhood) AS type_5_neighbourhood,
    #   avg(type_6_neighbourhood) AS type_6_neighbourhood
    #   FROM all_sums"""


    # prepare_separation_table(db)
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='0',
    #                                        featur_cluster='1')
    # print("1")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='0',
    #                                        featur_cluster='2')
    # print("2")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='0',
    #                                        featur_cluster='3')
    # print("3")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='0',
    #                                        featur_cluster='4')
    # print("4")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='0',
    #                                        featur_cluster='5')
    # print("5")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='0',
    #                                        featur_cluster='6')
    # print("6")
    # fill_separation_final_table(db, "0")
    # print("done")
    #
    # prepare_separation_table(db)
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='1',
    #                                        featur_cluster='0')
    # print("1")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='1',
    #                                        featur_cluster='2')
    # print("2")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='1',
    #                                        featur_cluster='3')
    # print("3")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='1',
    #                                        featur_cluster='4')
    # print("4")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='1',
    #                                        featur_cluster='5')
    # print("5")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='1',
    #                                        featur_cluster='6')
    # print("6")
    # fill_separation_final_table(db, "1")
    # print("done")
    #
    # prepare_separation_table(db)
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='2',
    #                                        featur_cluster='0')
    # print("1")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='2',
    #                                        featur_cluster='1')
    # print("2")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='2',
    #                                        featur_cluster='3')
    # print("3")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='2',
    #                                        featur_cluster='4')
    # print("4")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='2',
    #                                        featur_cluster='5')
    # print("5")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='2',
    #                                        featur_cluster='6')
    # print("6")
    # fill_separation_final_table(db, "2")
    # print("done")
    #
    # prepare_separation_table(db)
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='3',
    #                                        featur_cluster='0')
    # print("1")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='3',
    #                                        featur_cluster='1')
    # print("2")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='3',
    #                                        featur_cluster='2')
    # print("3")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='3',
    #                                        featur_cluster='4')
    # print("4")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='3',
    #                                        featur_cluster='5')
    # print("5")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='3',
    #                                        featur_cluster='6')
    # print("6")
    # fill_separation_final_table(db, "3")
    # print("done")
    #
    # prepare_separation_table(db)
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='4',
    #                                        featur_cluster='0')
    # print("1")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='4',
    #                                        featur_cluster='1')
    # print("2")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='4',
    #                                        featur_cluster='2')
    # print("3")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='4',
    #                                        featur_cluster='3')
    # print("4")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='4',
    #                                        featur_cluster='5')
    # print("5")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='4',
    #                                        featur_cluster='6')
    # print("6")
    # fill_separation_final_table(db, "4")
    # print("done")
    #
    # prepare_separation_table(db)
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='5',
    #                                        featur_cluster='0')
    # print("1")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='5',
    #                                        featur_cluster='1')
    # print("2")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='5',
    #                                        featur_cluster='2')
    # print("3")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='5',
    #                                        featur_cluster='3')
    # print("4")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='5',
    #                                        featur_cluster='4')
    # print("5")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='5',
    #                                        featur_cluster='6')
    # print("6")
    # fill_separation_final_table(db, "5")
    # print("done")
    #
    # prepare_separation_table(db)
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='6',
    #                                        featur_cluster='0')
    # print("1")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='6',
    #                                        featur_cluster='1')
    # print("2")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='6',
    #                                        featur_cluster='2')
    # print("3")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='6',
    #                                        featur_cluster='3')
    # print("4")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='6',
    #                                        featur_cluster='4')
    # print("5")
    # get_cluster_different_roles_statistics(db, all_data="all_hp_data", user_mapping="user_mapping", cluster='6',
    #                                        featur_cluster='5')
    # print("6")
    # fill_separation_final_table(db, "6")
    # print("done final")
