from src.python.database.Database import Database
from src.python.database.DbUtils import connection_HP, engine_HP
from src.python.utils.util import save_data_to_file, save_data_to_file_with_index, save_fig, cluster_dict
import matplotlib.pyplot as plt
import pandas as pd

base = "../data/"


def number_of_changes_from_role_to_different(db, user_mapping, folder):
    conn = db.conn
    cur = conn.cursor()
    query = f"""
    WITH aggregates AS (
        SELECT l.label as from_label, r.label AS to_label, count(*) AS number_of_changes 
        FROM {user_mapping} l 
        JOIN {user_mapping} r 
        ON l.user_id = r.user_id AND l.label <> r.label AND l.date = r.date - interval '14 day' 
        GROUP BY l.label, r.label 
        ORDER BY count(*) desc), 
    num_of_users AS (
        SELECT count(*) AS users_num, label 
        FROM {user_mapping} 
    GROUP BY label) 
    SELECT DISTINCT from_label,
                    to_label, 
                    number_of_changes, 
                    (CAST(number_of_changes AS DECIMAL)  / CAST(users_num AS DECIMAL)) * 100 AS change_ratio 
    FROM aggregates 
    JOIN num_of_users ON from_label = label 
    ORDER BY change_ratio desc;
    """
    cur.execute(query)
    res = cur.fetchall()
    conn.commit()
    cur.close()
    data = []
    data.extend(res)
    df = pd.DataFrame(data,
                  columns=["from_claster", "to_claster", "number_of_changes", "change_ratio"])
    save_data_to_file(folder, "numbers_of_roles_changes", df)

def number_of_unchanged_roles_slots(db, user_mapping, folder):
    conn = db.conn
    cur = conn.cursor()
    query = f"""
    WITH sequences AS (
        SELECT user_id, label, date, row_number() over (partition by label, user_id, grup order by user_id, date) as sequence_number
        FROM (
            SELECT user_id,
                   label,
                   date,
                   (row_number() over (order by user_id, date) - row_number() over (partition by label, user_id order by user_id, date) ) as grup
                   FROM {user_mapping}) t
        ORDER BY user_id, date, sequence_number
    ),
    sequences_with_groups AS (
        SELECT user_id, label, sequence_number, row_number() over (order by user_id, date) - sequence_number as unique_group
        FROM sequences
    ),
    aggregated_sequences AS (
        SELECT user_id, label, MAX(sequence_number) AS stability_number
        FROM sequences_with_groups
        GROUP BY user_id, label, unique_group
    )
    SELECT label,
           AVG(stability_number),
           STDDEV(stability_number) AS standard_deviation,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY stability_number) AS median,
           PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY stability_number) AS q3,
           MAX(stability_number) AS max,
           MIN(stability_number) AS min
    FROM aggregated_sequences
    WHERE stability_number > 1
    GROUP BY label
    """
    cur.execute(query)
    res = cur.fetchall()
    conn.commit()
    cur.close()
    data = []
    data.extend(res)
    df = pd.DataFrame(data,
                  columns=["cluster", "average", "standard_deviation", "median", "q3", "max", "min"])
    save_data_to_file(folder, "numbers_of_sequences_of_persisted_roles", df)

def number_of_unchanged_roles_slots_ratio(db, user_mapping, folder):
    conn = db.conn
    cur = conn.cursor()
    query = f"""
    WITH sequences AS (
        SELECT user_id, label, date, row_number() over (partition by label, user_id, grup order by user_id, date) as sequence_number
        FROM (
            SELECT user_id,
                   label,
                   date,
                   (row_number() over (order by user_id, date) - row_number() over (partition by label, user_id order by user_id, date) ) as grup
                   FROM {user_mapping}) t
        ORDER BY user_id, date, sequence_number
    ),
    sequences_with_groups AS (
        SELECT user_id, label, sequence_number, row_number() over (order by user_id, date) - sequence_number as unique_group
        FROM sequences
    ),
    aggregated_sequences AS (
        SELECT user_id, label, MAX(sequence_number) AS stability_number
        FROM sequences_with_groups
        GROUP BY user_id, label, unique_group
    ),
    num_of_users AS (
        SELECT label, count(*) AS users_num
        FROM {user_mapping}
        GROUP BY label
    ),
    aggregates AS (
        SELECT label,
               SUM(CASE WHEN stability_number = 2 THEN 1 ELSE stability_number - 1 END) AS number_of_pesisted_slots
        FROM aggregated_sequences
        WHERE stability_number > 1
        GROUP BY label
    )
    SELECT label,
           number_of_pesisted_slots, 
           (CAST(number_of_pesisted_slots AS DECIMAL)  / CAST(users_num AS DECIMAL)) * 100 AS stability_ratio 
    FROM aggregates 
    JOIN num_of_users USING (label)
    ORDER BY stability_ratio desc;
    """
    cur.execute(query)
    res = cur.fetchall()
    conn.commit()
    cur.close()
    data = []
    data.extend(res)
    df = pd.DataFrame(data,
                      columns=["cluster", "number_of_persisted_slots", "stability_ratio"])
    save_data_to_file(folder, "numbers_of_unchanged_ration", df)

def date_sequence(num):
    return f"l.date = r.date - interval '{num * 14} day' "

def join_statment(left, right):
    return f"""JOIN cluster_mapping {right} 
                ON {left}.user_id = {right}.user_id 
                AND {left}.label = {right}.label 
                AND {left}.date = {right}.date - interval '14 day' """

def number_of_unchanged_roles_slots_plots(db, user_mapping, folder, cluster):

    conn = db.conn
    cur = conn.cursor()
    query = f"""
    WITH cluster_mapping AS (
        SELECT user_id, label, date FROM {user_mapping} WHERE label = '{cluster}'
    ),
    mapping_with_zero_agg AS (
        SELECT date, COUNT(*) as all_user_num FROM cluster_mapping GROUP BY date
    ),
    mapping_with_one_slot AS (
        SELECT l.user_id, l.date as date FROM cluster_mapping l 
        {join_statment("l", "r")}
    ),
    mapping_with_one_agg AS (
        SELECT date, COUNT(*) as the_same_role_for_2_slots FROM mapping_with_one_slot GROUP BY date
    ),

    mapping_with_two_slot AS (
        SELECT l.user_id, l.date as date FROM cluster_mapping l 
        {join_statment("l", "r")}
        {join_statment("r", "r2")}
    ),
    mapping_with_two_agg AS (
        SELECT date, COUNT(*) as the_same_role_for_3_slots FROM mapping_with_two_slot GROUP BY date
    ),

    mapping_with_three_slot AS (
        SELECT l.user_id, l.date as date FROM cluster_mapping l 
        {join_statment("l", "r")}
        {join_statment("r", "r2")}
        {join_statment("r2", "r3")}
    ),
    mapping_with_three_agg AS (
        SELECT date, COUNT(*) as the_same_role_for_4_slots FROM mapping_with_three_slot GROUP BY date
    ),

    mapping_with_four_slot AS (
        SELECT l.user_id, l.date as date FROM cluster_mapping l 
        {join_statment("l", "r")}
        {join_statment("r", "r2")}
        {join_statment("r2", "r3")}
        {join_statment("r3", "r4")}
    ),
    mapping_with_four_agg AS (
        SELECT date, COUNT(*) as the_same_role_for_5_slots FROM mapping_with_four_slot GROUP BY date
    ),

    mapping_with_five_slot AS (
        SELECT l.user_id, l.date as date FROM cluster_mapping l
        {join_statment("l", "r")}
        {join_statment("r", "r2")}
        {join_statment("r2", "r3")}
        {join_statment("r3", "r4")}
        {join_statment("r4", "r5")}
    ),
    mapping_with_five_agg AS (
        SELECT date, COUNT(*) as the_same_role_for_6_slots FROM mapping_with_five_slot GROUP BY date
    ),

    mapping_with_six_slot AS (
        SELECT l.user_id, l.date as date FROM cluster_mapping l 
        {join_statment("l", "r")}
        {join_statment("r", "r2")}
        {join_statment("r2", "r3")}
        {join_statment("r3", "r4")}
        {join_statment("r4", "r5")}
        {join_statment("r5", "r6")}
    ),
    mapping_with_six_agg AS (
        SELECT date, COUNT(*) as the_same_role_for_7_slots FROM mapping_with_six_slot GROUP BY date
    ),

    dates AS (
    SELECT distinct date FROM {user_mapping}
    )

    SELECT all_user_num,
           the_same_role_for_2_slots,
           the_same_role_for_3_slots,
           the_same_role_for_4_slots,
           the_same_role_for_5_slots,
           the_same_role_for_6_slots,
           the_same_role_for_7_slots
    FROM dates d
    LEFT JOIN mapping_with_zero_agg zero ON d.date = zero.date
    LEFT JOIN mapping_with_one_agg one ON d.date = one.date
    LEFT JOIN mapping_with_two_agg two ON d.date = two.date
    LEFT JOIN mapping_with_three_agg three ON d.date = three.date
    LEFT JOIN mapping_with_four_agg four ON d.date = four.date
    LEFT JOIN mapping_with_five_agg five ON d.date = five.date
    LEFT JOIN mapping_with_six_agg six ON d.date = six.date
    ORDER BY d.date
    """

    cur.execute(query)
    res = cur.fetchall()
    conn.commit()
    cur.close()
    data = []
    data.extend(res)
    df = pd.DataFrame(data,
                      columns=["all_users",
                               "the_same_role_for_2_slots",
                               "the_same_role_for_3_slots",
                               "the_same_role_for_4_slots",
                               "the_same_role_for_5_slots",
                               "the_same_role_for_6_slots",
                               "the_same_role_for_7_slots"])
    save_data_to_file(folder, f"stability_for_cluser_{cluster}", df)

def plot_stability_for_cluser(folder, cluster, dest_folder):
    df = pd.read_csv(f"{folder}/stability_for_cluser_{cluster}").fillna(0)
    df.plot(title="Stability for role {}".format(cluster_dict.get(cluster)), figsize=(15, 8), legend=True)
    plt.ylabel("number of users")
    plt.xlabel("time slot")
    save_fig(dest_folder, f"stability_for_cluser_{cluster}.png", plt)
    plt.show()

def get_general_roles_statistics(db, user_mapping, folder):
    conn = db.conn
    cur = conn.cursor()
    query = f"""
    WITH 
    general_stats AS (SELECT COUNT(1) as number_of_slots, label
    FROM {user_mapping}
    GROUP BY user_id, label)
    
    SELECT AVG(number_of_slots), stddev(number_of_slots),
    percentile_cont(0.25) WITHIN GROUP (ORDER BY number_of_slots),
    percentile_cont(0.5) WITHIN GROUP (ORDER BY number_of_slots),
    percentile_cont(0.75) WITHIN GROUP (ORDER BY number_of_slots),
    MAX(number_of_slots), MIN(number_of_slots) FROM general_stats
    GROUP BY label
    """
    cur.execute(query)
    res = cur.fetchall()
    conn.commit()
    cur.close()
    data = []
    data.extend(res)
    df = pd.DataFrame(data,
                      columns=["mean", "stddev", "25%", "50%", "75%", "max", "min"])
    save_data_to_file(folder, f"how_many_slots_in_a_rol_general_stats", df)

# jezeli dany uzytkownik chociaz raz byl jakas rola to w jakie inne role tez wpadal

column_cluster_dict = {
    "0": "number_of_slots_as_engaged_commentator",
    "1": "number_of_slots_as_systematic_blogger",
    "2": "number_of_slots_as_engaged_blogger",
    "3": "number_of_slots_as_authority",
    "4": "number_of_slots_as_influential_blogger",
    "5": "number_of_slots_as_encouraging_blogger",
    "6": "number_of_slots_as_common_commentator"
}

def get_cluster_different_roles_statistics(db, user_mapping, cluster, folder):
    conn = db.conn
    cur = conn.cursor()
    query = f"""
    WITH
    general_stats AS (
    SELECT
    user_id,
    SUM (CASE WHEN label = '0' THEN 1 ELSE 0 END) AS {column_cluster_dict.get('0')},
    SUM (CASE WHEN label = '1' THEN 1 ELSE 0 END) AS {column_cluster_dict.get('1')},
    SUM (CASE WHEN label = '2' THEN 1 ELSE 0 END) AS {column_cluster_dict.get('2')},
    SUM (CASE WHEN label = '3' THEN 1 ELSE 0 END) AS {column_cluster_dict.get('3')},
    SUM (CASE WHEN label = '4' THEN 1 ELSE 0 END) AS {column_cluster_dict.get('4')},
    SUM (CASE WHEN label = '5' THEN 1 ELSE 0 END) AS {column_cluster_dict.get('5')},
    SUM (CASE WHEN label = '6' THEN 1 ELSE 0 END) AS {column_cluster_dict.get('6')}
    FROM {user_mapping}
    GROUP BY user_id)
    
    SELECT *
    FROM general_stats
    WHERE {column_cluster_dict.get(cluster)} != 0
    """
    cur.execute(query)
    res = cur.fetchall()
    conn.commit()
    cur.close()
    data = []
    data.extend(res)
    df = pd.DataFrame(data,
                      columns=["user_id", column_cluster_dict.get('0'),
                               column_cluster_dict.get('1'),
                               column_cluster_dict.get('2'),
                               column_cluster_dict.get('3'),
                               column_cluster_dict.get('4'),
                               column_cluster_dict.get('5'),
                               column_cluster_dict.get('6')])
    save_data_to_file_with_index(folder, f"cluster_different_roles_statistics_{cluster}", df.drop(columns = ["user_id"]).describe()[1:])



def HP_stability():
    db = Database(connection_HP)
    # number_of_changes_from_role_to_different(db, "user_mapping_hp", "HP_stability")
    # number_of_unchanged_roles_slots_ratio(db, "user_mapping_hp", "HP_stability")
    # print("DONE HP_stability_ratio")
    #
    # #number_of_unchanged_roles_slots(db, "user_mapping_hp", "HP_stability")
    # print("DONE HP_stability_general_stats")
    number_of_unchanged_roles_slots_plots(db, "user_mapping_hp", "HP_stability", "0")
    print("Done 0")
    number_of_unchanged_roles_slots_plots(db, "user_mapping_hp", "HP_stability", "1")
    print("Done 1")
    number_of_unchanged_roles_slots_plots(db, "user_mapping_hp", "HP_stability", "2")
    print("Done 2")

    number_of_unchanged_roles_slots_plots(db, "user_mapping_hp", "HP_stability", "3")
    print("Done 3")

    number_of_unchanged_roles_slots_plots(db, "user_mapping_hp", "HP_stability", "4")
    print("Done 4")

    number_of_unchanged_roles_slots_plots(db, "user_mapping_hp", "HP_stability", "5")
    print("Done 5")

    number_of_unchanged_roles_slots_plots(db, "user_mapping_hp", "HP_stability", "6")
    print("DONE HP_stability")
    plot_stability_for_cluser(f"{base}/HP_stability", "0", "HP_STABILITY_PLOT")
    plot_stability_for_cluser(f"{base}/HP_stability", "1", "HP_STABILITY_PLOT")
    plot_stability_for_cluser(f"{base}/HP_stability", "2", "HP_STABILITY_PLOT")
    plot_stability_for_cluser(f"{base}/HP_stability", "3", "HP_STABILITY_PLOT")
    plot_stability_for_cluser(f"{base}/HP_stability", "4", "HP_STABILITY_PLOT")
    plot_stability_for_cluser(f"{base}/HP_stability", "5", "HP_STABILITY_PLOT")
    plot_stability_for_cluser(f"{base}/HP_stability", "6", "HP_STABILITY_PLOT")
    print("DONE plot")
    # get_general_roles_statistics(db, user_mapping="user_mapping_hp", folder="HP_stability")
    # print("DONE general stats")
    # get_cluster_different_roles_statistics(db, cluster='0', user_mapping="user_mapping_hp", folder="HP_stability")
    # get_cluster_different_roles_statistics(db, cluster='1', user_mapping="user_mapping_hp", folder="HP_stability")
    # get_cluster_different_roles_statistics(db, cluster='2', user_mapping="user_mapping_hp", folder="HP_stability")
    # get_cluster_different_roles_statistics(db, cluster='3', user_mapping="user_mapping_hp", folder="HP_stability")
    # get_cluster_different_roles_statistics(db, cluster='4', user_mapping="user_mapping_hp", folder="HP_stability")
    # get_cluster_different_roles_statistics(db, cluster='5', user_mapping="user_mapping_hp", folder="HP_stability")
    # get_cluster_different_roles_statistics(db, cluster='6', user_mapping="user_mapping_hp", folder="HP_stability")
    # print("DONE different_roles_statistics")




if __name__ == '__main__':
    HP_stability()