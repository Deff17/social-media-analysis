from datetime import timedelta
import pandas as pd

from python.clustering.ClusteringFeatures import start_date_clustering, end_date_clustering
from python.database.Database import Database
from python.database.DbUtils import connection_HP
from python.utils.util import save_data_to_file

def getVectors(db, date_start, date_end):
    conn = db.conn
    cur = conn.cursor()
    query = f"""
WITH dates AS ( SELECT date FROM unique_dates WHERE date between '{date_start}' and '{date_end}'),
users AS (SELECT user_id FROM  user_mapping_hp WHERE date = '{date_start}'),
users_and_dates AS (SELECT date, user_id FROM dates, users),
user_labels AS (SELECT user_id, label, date FROM user_mapping_hp WHERE date between '{date_start}' AND '{date_end}' AND user_id IN (SELECT user_id FROM user_mapping_hp WHERE date = '{date_start}')),
filled_lables AS (SELECT user_id, label, date FROM users_and_dates LEFT JOIN user_labels USING (user_id, date) ORDER BY date)
SELECT ARRAY_AGG (label ORDER BY date) AS VECTOR  FROM filled_lables GROUP BY user_id;

         """
    cur.execute(query)
    vectors = cur.fetchall()
    conn.commit()
    cur.close()
    return [vec[0] for vec in vectors]


def get_quenry_for_length(length):
    query = ""
    for i in range(1, length+1):
        query += f"""vector[{i}] -> 'label',
        vector[{i}] -> 'number_of_links_0',
        vector[{i}] -> 'number_of_links_1',
        vector[{i}] -> 'number_of_links_2',
        vector[{i}] -> 'number_of_links_3',
        vector[{i}] -> 'number_of_links_4',
        vector[{i}] -> 'number_of_links_5',
        vector[{i}] -> 'number_of_links_6',"""
    query += f" vector[{length+1}] -> 'label'"
    return query


def getVectorsWithNeighbours(db, date_start, date_end, length):
    conn = db.conn
    cur = conn.cursor()
    query = f"""
    WITH dates AS ( SELECT date FROM unique_dates WHERE date between '{date_start}' and '{date_end}'),
    users AS (SELECT user_id FROM  user_mapping_hp WHERE date = '{date_start}'),
    users_and_dates AS (SELECT date, user_id FROM dates, users),
    user_labels AS (SELECT user_id, label, date FROM user_mapping_hp WHERE date between '{date_start}' AND '{date_end}' AND user_id IN (SELECT user_id FROM user_mapping_hp WHERE date = '{date_start}')),
    user_neighbours AS (
        SELECT user_id,
               date,
               number_of_links_0,
               number_of_links_1,
               number_of_links_2,
               number_of_links_3,
               number_of_links_4,
               number_of_links_5,
               number_of_links_6
    FROM counted_neighbours_hp_new WHERE date between '{date_start}' AND '{date_end}' AND user_id IN (SELECT user_id FROM user_mapping_hp WHERE date = '{date_start}')),
    filled_lables AS (
        SELECT
    user_id,
    label,
                             date,
                             number_of_links_0,
                             number_of_links_1,
                             number_of_links_2,
                             number_of_links_3,
                             number_of_links_4,
                             number_of_links_5,
                             number_of_links_6
    FROM users_and_dates
    LEFT JOIN user_labels USING (user_id, date)
    LEFT JOIN user_neighbours USING (user_id, date)
    ORDER BY date),
    agg_all AS ( SELECT
    ARRAY_AGG (
        json_build_object(
            'label', label,
            'number_of_links_0', coalesce(number_of_links_0, 0),
            'number_of_links_1', coalesce(number_of_links_1, 0),
            'number_of_links_2', coalesce(number_of_links_2, 0),
            'number_of_links_3', coalesce(number_of_links_3, 0),
            'number_of_links_4', coalesce(number_of_links_4, 0),
            'number_of_links_5', coalesce(number_of_links_5, 0),
            'number_of_links_6', coalesce(number_of_links_6, 0)
        ) ORDER BY date) AS vector
    FROM filled_lables GROUP BY user_id)
    SELECT
        {get_quenry_for_length(length)}
    FROM agg_all;

         """
    cur.execute(query)
    vectors = cur.fetchall()
    conn.commit()
    cur.close()
    return [vec for vec in vectors]  # vector[:length], vector[length:]


def getPredictionData(db, start_date, end_date, vector_length, file_name):
    result = []
    predicted_date = start_date + timedelta(days=14 * (vector_length))

    while start_date < end_date:
        vectors = getVectors(db, start_date, predicted_date)
        # print(vector)
        result.extend(vectors)
        start_date += timedelta(days=14)
        predicted_date = start_date + timedelta(days=14 * (vector_length))
        print(f"finished for {start_date}")

    columns = ["slot_" + str(i) for i in range(1, vector_length + 1)]
    columns.append("predicted")
    print(columns)
    df = pd.DataFrame(result, columns=columns)
    save_data_to_file("PREDICTION_SET_NOT_OVERSAMPLED", file_name, df)


def getPredictionDataNeighbours(db, start_date, end_date, vector_length, file_name):
    result = []
    predicted_date = start_date + timedelta(days=14 * (vector_length))
    # print(vectors[0])

    while start_date < end_date:
        vectors = getVectorsWithNeighbours(db, start_date, predicted_date, vector_length)
        # print(vector)
        result.extend(vectors)
        start_date += timedelta(days=14)
        predicted_date = start_date + timedelta(days=14 * (vector_length))
        print(f"finished for {start_date}")

    columns_not_flat = [[f"slot_{i}",
                         f"n_type_0_{i}",
                         f"n_type_1_{i}",
                         f"n_type_2_{i}",
                         f"n_type_3_{i}",
                         f"n_type_4_{i}",
                         f"n_type_5_{i}",
                         f"n_type_6_{i}"] for i in range(1, vector_length + 1)]

    columns = [item for sublist in columns_not_flat for item in sublist]
    columns.append("predicted")

    df = pd.DataFrame(result, columns=columns)
    save_data_to_file("PREDICTION_SET_NOT_OVERSAMPLED_NEIGHBOURS", file_name, df)


if __name__ == '__main__':
    db = Database(connection=connection_HP)
    getPredictionData(db, start_date_clustering, end_date_clustering, 2, "only_roles_2.csv")
    getPredictionData(db, start_date_clustering, end_date_clustering, 3, "only_roles_3.csv")
    getPredictionData(db, start_date_clustering, end_date_clustering, 4, "only_roles_4.csv")
    getPredictionData(db, start_date_clustering, end_date_clustering, 5, "only_roles_5.csv")
    #print(get_quenry_for_length(2))
    getPredictionDataNeighbours(db, start_date_clustering, end_date_clustering, 2, "roles_with_neighbours_2.csv")
    getPredictionDataNeighbours(db, start_date_clustering, end_date_clustering, 3, "roles_with_neighbours_3.csv")
    getPredictionDataNeighbours(db, start_date_clustering, end_date_clustering, 4, "roles_with_neighbours_4.csv")
    getPredictionDataNeighbours(db, start_date_clustering, end_date_clustering, 5, "roles_with_neighbours_5.csv")
