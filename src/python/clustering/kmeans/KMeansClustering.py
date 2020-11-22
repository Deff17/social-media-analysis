import csv
import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
import os
import os
import itertools
import matplotlib.pyplot as plt
from IPython.core.display import display, HTML

from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn import metrics

from src.python.utils.util import save_data_to_file, save_data_to_file_with_index, save_fig


class KMeansClustering:
    all_data_path = os.getcwd()
    base_data_path = "../../data/"

    def __init__(self, experiment):
        self.name_of_experiment = experiment.name_of_experiment
        self.clustered_columns = experiment.clustered_columns
        self.suffix = experiment.suffix

    def _generate_stats_for_entire_data(self, start_date, end_date, salon_path_prefix=""):
        results = pd.DataFrame()
        while start_date < end_date:
            features = pd.read_csv(
                f"{self.base_data_path}{salon_path_prefix}All_Data_In_Slots_Joined/" + str(start_date) + "_joined.csv")
            features.insert(1, 'start_date', start_date)
            results = pd.concat([results, features], ignore_index=True)
            start_date += timedelta(days=14)
        return results

    def save_clustering_data(self, start_date_comments, end_date_comments, salon_path_prefix=''):
        alldata = self._generate_stats_for_entire_data(start_date_comments, end_date_comments, salon_path_prefix)[
            self.clustered_columns]
        save_data_to_file(f"{self.name_of_experiment}_Cluster_Data_All", "cluster.csv", alldata)

    def _benchmark_custering(self, k_range, sample_num, X):
        columns = ['k', 'score_mean', 'scored_std']
        results = pd.DataFrame(columns=columns)
        print("Benchmarking..")
        for k in k_range:
            scores = []
            for sam in range(0, sample_num):
                kmeans = KMeans(n_clusters=k).fit(X)
                labels = kmeans.labels_
                scores.append(metrics.calinski_harabasz_score(X, labels))
                print("Clustering done for sample: " + str(sam))
            row = pd.DataFrame([[k, np.mean(scores), np.std(scores)]], columns=columns)
            results = pd.concat([row, results], ignore_index=True)
            print("Clustering done for: " + str(k))
        return results

    # def generate_benchmark_clustering_data(self, k_range, sample_num):
    #     df = pd.read_csv(f"{self.base_data_path}{self.name_of_experiment}_Cluster_Data_All" + "/cluster.csv").drop(
    #         columns=["user_id", "start_date"])
    #     X = MinMaxScaler().fit_transform(df)
    #     results = self._benchmark_custering(k_range, sample_num, X)
    #     save_data_to_file(f"{self.name_of_experiment}_Cluster_Benchmarks_All", "cluster_benchmark.csv", results)
    #     print("Results for {} saved")

    def generate_benchmark_clustering_data(self, k_range, sample_num, scaler):
        df = pd.read_csv(f"{self.base_data_path}{self.name_of_experiment}_Cluster_Data_All" + "/cluster.csv").drop(
            columns=["user_id", "start_date"])
        X = scaler.fit_transform(df)
        results = self._benchmark_custering(k_range, sample_num, X)
        save_data_to_file(f"{self.name_of_experiment}{self.suffix}_Cluster_Benchmarks_All", "cluster_benchmark.csv",
                          results)
        print("Results for {} saved")

    def plot(self):
        df = pd.read_csv(
            f"{self.base_data_path}{self.name_of_experiment}{self.suffix}_Cluster_Benchmarks_All/cluster_benchmark.csv")
        x = df["k"]
        y = df["score_mean"]
        print(y)
        print(df["scored_std"])
        yerr = df["scored_std"]
        plt.figure(figsize=(15, 10))
        plt.title(f"Calinski Harabaz scores for different k")
        plt.ylabel("Score")
        plt.xlabel("Number of k")
        plt.errorbar(x, y, yerr=yerr, color='red', ecolor='lightgray', elinewidth=5, capthick=5)
        save_fig(f"{self.name_of_experiment}{self.suffix}_PLOT", "cluster_benchmark.png", plt)
        plt.show()

    def cluster_data(self, scelar, k, loops=1):
        data_to_cluster = pd.read_csv(
            f"{self.base_data_path}{self.name_of_experiment}_Cluster_Data_All" + "/cluster.csv")
        data_without_id = data_to_cluster.drop(columns=["user_id", "start_date"])

        max_score = 0
        for i in range(loops):
            X = scelar.fit_transform(data_without_id)
            kmeans = KMeans(n_clusters=k).fit(X)
            score = metrics.calinski_harabasz_score(X, kmeans.labels_)
            if max_score < score:
                print(f"SCORE: {score}")
                max_score = score
                labels_df = pd.DataFrame(kmeans.labels_, columns=["label"])
                labeled_users = pd.concat([data_to_cluster, labels_df], axis=1)
                cluster_df = pd.DataFrame(kmeans.cluster_centers_, columns=data_without_id.columns.values)

        #     save_data_to_file("All_Labeled_users", "labeled_users.csv", labeled_users)
        #     save_data_to_file("All_Cluster_centers", "cluster_centers.csv", cluster_df)

        save_data_to_file("All_Labeled_users", f"{self.name_of_experiment}_{self.suffix}_labeled_users.csv",
                          labeled_users)
        save_data_to_file("All_Cluster_centers", f"{self.name_of_experiment}_{self.suffix}_cluster_centers.csv",
                          cluster_df)


    def analysis(self, start_date, end_date):
        df = pd.read_csv(
            f"{self.base_data_path}All_Labeled_users/{self.name_of_experiment}_{self.suffix}_labeled_users.csv")
        #self.analysis_top(df)
        self.plot_clusters_size(df, start_date, end_date)

    def analysis_top(self, df):
        # df = pd.read_csv(
        #     f"{self.base_data_path}All_Labeled_users/{self.name_of_experiment}_{self.suffix}_labeled_users.csv")

        top = df.sort_values('number_of_received_responses_to_post_avg', ascending=False)[
            ["user_id", "number_of_received_responses_to_post_avg", "label"]].head(500)
        top_with_quantity = top.groupby(['label']).agg({'label': 'count'})
        top_with_quantity.columns = ['num_of_top_users']
        all_users = df.groupby(['label']).agg({'label': 'count'})
        all_users.columns = ['num_of_all_users']
        top_users_stats = pd.merge(all_users, top_with_quantity, how='left', on=['label'])
        top_users_stats['percentage'] = (top_users_stats['num_of_top_users'] / top_users_stats[
            'num_of_all_users']) * 100
        top_users_stats.fillna(0.0)
        save_data_to_file("Top_500_users", f"{self.name_of_experiment}_{self.suffix}_top_users.csv",
                          top)
        save_data_to_file("Top_users_stats", f"{self.name_of_experiment}_{self.suffix}_top_users.csv",
                          top_users_stats)

    def size_of_clusters(self, df, cluser_number, start_date, end_date):
        result = []
        while start_date < end_date:
            cluser = df.loc[(df["start_date"] == str(start_date)) & (df["label"] == cluser_number)]
            result.append(len(cluser.index))
            start_date += timedelta(days=14)
        return result

    def plot_clusters_size(self, df, start_date, end_date):
        cluster_numbers = df["label"].drop_duplicates()
        for clust_num in cluster_numbers:
            results = self.size_of_clusters(df, clust_num, start_date, end_date)

            plt.plot(results)
            plt.ylabel("Number of users")
            plt.xlabel("Slot number")
            plt.title("Cluseter number {}".format(clust_num))
            save_fig("ClustersAnalysisFigs",
                     f"{self.name_of_experiment}_{self.suffix}_number_of_users_in_{clust_num}.png", plt)
            plt.show()

    def get_stas_from_db(self, conn, columns, all_data, user_mapping, norm=False):
        conn = conn
        cur = conn.cursor()
        col = columns[2:]
        if norm:
            stat_table = "StatsNormFinal"
            sql = self.get_sql_query_norm(col)
            self.prepare_separation_final_table(conn, stat_table)
        else:
            sql = self.get_sql_query(col)
            stat_table = "StatsFinal"
            self.prepare_separation_final_table(conn, stat_table)

        query = f"""

         DROP TABLE SeparationRange_Stats;

        CREATE TABLE SeparationRange_Stats(
    col_max_posts_activity_time decimal,
    col_max_frequency_of_posts_avg decimal,
    col_max_frequency_of_posts_stddev decimal,
    col_max_frequency_of_comments_q3 decimal,
    col_max_number_of_received_responses_to_post_max decimal,
    col_max_number_of_received_responses_to_post_stddev decimal,
    col_max_number_of_received_responses_to_post_avg decimal,
    col_max_number_of_received_responses_to_post_q3 decimal,
    col_max_number_of_received_responses_under_comments_q3 decimal,
    col_max_number_of_received_responses_under_comments_max decimal,
    col_max_number_of_own_post_responses_q3 decimal,
    col_max_number_of_words_in_posts_q3 decimal,
    col_max_number_of_words_in_comments_median decimal,
    col_max_number_of_words_in_responses_to_posts_q3 decimal,
    col_max_number_of_words_in_own_post_responses_q3 decimal
    );

        do $$
        declare
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


    INSERT INTO SeparationRange_Stats 
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
           max(posts_activity_time), 
           max(frequency_of_posts_avg), 
           max(frequency_of_posts_stddev), 
           max(frequency_of_comments_q3),
           max(number_of_received_responses_to_post_max),
           max(number_of_received_responses_to_post_stddev),
           max(number_of_received_responses_to_post_avg),
           max(number_of_received_responses_to_post_q3),
           max(number_of_received_responses_under_comments_q3),
           max(number_of_received_responses_under_comments_max),
           max(number_of_own_post_responses_q3),
           max(number_of_words_in_posts_q3),
           max(number_of_words_in_comments_median),
           max(number_of_words_in_responses_to_posts_q3),
           max(number_of_words_in_own_post_responses_q3)
    FROM enriched;

        max_posts_activity_time := (SELECT col_max_posts_activity_time FROM SeparationRange_Stats);
        max_frequency_of_posts_avg := (SELECT col_max_frequency_of_posts_avg FROM SeparationRange_Stats);
        max_frequency_of_posts_stddev := (SELECT col_max_frequency_of_posts_stddev FROM SeparationRange_Stats);
        max_frequency_of_comments_q3 := (SELECT col_max_frequency_of_comments_q3 FROM SeparationRange_Stats);
        max_number_of_received_responses_to_post_max := (SELECT col_max_number_of_received_responses_to_post_max FROM SeparationRange_Stats);
        max_number_of_received_responses_to_post_stddev := (SELECT col_max_number_of_received_responses_to_post_stddev FROM SeparationRange_Stats);
        max_number_of_received_responses_to_post_avg := (SELECT col_max_number_of_received_responses_to_post_avg FROM SeparationRange_Stats);
        max_number_of_received_responses_to_post_q3 := (SELECT col_max_number_of_received_responses_to_post_q3 FROM SeparationRange_Stats);
        max_number_of_received_responses_under_comments_q3 := (SELECT col_max_number_of_received_responses_under_comments_q3 FROM SeparationRange_Stats);
        max_number_of_received_responses_under_comments_max := (SELECT col_max_number_of_received_responses_under_comments_max FROM SeparationRange_Stats);
        max_number_of_own_post_responses_q3 := (SELECT col_max_number_of_own_post_responses_q3 FROM SeparationRange_Stats);
        max_number_of_words_in_posts_q3 := (SELECT col_max_number_of_words_in_posts_q3 FROM SeparationRange_Stats);
        max_number_of_words_in_comments_median := (SELECT col_max_number_of_words_in_comments_median FROM SeparationRange_Stats);
        max_number_of_words_in_responses_to_posts_q3 := (SELECT col_max_number_of_words_in_responses_to_posts_q3 FROM SeparationRange_Stats);
        max_number_of_words_in_own_post_responses_q3 := (SELECT col_max_number_of_words_in_own_post_responses_q3 FROM SeparationRange_Stats);

        
        INSERT INTO {stat_table}
        WITH enriched AS (SELECT * FROM {all_data} JOIN {user_mapping} USING (user_id, date))
        SELECT
        label,
        {sql}
        FROM enriched
        GROUP BY label;
        end$$;


    """
        cur.execute(query)
        # res = cur.fetchall()
        conn.commit()
        cur.close()
        # return res

    def get_sql_query_norm(self, cols):
        base = ""
        for col_name in cols:
            base += f""" MIN({col_name}) / max_{col_name} AS min_{col_name}, AVG({col_name}) / max_{col_name} AS {col_name}, (percentile_cont(0.5) WITHIN GROUP (ORDER BY {col_name}) / max_{col_name}) AS median_{col_name}, (percentile_cont(0.75) WITHIN GROUP (ORDER BY {col_name}) / max_{col_name}) AS q3_{col_name}, MAX({col_name}) / max_{col_name} AS max,""".strip(
                '\n')
        return base.strip('\n')[:-1]

    def get_sql_query(self, cols):
        base = ""
        for col_name in cols:
            base += f""" MIN({col_name}) AS min_{col_name}, AVG({col_name}) AS {col_name}, (percentile_cont(0.5) WITHIN GROUP (ORDER BY {col_name})) AS median_{col_name}, (percentile_cont(0.75) WITHIN GROUP (ORDER BY {col_name}) ) AS q3_{col_name}, MAX({col_name}) AS max,""".strip(
                '\n')
        return base.strip('\n')[:-1]

    def prepare_separation_final_table(self, con, stat_table):
        conn = con
        cur = conn.cursor()
        query = f""" 
    DROP TABLE {stat_table};

    CREATE TABLE {stat_table}(
    label varchar(50),
    posts_activity_time_min decimal,
    posts_activity_time_mean decimal,
    posts_activity_time_median decimal,
    posts_activity_time_q3 decimal,
    posts_activity_time_max decimal,
    frequency_of_posts_avg_min decimal,
    frequency_of_posts_avg_mean decimal,
    frequency_of_posts_avg_median decimal,
    frequency_of_posts_avg_q3 decimal,
    frequency_of_posts_avg_max decimal,
    frequency_of_posts_stddev_min decimal,
    frequency_of_posts_stddev_mean decimal,
    frequency_of_posts_stddev_median decimal,
    frequency_of_posts_stddev_q3 decimal,
    frequency_of_posts_stddev_max decimal,
    frequency_of_comments_q3_min decimal,
    frequency_of_comments_q3_mean decimal,
    frequency_of_comments_q3_median decimal,
    frequency_of_comments_q3_q3 decimal,
    frequency_of_comments_q3_max decimal,
    number_of_received_responses_to_post_max_min decimal,
    number_of_received_responses_to_post_max_mean decimal,
    number_of_received_responses_to_post_max_median decimal,
    number_of_received_responses_to_post_max_q3 decimal,
    number_of_received_responses_to_post_max_max decimal,
    number_of_received_responses_to_post_stddev_min decimal,
    number_of_received_responses_to_post_stddev_mean decimal,
    number_of_received_responses_to_post_stddev_median decimal,
    number_of_received_responses_to_post_stddev_q3 decimal,
    number_of_received_responses_to_post_stddev_max decimal,
    number_of_received_responses_to_post_avg_min decimal,
    number_of_received_responses_to_post_avg_mean decimal,
    number_of_received_responses_to_post_avg_median decimal,
    number_of_received_responses_to_post_avg_q3 decimal,
    number_of_received_responses_to_post_avg_max decimal,
    number_of_received_responses_to_post_q3_min decimal,
    number_of_received_responses_to_post_q3_mean decimal,
    number_of_received_responses_to_post_q3_median decimal,
    number_of_received_responses_to_post_q3_q3 decimal,
    number_of_received_responses_to_post_q3_max decimal,
    number_of_received_responses_under_comments_q3_min decimal,
    number_of_received_responses_under_comments_q3_mean decimal,
    number_of_received_responses_under_comments_q3_median decimal,
    number_of_received_responses_under_comments_q3_q3 decimal,
    number_of_received_responses_under_comments_q3_max decimal,
    number_of_received_responses_under_comments_max_min decimal,
    number_of_received_responses_under_comments_max_mean decimal,
    number_of_received_responses_under_comments_max_median decimal,
    number_of_received_responses_under_comments_max_q3 decimal,
    number_of_received_responses_under_comments_max_max decimal,
    number_of_own_post_responses_q3_min decimal,
    number_of_own_post_responses_q3_mean decimal,
    number_of_own_post_responses_q3_median decimal,
    number_of_own_post_responses_q3_q3 decimal,
    number_of_own_post_responses_q3_max decimal,
    number_of_words_in_posts_q3_min decimal,
    number_of_words_in_posts_q3_mean decimal,
    number_of_words_in_posts_q3_median decimal,
    number_of_words_in_posts_q3_q3 decimal,
    number_of_words_in_posts_q3_max decimal,
    number_of_words_in_comments_median_min decimal,
    number_of_words_in_comments_median_mean decimal,
    number_of_words_in_comments_median_median decimal,
    number_of_words_in_comments_median_q3 decimal,
    number_of_words_in_comments_median_max decimal,
    number_of_words_in_responses_to_posts_q3_min decimal,
    number_of_words_in_responses_to_posts_q3_mean decimal,
    number_of_words_in_responses_to_posts_q3_median decimal,
    number_of_words_in_responses_to_posts_q3_q3 decimal,
    number_of_words_in_responses_to_posts_q3_max decimal,
    number_of_words_in_own_post_responses_q3_min decimal,
    number_of_words_in_own_post_responses_q3_mean decimal,
    number_of_words_in_own_post_responses_q3_median decimal,
    number_of_words_in_own_post_responses_q3_q3 decimal,
    number_of_words_in_own_post_responses_q3_max decimal
    );
        """
        cur.execute(query)
        conn.commit()
        cur.close()

    def get_cluster(self, conn, base, clust, statTable):
        cur = conn.cursor()
        query = f"""
        SELECT {base} FROM {statTable} WHERE label = '{clust}'
        """.format()
        cur.execute(query)
        res = cur.fetchall()
        conn.commit()
        cur.close()
        return res

    def save_norm_stats(self, conn):
        l = [[f"{feat}_min", f"{feat}_mean", f"{feat}_median", f"{feat}_q3", f"{feat}_max"] for feat in
             self.clustered_columns[2:]]
        flat_list_cols = [item for sublist in l for item in sublist]

        base = ""
        for f in flat_list_cols:
            base += f"{f}, "

        base = base[:-2]

        # clust = ['0', '1', '2', '3', '4', '5', '6']
        clust = ['0', '1', '2', '3', '4', '5', '6', '7', '8']
        data = []
        for c in clust:
            res = self.get_cluster(conn, base, c, "StatsNormFinal")
            data.extend(res)

        mux = pd.MultiIndex.from_product([self.clustered_columns[2:], ['min', 'mean', 'median', 'q3', 'max']])
        df = pd.DataFrame(data, columns=mux)
        to_L = df.T.astype(float)
        save_data_to_file_with_index(f"NORM_STATS_SQL{self.name_of_experiment}", f"norm_stats_{self.suffix}.csv", to_L)

    def save_separate_stats(self, conn):
        l = [[f"{feat}_min", f"{feat}_mean", f"{feat}_median", f"{feat}_q3", f"{feat}_max"] for feat in
             self.clustered_columns[2:]]
        flat_list_cols = [item for sublist in l for item in sublist]

        base = ""
        for f in flat_list_cols:
            base += f"{f}, "
        base = base[:-2]

        # clust = ['0', '1', '2', '3', '4', '5', '6']
        clust = ['0', '1', '2', '3', '4', '5', '6', '7', '8']

        for c in clust:
            data = []
            res = self.get_cluster(conn, base, c, "StatsFinal")
            data.extend(res)

            mux = pd.MultiIndex.from_product([self.clustered_columns[2:], ['min', 'mean', 'median', 'q3', 'max']])
            df = pd.DataFrame(data, columns=mux)
            to_L = df.T.astype(float)
            save_data_to_file_with_index(f"STATS_SQL{self.name_of_experiment}", f"{c}_stats_{self.suffix}.csv", to_L)


################ SALON


    def get_stas_from_db_salon(self, conn, columns, all_data, user_mapping, norm=False):
        conn = conn
        cur = conn.cursor()
        col = columns[2:]
        if norm:
            stat_table = "StatsNormFinal"
            sql = self.get_sql_query_norm(col)
            self.prepare_separation_final_table_salon(conn, stat_table)
        else:
            sql = self.get_sql_query(col)
            stat_table = "StatsFinal"
            self.prepare_separation_final_table_salon(conn, stat_table)

    # number_of_posts

        query = f"""

        DROP TABLE SeparationRange_Stats;

        CREATE TABLE SeparationRange_Stats(
    col_max_number_of_posts decimal,
    col_max_number_of_comments decimal,
    col_max_frequency_of_posts_stddev decimal,
    col_max_frequency_of_comments_q3 decimal,
    col_max_frequency_of_comments_stddev decimal,
    col_max_number_of_received_responses_to_post_max decimal,
    col_max_number_of_received_responses_to_post_q3 decimal,
    col_max_number_of_received_responses_under_comments_q3 decimal,
    col_max_number_of_received_responses_under_comments_max decimal,
    col_max_number_of_own_post_responses_q3 decimal,
    col_max_number_of_words_in_posts_q3 decimal,
    col_max_number_of_words_in_comments_q3 decimal,
    col_max_number_of_words_in_responses_to_posts_q3 decimal,
    col_max_number_of_words_in_own_post_responses_q3 decimal
    );

        do $$
        declare
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


    INSERT INTO SeparationRange_Stats 
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
    max(number_of_posts),
    max(number_of_comments),
    max(frequency_of_posts_stddev),
    max(frequency_of_comments_q3),
    max(frequency_of_comments_stddev),
    max(number_of_received_responses_to_post_max),
    max(number_of_received_responses_to_post_q3),
    max(number_of_received_responses_under_comments_q3),
    max(number_of_received_responses_under_comments_max),
    max(number_of_own_post_responses_q3),
    max(number_of_words_in_posts_q3),
    max(number_of_words_in_comments_q3),
    max(number_of_words_in_responses_to_posts_q3),
    max(number_of_words_in_own_post_responses_q3)
    FROM enriched;

    max_number_of_posts := (SELECT col_max_number_of_posts FROM SeparationRange_Stats);
    max_number_of_comments := (SELECT col_max_number_of_comments FROM SeparationRange_Stats);
    max_frequency_of_posts_stddev := (SELECT col_max_frequency_of_posts_stddev FROM SeparationRange_Stats);
    max_frequency_of_comments_q3 := (SELECT col_max_frequency_of_comments_q3 FROM SeparationRange_Stats);
    max_frequency_of_comments_stddev := (SELECT col_max_frequency_of_comments_stddev FROM SeparationRange_Stats);
    max_number_of_received_responses_to_post_max := (SELECT col_max_number_of_received_responses_to_post_max FROM SeparationRange_Stats);
    max_number_of_received_responses_to_post_q3 := (SELECT col_max_number_of_received_responses_to_post_q3 FROM SeparationRange_Stats);
    max_number_of_received_responses_under_comments_q3 := (SELECT col_max_number_of_received_responses_under_comments_q3 FROM SeparationRange_Stats);
    max_number_of_received_responses_under_comments_max := (SELECT col_max_number_of_received_responses_under_comments_max FROM SeparationRange_Stats);
    max_number_of_own_post_responses_q3 := (SELECT col_max_number_of_own_post_responses_q3 FROM SeparationRange_Stats);
    max_number_of_words_in_posts_q3 := (SELECT col_max_number_of_words_in_posts_q3 FROM SeparationRange_Stats);
    max_number_of_words_in_comments_q3 := (SELECT col_max_number_of_words_in_comments_q3 FROM SeparationRange_Stats);
    max_number_of_words_in_responses_to_posts_q3 := (SELECT col_max_number_of_words_in_responses_to_posts_q3 FROM SeparationRange_Stats);
    max_number_of_words_in_own_post_responses_q3 := (SELECT col_max_number_of_words_in_own_post_responses_q3 FROM SeparationRange_Stats);

        
        INSERT INTO {stat_table}
        WITH enriched AS (SELECT * FROM {all_data} JOIN {user_mapping} USING (user_id, date))
        SELECT
        label,
        {sql}
        FROM enriched
        GROUP BY label;
        end$$;


    """
        cur.execute(query)
        # res = cur.fetchall()
        conn.commit()
        cur.close()
        # return res


    def prepare_separation_final_table_salon(self, con, stat_table):
        conn = con
        cur = conn.cursor()
        query = f""" 
    DROP TABLE {stat_table};

    CREATE TABLE {stat_table}(
    label varchar(50),
    number_of_posts_min decimal,
    number_of_posts_mean decimal,
    number_of_posts_median decimal,
    number_of_posts_q3 decimal,
    number_of_posts_max decimal,
    number_of_comments_min decimal,
    number_of_comments_mean decimal,
    number_of_comments_median decimal,
    number_of_comments_q3 decimal,
    number_of_comments_max decimal,
    frequency_of_posts_stddev_min decimal,
    frequency_of_posts_stddev_mean decimal,
    frequency_of_posts_stddev_median decimal,
    frequency_of_posts_stddev_q3 decimal,
    frequency_of_posts_stddev_max decimal,
    frequency_of_comments_q3_min decimal,
    frequency_of_comments_q3_mean decimal,
    frequency_of_comments_q3_median decimal,
    frequency_of_comments_q3_q3 decimal,
    frequency_of_comments_q3_max decimal,
    frequency_of_comments_stddev_min decimal,
    frequency_of_comments_stddev_mean decimal,
    frequency_of_comments_stddev_median decimal,
    frequency_of_comments_stddev_q3 decimal,
    frequency_of_comments_stddev_max decimal,
    number_of_received_responses_to_post_max_min decimal,
    number_of_received_responses_to_post_max_mean decimal,
    number_of_received_responses_to_post_max_median decimal,
    number_of_received_responses_to_post_max_q3 decimal,
    number_of_received_responses_to_post_max_max decimal,
    number_of_received_responses_to_post_q3_min decimal,
    number_of_received_responses_to_post_q3_mean decimal,
    number_of_received_responses_to_post_q3_median decimal,
    number_of_received_responses_to_post_q3_q3 decimal,
    number_of_received_responses_to_post_q3_max decimal,
    number_of_received_responses_under_comments_q3_min decimal,
    number_of_received_responses_under_comments_q3_mean decimal,
    number_of_received_responses_under_comments_q3_median decimal,
    number_of_received_responses_under_comments_q3_q3 decimal,
    number_of_received_responses_under_comments_q3_max decimal,
    number_of_received_responses_under_comments_max_min decimal,
    number_of_received_responses_under_comments_max_mean decimal,
    number_of_received_responses_under_comments_max_median decimal,
    number_of_received_responses_under_comments_max_q3 decimal,
    number_of_received_responses_under_comments_max_max decimal,
    number_of_own_post_responses_q3_min decimal,
    number_of_own_post_responses_q3_mean decimal,
    number_of_own_post_responses_q3_median decimal,
    number_of_own_post_responses_q3_q3 decimal,
    number_of_own_post_responses_q3_max decimal,
    number_of_words_in_posts_q3_min decimal,
    number_of_words_in_posts_q3_mean decimal,
    number_of_words_in_posts_q3_median decimal,
    number_of_words_in_posts_q3_q3 decimal,
    number_of_words_in_posts_q3_max decimal,
    number_of_words_in_comments_q3_min decimal,
    number_of_words_in_comments_q3_mean decimal,
    number_of_words_in_comments_q3_median decimal,
    number_of_words_in_comments_q3_q3 decimal,
    number_of_words_in_comments_q3_max decimal,
    number_of_words_in_responses_to_posts_q3_min decimal,
    number_of_words_in_responses_to_posts_q3_mean decimal,
    number_of_words_in_responses_to_posts_q3_median decimal,
    number_of_words_in_responses_to_posts_q3_q3 decimal,
    number_of_words_in_responses_to_posts_q3_max decimal,
    number_of_words_in_own_post_responses_q3_min decimal,
    number_of_words_in_own_post_responses_q3_mean decimal,
    number_of_words_in_own_post_responses_q3_median decimal,
    number_of_words_in_own_post_responses_q3_q3 decimal,
    number_of_words_in_own_post_responses_q3_max decimal
    );
        """
        cur.execute(query)
        conn.commit()
        cur.close()
