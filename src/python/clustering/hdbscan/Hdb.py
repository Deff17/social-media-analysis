import os
from datetime import timedelta

import hdbscan
import pandas as pd
import numpy as np

from src.python.utils.util import save_data_to_file_with_index, save_data_to_file


class Hdb:
    all_data_path = os.getcwd()
    base_data_path = "../../data/"

    def __init__(self, name_of_experiment, columns, suffix):
        self.name_of_experiment = name_of_experiment
        self.clustered_columns = columns
        self.suffix = suffix

    def _generate_stats_for_entire_data(self, start_date, end_date, salonPrefix = ''):
        results = pd.DataFrame()
        while start_date < end_date:
            features = pd.read_csv(f"{self.base_data_path}{salonPrefix}All_Data_In_Slots_Joined/" + str(start_date) + "_joined.csv")
            features.insert(1, 'start_date', start_date)
            results = pd.concat([results, features], ignore_index=True)
            start_date += timedelta(days=14)
        return results

    def save_clustering_data(self, start_date_comments, end_date_comments, salonPrefix = ''):
        alldata = self._generate_stats_for_entire_data(start_date_comments, end_date_comments, salonPrefix)[self.clustered_columns]
        save_data_to_file(f"{self.name_of_experiment}_Cluster_Data_All", "cluster.csv", alldata)

    def cluster(self, scaler):
        data_to_cluster = pd.read_csv(
            f"{self.base_data_path}{self.name_of_experiment}_Cluster_Data_All" + "/cluster.csv")
        data_without_id = data_to_cluster.drop(columns=["user_id", "start_date"])
        X = scaler.fit_transform(data_without_id)
        clusterer = hdbscan.HDBSCAN(min_cluster_size=50000)
        clusterer.fit(X)
        labels_df = pd.DataFrame(clusterer.labels_, columns=["label"])
        labeled_users = pd.concat([data_to_cluster, labels_df], axis=1)
        # cluster_df = pd.DataFrame(clusterer.cluster_centers_, columns=data_without_id.columns.values)

        save_data_to_file("HDBSCAN_All_Labeled_users", f"{self.name_of_experiment}_{self.suffix}_labeled_users.csv",
                          labeled_users)
        # save_data_to_file("HDBSCAN_All_Cluster_centers", f"{self.name_of_experiment}_{self.suffix}_cluster_centers.csv",
        #               cluster_df)
        print("Saved cluster")

    def generate_normalized_statistics(self):
        stats = [np.min, np.mean, self.median, self.q75, np.max]
        column_names = columns = {'min': 'min', 'mean': 'mean', 'median': 'median', 'q3': 'q3', 'max': 'max'}

        # get sample feature names and create aggregats eg {'std_post_frequency': [np.mean, np.std, np.min, np.max]}
        df = pd.read_csv(
                f"{self.base_data_path}HDBSCAN_All_Labeled_users/{self.name_of_experiment}_{self.suffix}_labeled_users.csv").drop(
                columns=["user_id", "start_date"])
        features = df.drop(columns=["label"]).columns.values
        aggreagates = {feat: stats for feat in features}

        #     df = pd.read_csv("Labeled_users" + "/labeled_users" + str(start_date) + ".csv").drop(columns =["user_id"])

        stats = (df.groupby(['label']).agg(aggreagates).rename(column_names))
        all_feature = pd.read_csv(
                f"{self.base_data_path}{self.name_of_experiment}_Cluster_Data_All" + "/cluster.csv").drop(
                columns=["user_id", "start_date"])
        divisors = []
        for feat in all_feature.columns.values:
            max_v = all_feature[feat].max()
            divisors.append(max_v)
            divisors.append(max_v)
            divisors.append(max_v)
            divisors.append(max_v)
            divisors.append(max_v)
        normalized = stats.div(divisors, axis='columns')
        res = normalized.T
        save_data_to_file_with_index("HDBSCAN_All_ClustersStatistics",
                                     f"{self.name_of_experiment}_{self.suffix}_normalized_clusters_stats.csv", res)
        return res

