import os


class Experiment:
    all_data_path = os.getcwd()
    base_data_path = "../../data/"

    def __init__(self, name_of_experiment, columns, suffix, k=0, clusters_dict=None):
        self.name_of_experiment = name_of_experiment
        self.clustered_columns = columns
        self.suffix = suffix
        self.k = k
        self.clusters = [str(i) for i in range(0, k)]
        self.cluster_dict = clusters_dict

    def init_clusters(self, k):
        self.k = k
        self.suffix = self.suffix + f"_k_{k}"
        self.clusters = [str(i) for i in range(0, k)]
        return k

    def init_clusters_dict(self, clust_dict):
        self.cluster_dict = clust_dict
