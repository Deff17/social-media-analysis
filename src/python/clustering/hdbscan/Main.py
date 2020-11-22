import hdbscan
from sklearn.preprocessing import StandardScaler

from src.python.clustering.ClusteringFeatures import start_date_clustering, end_date_clustering, basic_columns, \
    end_date_salon, start_date_salon
from src.python.clustering.hdbscan.Hdb import Hdb

experiment_name = "FIRST_EXP"
experiment_name_salon = "FIRST_EXP_SALON"

def cluster_experiment(experiment_name, columns, suffix, scaler=StandardScaler(), salon_path_prefix = "", start_date = start_date_clustering, end_date= end_date_clustering):
    hdb = Hdb(experiment_name, columns, suffix)
    hdb.save_clustering_data(start_date, end_date, salon_path_prefix)
    hdb.cluster(scaler)
    hdb.generate_normalized_statistics()
    #firstKmeans.plot()

def huff():
    cluster_experiment(experiment_name, basic_columns, "first")

def salon():
    cluster_experiment(experiment_name_salon, basic_columns, "firstSALON", salon_path_prefix="SALON_",start_date=start_date_salon, end_date=end_date_salon)

if __name__ == '__main__':
    #huff()
    salon()
    print("done")