import hdbscan
from sklearn.datasets import make_blobs


if __name__ == '__main__':
    data, _ = make_blobs(100)

    clusterer = hdbscan.HDBSCAN(min_cluster_size=5)
    cluster_labels = clusterer.fit_predict(data)
    print(clusterer.labels_)