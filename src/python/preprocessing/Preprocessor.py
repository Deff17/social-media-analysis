import os

# Klasa przygotowująca dane do analizowania po wykonaniu klasteringu
class Preprocessor:
    all_data_path = os.getcwd()
    base_data_path = "../../data/"

    def __init__(self, experiment):
        self.name_of_experiment = experiment.name_of_experiment
        self.clustered_columns = experiment.clustered_columns
        self.suffix = experiment.suffix
        self.clusters = experiment.clusters
        self.cluster_dict = experiment.cluster_dict
