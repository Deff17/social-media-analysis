from src.python.clustering.ClusteringFeatures import basic_columns, start_date_clustering, end_date_clustering, \
    columns_chosen_by_klaudia, columns_with_additional_new_feature, basic_columns_without_std_and_responses_q3, \
    basic_columns_modified, start_date_salon, end_date_salon, basic_columns_new_corr_fixed_act, \
    new_selected_columns1, new_selected_columns2, basic_columns_salon
from src.python.clustering.Experiment import Experiment
from src.python.clustering.kmeans.KMeansClustering import KMeansClustering
from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler, QuantileTransformer, PowerTransformer, \
    Normalizer, MaxAbsScaler

#from src.python.database.DbUtils import connection_HP
from src.python.database.DbUtils import connection_SALON

first_experiment_name = "HPMojeCechy"  # cechy wybrane przeze mnie podczas analizy
second_experiment_name = "HPCechyKlaudii"  # klastrowanie z cechami jakie miala klaudai
third_experiment_name = "HPMojeCechyZDodakowaCecha"  # dodanie cechy unikalnych odpowiedzi na posty
features_without_std_exp_name = "HPMojeCechy_without_STD"
features_modified_exp_name = "HPMojeCechy_zmodyfikowane"
features_new_corr_exp_name = "HPMojeCechyCORRR"
fixed_post_activity_exp = "HPMojeFixedPostAct"
fixed_post_activity_exp_old = "HPMojeFixedPostActOldColumns"
fixed_post_activity_new_analysis1 = "HPMojeNowaAnaliza1"
fixed_post_activity_new_analysis2 = "HPMojeNowaAnaliza2"
fixed_post_activity_new_analysis3 = "HPMojeNowaAnaliza3"
salon_24_new_analysis = "Salon24_new_analysis"

salon_experiment_name = "SALON_MojeCechy"
salon_fixed_post_activity_exp = "SALON_MojeFixedPostAct"
salon_fixed_post_activity_exp_old = "SALON_MojeFixedPostActOldColumns"

standard_scaler_experiment_name = "HPMojeCechy_StandardScaler"
robust_scaler_experiment_name = "HPMojeCechy_RobustScaler"
normalizer_experiment_name = "HPMojeCechy_Normalizer"

old_columns_experiment_with_SS = Experiment(fixed_post_activity_exp_old, basic_columns, "_StandardScaler")
salon_24_new_analysis_experiment = Experiment(salon_24_new_analysis, basic_columns_salon, "_StandardScaler")


# def first_experiment():
#     firstKmeans = KMeansClustering(first_experiment_name, basic_columns, "")
#     # firstKmeans.save_clustering_data(start_date_clustering, end_date_clustering)
#     firstKmeans.generate_benchmark_clustering_data(range(4, 11), 10)
#
#
# def second_experiment():
#     secondKmeans = KMeansClustering(second_experiment_name, columns_chosen_by_klaudia, "")
#     secondKmeans.save_clustering_data(start_date_clustering, end_date_clustering)
#     secondKmeans.generate_benchmark_clustering_data(range(4, 11), 10)
#
#
# def third_experiment():
#     thirdKmeans = KMeansClustering(third_experiment_name, columns_with_additional_new_feature, "")
#     thirdKmeans.save_clustering_data(start_date_clustering, end_date_clustering)
#     thirdKmeans.generate_benchmark_clustering_data(range(4, 11), 10)


def experiment(experiment, scaler=StandardScaler(), salon_path_prefix="", start_date=start_date_clustering,
               end_date=end_date_clustering):
    firstKmeans = KMeansClustering(experiment)
    # firstKmeans.save_clustering_data(start_date, end_date, salon_path_prefix)
    # firstKmeans.generate_benchmark_clustering_data(range(5, 15), 5, scaler)
    firstKmeans.plot()


def cluster_best(experiment, k, scaler=StandardScaler()):
    firstKmeans = KMeansClustering(experiment)
    firstKmeans.cluster_data(scaler, k, 1)
    # firstKmeans.generate_normalized_statistics()
    # firstKmeans.generate_cluster_statistics()
    # firstKmeans.generate_seperate_cluster_statistics(k)
    print("Done cluster_best")


def analysis(experiment_name, start_date=start_date_clustering, end_date=end_date_clustering):
    firstKmeans = KMeansClustering(experiment_name)
    firstKmeans.analysis(start_date, end_date)


def stats(experiment_name, columns, suffix, conn):
    firstKmeans = KMeansClustering(experiment_name, columns, suffix)
    firstKmeans.get_stas_from_db(conn, columns, "all_hp_data", "user_mapping_hp")
    firstKmeans.get_stas_from_db(conn, columns, "all_hp_data", "user_mapping_hp", True)
    firstKmeans.save_norm_stats(conn)
    firstKmeans.save_separate_stats(conn)


def statsSalon(experiment, conn):
    firstKmeans = KMeansClustering(experiment)
    firstKmeans.get_stas_from_db_salon(conn, experiment.clustered_columns, "all_hp_data", "user_mapping")
    firstKmeans.get_stas_from_db_salon(conn, experiment.clustered_columns, "all_hp_data", "user_mapping", True)
    firstKmeans.save_norm_stats(conn)
    firstKmeans.save_separate_stats(conn)


def huff():
    # first_experiment()
    # second_experiment()
    # third_experiment()
    # experiment(features_without_std_exp_name, basic_columns_without_std_and_responses_q3, "_StandardScaler", StandardScaler())
    # experiment(features_without_std_exp_name, basic_columns_without_std_and_responses_q3, "_RobustScaler", RobustScaler())
    # experiment(features_without_std_exp_name, basic_columns_without_std_and_responses_q3, "_Normalizer", Normalizer())
    #
    # experiment(features_modified_exp_name, basic_columns_modified, "_StandardScaler", StandardScaler())
    # experiment(features_modified_exp_name, basic_columns_modified, "_RobustScaler", RobustScaler())
    # experiment(features_modified_exp_name, basic_columns_modified, "_Normalizer", Normalizer())
    # experiment(first_experiment_name, basic_columns, "_QuantileTransformer", QuantileTransformer())
    # experiment(first_experiment_name, basic_columns, "_PowerTransformer", PowerTransformer())
    # experiment(first_experiment_name, basic_columns, "_MaxAbsScaler", MaxAbsScaler())
    # experiment(first_experiment_name, basic_columns, "_StandardScaler", StandardScaler())
    # experiment(fixed_post_activity_exp, basic_columns_new_corr_fixed_act, "_StandardScaler", StandardScaler())
    # experiment(fixed_post_activity_exp_old, basic_columns, "_StandardScaler", StandardScaler())

    # cluster_best(first_experiment_name, basic_columns, "_StandardScaler_k_6", 6)
    # cluster_best(first_experiment_name, basic_columns, "_StandardScaler_k_7", 7)
    # cluster_best(first_experiment_name, basic_columns, "_StandardScaler_k_8", 8)
    # cluster_best(first_experiment_name, basic_columns, "_StandardScaler_k_9", 9)

    # cluster_best(features_without_std_exp_name, basic_columns_without_std_and_responses_q3, "_StandardScaler_k_8", 8, StandardScaler())
    # cluster_best(features_without_std_exp_name, basic_columns_without_std_and_responses_q3, "_RobustScaler_k_9", 9, RobustScaler())
    # cluster_best(features_without_std_exp_name, basic_columns_without_std_and_responses_q3, "_Normalizer_k_9", 9, Normalizer())
    # cluster_best(features_modified_exp_name, basic_columns_modified, "_StandardScaler_k_9", 9, StandardScaler())
    # cluster_best(features_modified_exp_name, basic_columns_modified, "_RobustScaler_k_9", 9, RobustScaler())
    # cluster_best(features_modified_exp_name, basic_columns_modified, "_Normalizer_k_9", 9, Normalizer())

    # cluster_best(fixed_post_activity_exp, basic_columns_new_corr_fixed_act, "_StandardScaler_k_7", 7)
    # cluster_best(fixed_post_activity_exp_old, basic_columns, "_StandardScaler_k_7", 7)

    # analysis(fixed_post_activity_exp, basic_columns_new_corr_fixed_act, "_StandardScaler_k_7")
    # analysis(fixed_post_activity_exp_old, basic_columns, "_StandardScaler_k_7")
    # analysis(fixed_post_activity_exp_old, basic_columns, "_StandardScaler_k_7")
    # stats(fixed_post_activity_exp_old, basic_columns, "_StandardScaler_k_7", connection_HP)
    # stats(fixed_post_activity_exp_old, basic_columns, "_StandardScaler_k_9", connection_HP)

    # experiment(fixed_post_activity_exp_old, basic_columns, "_StandardScaler", StandardScaler())

    # experiment(fixed_post_activity_new_analysis1, new_selected_columns1, "_StandardScaler", StandardScaler())
    # experiment(fixed_post_activity_new_analysis2, new_selected_columns2, "_StandardScaler", StandardScaler())

    # cluster_best(fixed_post_activity_new_analysis1, new_selected_columns1, "_StandardScaler_k_9", 9)
    # cluster_best(fixed_post_activity_new_analysis2, new_selected_columns2, "_StandardScaler_k_12", 12)
    # cluster_best(fixed_post_activity_exp_old, basic_columns, "_StandardScaler_k_11", 11)
    # cluster_best(fixed_post_activity_exp_old, basic_columns, "_StandardScaler_k_9", 9)

    # experiment(fixed_post_activity_new_analysis3, new_selected_columns, "_StandardScaler", StandardScaler())
    # cluster_best(fixed_post_activity_new_analysis1, new_selected_columns, "_StandardScaler_k_9", 9)

    # experiment(old_columns_experiment_with_SS, StandardScaler())
    # cluster_best(old_columns_experiment_with_SS, old_columns_experiment_with_SS.init_clusters(7))
    print("end")




#
# #
# analysis(first_experiment_name, basic_columns, "_StandardScaler_k_6")
# analysis(first_experiment_name, basic_columns, "_StandardScaler_k_7")
# analysis(first_experiment_name, basic_columns, "_StandardScaler_k_9")

def salon():
    # experiment(salon_fixed_post_activity_exp, basic_columns_new_corr_fixed_act, "_StandardScaler", StandardScaler(), "SALON_", start_date_salon, end_date_salon)
    # experiment(salon_fixed_post_activity_exp_old, basic_columns, "_StandardScaler", StandardScaler(), "SALON_", start_date_salon, end_date_salon)
    # print("done experiment")
    # cluster_best(salon_experiment_name, basic_columns, "_StandardScaler_k_7", 7)
    # cluster_best(salon_fixed_post_activity_exp, basic_columns_new_corr_fixed_act, "_StandardScaler_k_7", 7)
    # cluster_best(salon_fixed_post_activity_exp_old, basic_columns, "_StandardScaler_k_7", 7)
    # print("done cluster best")
    # analysis(salon_experiment_name, basic_columns, "_StandardScaler_k_7", start_date_salon, end_date_salon)
    # analysis(salon_fixed_post_activity_exp, basic_columns_new_corr_fixed_act, "_StandardScaler_k_7", start_date_salon, end_date_salon)
    # analysis(salon_fixed_post_activity_exp_old, basic_columns, "_StandardScaler_k_7", start_date_salon, end_date_salon)


    experiment(salon_24_new_analysis_experiment, StandardScaler(), "SALON_", start_date_salon, end_date_salon)
    #cluster_best(salon_24_new_analysis_experiment, salon_24_new_analysis_experiment.init_clusters(7))

    # salon_24_new_analysis_experiment.init_clusters(7)
    # # statsSalon(salon_24_new_analysis_experiment, connection_SALON)
    # analysis(salon_24_new_analysis_experiment, start_date_salon, end_date_salon)
    # _StandardScaler_k_7

    print("done analysis cluster best")


if __name__ == '__main__':
    #huff()
    salon()
    print("done")
