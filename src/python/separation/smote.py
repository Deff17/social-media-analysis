# example of oversampling a multi-class classification dataset
import pandas as pd
from imblearn.over_sampling import SMOTE
from sklearn.model_selection import train_test_split
from imblearn.pipeline import make_pipeline
from imblearn.under_sampling import NearMiss

from python.utils.util import save_data_to_file


# folder = "SALON_NEIGHBOURS_OVERSAMPLED"
# file = "salon_oversampled_neighbours4.csv"
# base = "../data/"
# path = f"{base}SALON_PREDICTION_SET_NOT_OVERSAMPLED_NEIGHBOURS/roles_with_neighbours_4.csv"


roles = {
    2: 10000,
    5: 8000
}

# roles2 = {
#     0: 20000,
#     6: 20000
# }
#
folder = "SALON_ONLY_ROLES_OVERSAMPLED"
file = "salon_oversampled_roles4.csv"
base = "../data/"
path = f"{base}SALON_PREDICTION_SET_NOT_OVERSAMPLED/only_roles_4.csv"

if __name__ == '__main__':
    df = pd.read_csv(path, header=0, index_col=None)
    df.dropna(inplace=True)
    X = df.iloc[:, 0:-1]
    y = df.iloc[:, -1]
    columns = X.columns
    xx = SMOTE(sampling_strategy="auto", k_neighbors=20)
    #xx = NearMiss(sampling_strategy=roles2))

    X_res, y_res = xx.fit_resample(X, y)
    pred = pd.DataFrame(X_res)
    pred.columns = columns
    dframe = pd.DataFrame({'predicted': y_res})
    pred = pd.concat([pred, dframe], axis=1, sort=False)
    save_data_to_file(folder, file, pred)

    #save_data_to_file(folder, file, df)

    print("saved")
