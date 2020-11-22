import os
import csv
import pandas as pd
import numpy as np
from pathlib import Path
base = "/Users/dawid.prokopek/Documents/Private/MasterThesis/social_media_analysis/src/python/data"

def save_data_to_file(folder_name, file_name, data, header = True):
    try:
        # Create target Directory
        base = "/Users/dawid.prokopek/Documents/Private/MasterThesis/social_media_analysis/src/python/data"
        # Path().mkdir( exist_ok=True)
        # Path("/my/directory").os.mkdir(folder_name)
        os.mkdir(base + "/" + folder_name)
        print("Directory ", folder_name, " Created ")
    except FileExistsError:
        e = 1
        # print("Directory " , folder_name ,  " already exists")
    data.to_csv(base + "/" + folder_name + "/" + file_name, index=False, header = header)


def save_data_to_file_with_index(folder_name, file_name, data):
    try:
        # Create target Directory
        base = "/Users/dawid.prokopek/Documents/Private/MasterThesis/social_media_analysis/src/python/data"
        os.mkdir(base + "/" + folder_name)
        print("Directory ", folder_name, " Created ")
    except FileExistsError:
        e = 1
        # print("Directory " , folder_name ,  " already exists")
    data.to_csv(base + "/" + folder_name + "/" + file_name)


def save_fig(folder_name, file_name, plt):
    try:
        # Create target Directory
        base = "/Users/dawid.prokopek/Documents/Private/MasterThesis/social_media_analysis/src/python/data"
        # Path().mkdir( exist_ok=True)
        # Path("/my/directory").os.mkdir(folder_name)
        os.mkdir(base + "/" + folder_name)
        print("Directory ", folder_name, " Created ")
    except FileExistsError:
        e = 1
        # print("Directory " , folder_name ,  " already exists")
    plt.savefig(base + "/" + folder_name + "/" + file_name, dpi=300)


cluster_dict = {
    "0": "engaged_commentator",
    "1": "systematic_blogger",
    "2": "engaged_blogger",
    "3": "authority",
    "4": "influential_blogger",
    "5": "encouraging_blogger",
    "6": "common_commentator"
}

cluster_dict_num = {
    0: "engaged_commentator",
    1: "systematic_blogger",
    2: "engaged_blogger",
    3: "authority",
    4: "influential_blogger",
    5: "encouraging_blogger",
    6: "common_commentator"
}

cluster_dict_salon_num = {
    0: "expressive_commentator",
    1: "low_active_user",
    2: "active_user",
    3: "casual_commentator",
    4: "systematic_user",
    5: "influential_user",
    6: "engaging_blogger"
}

cluster_dict_salon = {
    "0": "expressive_commentator",
    "1": "low_active_user",
    "2": "active_user",
    "3": "casual_commentator",
    "4": "systematic_user",
    "5": "influential_user",
    "6": "engaging_blogger"
}