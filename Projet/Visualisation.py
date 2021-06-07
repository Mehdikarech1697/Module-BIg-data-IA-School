import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import os
import sys
from time import sleep
path = '/home/mehdi/Downloads/meteo.csv'
path2 = '/home/mehdi/Downloads/stats.csv'
if os.path.exists(path2):
    stats = pd.read_csv(path2,usecols=['summary','dt','temp_min','temp_max','temperature','humidité','pressure','wind_speed','wind_deg','weather'])
    stats.set_index('summary',inplace=True)
    total_columns = stats.columns
    num_col = stats._get_numeric_data().columns
    cat_col = list(set(total_columns)-set(num_col))
    describe_num_df = stats.describe(include=["int64","float64"])
    describe_num_df.reset_index(inplace=True)
    # To remove any variable from plot
    describe_num_df = describe_num_df[describe_num_df["index"] != "count"]
    for i in num_col:
        if i in ["index"]:
            continue
    sns.factorplot(x="index", y=i, data=describe_num_df)
    plt.show()
    sleep(10)
    corrmat = pd.read_csv(path,usecols=['summary','dt','temp_min','temp_max','temperature','humidité','pressure','wind_speed','wind_deg','weather']).corr()
    plt.figure(figsize=(13, 6))
    sns.heatmap(corrmat, vmax=1, annot=True, linewidths=.5)
    plt.xticks(rotation=30, horizontalalignment="right")
    plt.show()

else : 
    sys.exit()