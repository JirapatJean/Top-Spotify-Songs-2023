#%%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, concat, lit, to_date

spark = SparkSession.builder.master("local[*]").getOrCreate()

file_path = '../data/spotify-2023.csv'

dt = spark.read.csv(file_path, header = True, inferSchema = True)

# %%
# Data Profiling

dt.printSchema()
print('Row:', dt.count())
print('Distinct row:', dt.distinct().count())
print('Column:', len(dt.columns))

# %%
dt.summary().show()

# %%
rows_with_null = dt.select([ sum(col(colname).isNull().cast('int')).alias(colname) for colname in dt.columns ])
rows_with_null.show()

# %%
# Cast 'streams' to be 
dt = dt.withColumn('streams', col('streams').cast('float'))

# %%
# Drop other platform columns and all rows where the key is null
other_platforms = ['in_apple_playlists',
                   'in_apple_charts',
                   'in_deezer_playlists',
                   'in_deezer_charts',
                   'in_shazam_charts'
                   ]

dt = dt.drop(*other_platforms)
dt = dt.na.drop(subset = ['key'])

# %%
columns_to_rename = {
    'danceability_%': 'danceability',
    'valence_%': 'valence',
    'energy_%': 'energy',
    'acousticness_%': 'acousticness',
    'instrumentalness_%': 'instrumentalness',
    'liveness_%': 'liveness',
    'speechiness_%': 'speechiness'
}
for old_name, new_name in columns_to_rename.items():
    dt = dt.withColumnRenamed(old_name, new_name)

# %%
dt = dt.withColumn(
    'released_date',
        to_date(concat(
            'released_year', lit('-'), \
            'released_month', lit('-'), \
            'released_day'
        ), 'yyyy-M-d')
)
dt = dt.drop('released_year', 'released_month', 'released_day')

# %%
dt.printSchema()

# %%
# Dealing with key and mode
# According to 'Pitch class', there are 12 possibles key
# Mode can be Major and Minor
key_list = ['C', 'C#', 'D', 'D#', 'E', 'F', 'F#', 'G', 'G#', 'A', 'A#', 'B']
dt.filter(~dt.key.isin(key_list)).show()

# %%
dt.filter(~dt.mode.isin(['Major', 'Minor'])).show()

# %%
# EDA - Exploratory Data Analysis
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

dt_pd = dt.toPandas()
dt_pd.head()

# %%
sns.boxplot(x = dt_pd['streams'])

# %%
columns_to_plot = ['bpm', 'danceability', 'valence', 'energy', 'acousticness', 'instrumentalness', 'liveness', 'speechiness']

for i, column in enumerate(columns_to_plot, 1):
    plt.subplot(3, 3, i)
    sns.histplot(data=dt_pd, x = column, bins=20, color='blue')
    plt.xlabel(column, fontsize = 12)
    plt.ylabel("Score", fontsize = 12)

plt.tight_layout()
plt.show()

# %%
for i, column in enumerate(columns_to_plot, 1):
    plt.subplot(4, 2, i)
    sns.scatterplot(data = dt_pd, x = dt_pd.streams, y = dt_pd[column])
    plt.xlabel("Streams", fontsize = 12)
    plt.ylabel(column, fontsize = 12)

plt.tight_layout()
plt.show()

# %%
# Save to CSV
dt.coalesce(1).write.csv('../data/cleaned_spotify_songs', header = True)

