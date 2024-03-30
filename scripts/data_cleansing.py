#%%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, concat, lit, to_date, round, trim

# %%
spark = SparkSession.builder.master("local[*]").getOrCreate()

file_path = '../data/spotify-2023.csv'

df = spark.read.csv(file_path, header = True, inferSchema = True)

# %%
# Data Profiling

df.printSchema()
print('Row:', df.count())
print('Column:', len(df.columns))

# %%
df_cleaned = df.dropDuplicates()
df_cleaned.summary().show()

# %%
df_cleaned.where(df_cleaned['streams'].rlike("[^0-9]")).count()

# %%
rows_with_dirty_streams = df_cleaned.filter(df_cleaned['streams'].rlike("[^0-9]"))
df_cleaned = df_cleaned.subtract(rows_with_dirty_streams)

# %%
# Cast 'streams' to be float
df_cleaned = df_cleaned.withColumn('streams', col('streams').cast('float'))

# %%
df_cleaned = df_cleaned.withColumn(
    'streams_in_millions',
    round(col('streams') / 1000000, 2)
)
df_cleaned = df_cleaned.drop('streams')

# %%
df_cleaned = df_cleaned.filter(col('streams_in_millions') != 0)

# %%
df_cleaned = df_cleaned.withColumn("track_name", trim(df_cleaned['track_name']))
df_cleaned = df_cleaned.withColumn("artist(s)_name", trim(df_cleaned['artist(s)_name']))


# %%
rows_with_null = df_cleaned.select([ sum(col(colname).isNull().cast('int')).alias(colname) for colname in df_cleaned.columns ])
rows_with_null.show()

# %%
# Drop other platform columns and all rows where the key is null
other_platforms_coulumns = [
    'in_apple_playlists',
    'in_apple_charts',
    'in_deezer_playlists',
    'in_deezer_charts',
    'in_shazam_charts'
    ]

df_cleaned = df_cleaned.drop(*other_platforms_coulumns)
df_cleaned = df_cleaned.na.drop(subset = ['key'])

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
    df_cleaned = df_cleaned.withColumnRenamed(old_name, new_name)

# %%
# Formatted released columns into a single date type column
df_cleaned = df_cleaned.withColumn(
    'released_date',
        to_date(concat(
            'released_year', lit('-'), \
            'released_month', lit('-'), \
            'released_day'
        ), 'yyyy-M-d')
)
df_cleaned = df_cleaned.drop('released_year', 'released_month', 'released_day')

# %%
# Dealing with key and mode
# According to 'Pitch class', there are 12 possibles key
# Mode can be Major and Minor
key_list = ['C', 'C#', 'D', 'D#', 'E', 'F', 'F#', 'G', 'G#', 'A', 'A#', 'B']
df_cleaned.filter(~df.key.isin(key_list)).show()

# %%
df_cleaned.filter(~df.mode.isin(['Major', 'Minor'])).show()

# %%
# Validate that audio features information columns are between 0 - 100
audio_feature_columns = ['danceability', 'valence', 'energy', 'acousticness', 'instrumentalness', 'liveness', 'speechiness']

for feature in audio_feature_columns:
    filtered_df = df_cleaned.filter(~col(feature).between(0, 100))

# %%
# EDA - Exploratory Data Analysis
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

df_pd = df_cleaned.toPandas()
df_pd.head()

# %%
df_pd.describe()

# %%
sns.histplot(df_pd['streams_in_millions'], bins = 250, kde = True, color = 'blue')
plt.xlabel('Streams in millions')
plt.ylabel('Frequency')
plt.show()

# %%
sns.boxplot(x = df_pd['streams_in_millions'])
plt.show()

# %%
columns_to_plot = ['bpm', 'danceability', 'valence', 'energy', 'acousticness', 'instrumentalness', 'liveness', 'speechiness']

for i, column in enumerate(columns_to_plot, 1):
    plt.subplot(3, 3, i)
    sns.histplot(data = df_pd, x = column, bins = 20, color = 'blue')
    plt.xlabel(column, fontsize = 12)
    plt.ylabel("Score", fontsize = 12)

plt.tight_layout()
plt.show()

# %%
for i, column in enumerate(columns_to_plot, 1):
    plt.subplot(4, 2, i)
    sns.scatterplot(data = df_pd, x = df_pd.streams_in_millions, y = df_pd[column])
    plt.xlabel("Streams", fontsize = 12)
    plt.ylabel(column, fontsize = 12)

plt.tight_layout()
plt.show()

# %%
# Save to CSV
df_pd.to_csv('../data/cleaned_spotify_songs.csv', index = False)

# %%
spark.stop()
