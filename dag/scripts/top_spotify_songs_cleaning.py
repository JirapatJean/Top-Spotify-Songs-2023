# %%
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat,
    lit,
    to_date,
    trim,
    split,
    when,
    size,
    sum,
    round
)

# Variable
PROJECT_ID = 'top-spotify-songs-2023-419308'
GCS_BUCKET = 'asia-southeast1-top-spotify-787969f2-bucket'
DATA_PATH = f'{GCS_BUCKET}/data'
DATASET_NAME = 'spotify-2023.csv'
BIGQUERY_DATASET_NAME = 'Top_Spotify_Songs_2023'

# Initiate Spark
spark = SparkSession \
    .builder \
    .appName("PySpark Top Spotify Songs 2023 Cleaning") \
    .getOrCreate()

df = spark.read.csv(
    f'gs://{DATA_PATH}/{DATASET_NAME}',
    header = True, 
    inferSchema = True
)

drop_duplicates_df = df.dropDuplicates()

# Trim 'track_name' and 'artist(s)_name' columns
trim_track_and_artists_name_df = drop_duplicates_df \
                                .withColumn("track_name", trim(col('track_name'))) \
                                .withColumn("artist(s)_name", trim(col('artist(s)_name')))

# Split the 'artist(s)_name' column into 'main_artist' and 'featuring_artists'
# Then, drop 'artist(s)_name' column
clean_artists_df = trim_track_and_artists_name_df.withColumn(
                    'artists', \
                    split(trim_track_and_artists_name_df['artist(s)_name'], ',', 2)
)
clean_artists_df = clean_artists_df.withColumn('main_artist', clean_artists_df['artists'][0]) \
                    .withColumn(
                        'featuring_artists',
                        when(size(clean_artists_df['artists']) > 1, clean_artists_df['artists'][1])
                        .otherwise('-')
                    ) \
                    .drop('artist(s)_name', 'artists')

# Filter out rows with dirty streams data
rows_with_dirty_streams_df = clean_artists_df.filter(col('streams').rlike("[^0-9]"))
clean_streams_df = clean_artists_df.subtract(rows_with_dirty_streams_df)

# Convert the 'streams' column to float
clean_streams_df = clean_streams_df.withColumn('streams', col('streams').cast('float'))

# Create 'streams_in_millions' column by converting 'streams' into million units
# Then, drop 'streams' column
clean_streams_df = clean_streams_df.withColumn(
    'streams_in_millions',
    round(col('streams') / 1000000, 2)
).drop('streams')

# Filter out rows with 0 streams after converting into million units
clean_streams_df = clean_streams_df.filter(col('streams_in_millions') != 0)

# Create a 'release_date' column by concatenating year, month, and day columns
# Then, drop those columns
clean_release_date = clean_streams_df.withColumn(
    'released_date',
    to_date(concat(
        'released_year', lit('-'), \
        'released_month', lit('-'), \
        'released_day'
        ),'yyyy-M-d'
    )
).drop('released_year', 'released_month', 'released_day')

# Drop columns from other platforms to include only Spotify columns
other_platforms_columns = [
    'in_apple_playlists',
    'in_apple_charts',
    'in_deezer_playlists',
    'in_deezer_charts',
    'in_shazam_charts'
    ]
spotify_only_df = clean_release_date.drop(*other_platforms_columns)

# Drop rows where 'key' column is null
clean_key_df = spotify_only_df.na.drop(subset = ['key'])

# Filter 'key' column to include only 12 possibles key according to 'Pitch class'
# Filter 'mode' column to include only Major or Minor
key_list = ['C', 'C#', 'D', 'D#', 'E', 'F', 'F#', 'G', 'G#', 'A', 'A#', 'B']
cleaned_df = clean_key_df.filter(
    (col('key').isin(key_list)) &
    (col('mode').isin(['Major', 'Minor']))
)

# Rename audio feature columns
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
    cleaned_df = cleaned_df.withColumnRenamed(old_name, new_name)

# Filter audio features columnsto include only values from 0 to 100
audio_feature_columns = [
    'danceability',
    'valence',
    'energy',
    'acousticness',
    'instrumentalness',
    'liveness',
    'speechiness'
    ]

for feature in audio_feature_columns:
    cleaned_df = cleaned_df.filter(col(feature).between(0, 100))

# Write cleaned_df to BigQuery
cleaned_df.write \
    .format('bigquery') \
    .option('table', f'{PROJECT_ID}.{BIGQUERY_DATASET_NAME}.Cleaned_Spotify_Songs_2023') \
    .option('temporaryGcsBucket', f'{DATA_PATH}/temp') \
    .mode("overwrite") \
    .save()

spark.stop()