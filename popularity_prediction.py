import os
import pandas as pd
import psycopg2
from joblib import load
from dotenv import load_dotenv
from sklearn.preprocessing import StandardScaler
import re
import numpy as np

load_dotenv()

# Load the scaler and model
scaler = load('teddyxscaler.joblib')
model = load('kmeans_model_for_ted_videos.joblib')

# Database connection details (place in a .env file for security)
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_host = os.getenv("DB_HOST")
db_password = os.getenv("DB_PASSWORD")
db_port = os.getenv("DB_PORT")

if not all([db_name, db_user, db_host, db_password, db_port]):
    raise ValueError("Missing one or more environment variables for database connection.")

# Connect to the database
connection = psycopg2.connect(
    database=db_name,
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port
)
cursor = connection.cursor()

# Function to convert ISO 8601 duration to seconds (e.g., PT1H2M3S to seconds)
def convert_duration_to_seconds(duration):
    match = re.match(
        r'PT((?P<hours>\d+)H)?((?P<minutes>\d+)M)?((?P<seconds>\d+)S)?', duration)
    if not match:
        return 0
    time_data = match.groupdict()
    hours = int(time_data['hours'] or 0)
    minutes = int(time_data['minutes'] or 0)
    seconds = int(time_data['seconds'] or 0)
    return hours * 3600 + minutes * 60 + seconds

# Fetch new data that needs to be labeled
cursor.execute("""
    SELECT fvs.video_id, fvs.view_count, fvs.like_count, fvs.comment_count, dv.duration, dv.category_id, fvs.timestamp
    FROM Fact_Video_Statistics fvs
    JOIN Dim_Video dv ON fvs.video_id = dv.video_id
    WHERE fvs.popularity_rating IS NULL;
""")
rows = cursor.fetchall()
print(f"Fetched {len(rows)} rows of data.")

# Transform data into a DataFrame
df = pd.DataFrame(rows, columns=['video_id', 'view_count', 'like_count', 'comment_count', 'duration', 'category_id', 'timestamp'])

# Ensure the 'timestamp' column is a datetime object
df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

# Calculate the mean like_count and comment_count for each timestamp (date)
df['date'] = df['timestamp'].dt.date
mean_likes = df.groupby('date')['like_count'].transform('mean')
mean_comments = df.groupby('date')['comment_count'].transform('mean')

# Calculate mean like_count and comment_count for each timestamp (date)
df['date'] = df['timestamp'].dt.date
mean_likes = df.groupby('date')['like_count'].transform('mean')
mean_comments = df.groupby('date')['comment_count'].transform('mean')

# Replace zero values with the respective daily mean using np.where
df['like_count'] = np.where(df['like_count'] == 0, mean_likes, df['like_count'])
df['comment_count'] = np.where(df['comment_count'] == 0, mean_comments, df['comment_count'])

print("Converting video durations to seconds...")
# Convert duration to seconds
df['duration_seconds'] = df['duration'].apply(convert_duration_to_seconds)

print("Creating duration bins...")
# Create duration bins with updated labels (1, 2, 3, 4)
df['duration_bin'] = pd.cut(df['duration_seconds'], bins=[0, 300, 900, float('inf')], labels=[1, 2, 3], right=False)

print("Calculating ratios and interaction scores...")
df['like_view_ratio'] = df['like_count'] / df['view_count']
df['comment_view_ratio'] = df['comment_count'] / df['view_count']
df['interaction_score'] = df['like_view_ratio'] + df['comment_view_ratio']

print("Mapping category popularity...")
category_popularity = {
    10: 1,  # Music
    23: 2,  # Comedy
    22: 1,  # People & Blogs
    24: 4,  # Entertainment
    28: 1,  # Science & Technology
    27: 6,  # Education
    25: 7,  # News & Politics
    26: 8,  # Howto & Style
    17: 8,  # Sports
    19: 9,  # Travel & Events
    2: 10,  # Autos & Vehicles
    15: 10,  # Pets & Animals
    20: 10,  # Gaming
    1: 6,   # Film & Animation
    18: 7,  # Short Movies
    21: 7,  # Videoblogging
    29: 8,  # Nonprofits & Activism
    30: 9,  # Movies
    31: 9,  # Anime/Animation
    32: 9,  # Action/Adventure
    33: 10,  # Classics
    34: 2,  # Comedy (duplicate, consider using 23)
    35: 5,  # Documentary
    36: 7,  # Drama
    37: 6,  # Family
    38: 9,  # Foreign
    39: 8,  # Horror
    40: 7,  # Sci-Fi/Fantasy
    41: 8,  # Thriller
    42: 7,  # Shorts
    43: 8,  # Shows
    44: 9   # Trailers
}


# Calculate 'category_popularity' using the dictionary
df['category_popularity'] = df['category_id'].map(category_popularity).fillna(5)
# Print out category_id and category_popularity for debugging

print("Applying scaling to relevant features...")
# Apply scaling using the loaded scaler (only for the features that need scaling)
scaled_features = scaler.transform(df[['like_view_ratio', 'comment_view_ratio', 'interaction_score', 'category_popularity']])

for index, row in df.iterrows():
    print(scaled_features)
# Replace the original values with the scaled values
df[['like_view_ratio', 'comment_view_ratio', 'interaction_score', 'category_popularity']] = scaled_features

# Include 'duration_bin' directly in the features for clustering (no scaling needed for this)
features_for_clustering = ['like_view_ratio', 'comment_view_ratio', 'interaction_score', 'duration_bin', 'category_popularity']

print("Predicting clusters using the KMeans model...")
df['predicted_cluster'] = model.predict(df[features_for_clustering])

# Map predictions to 'populair' or 'niet populair'
print("Mapping predicted clusters to popularity status...")
df['popularity_status'] = df['predicted_cluster'].map({1: 'niet populair', 0: 'populair'})

print("Updating the database with popularity predictions...")
for _, row in df.iterrows():
    cursor.execute(
        """
        UPDATE Fact_Video_Statistics
        SET popularity_rating = %s
        WHERE video_id = %s;
        """,
        (row['popularity_status'], row['video_id'])
    )
    connection.commit()

# Close the database connection
cursor.close()
connection.close()

print("Database connection closed. Script completed.")