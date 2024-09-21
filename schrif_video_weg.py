import os
import requests
import psycopg2
import paramiko
import datetime
import pytz  # Import for time zone handling

# SSH connection details
ssh_host = '145.97.16.170'
ssh_username = 's1149010'
ssh_password = 's1149010'
remote_directory = '/data/video'

# Database connection details
connectie = psycopg2.connect(
    database=os.getenv("DB_NAME", "indatad_s1149010"),
    user=os.getenv("DB_USER", "s1149010"),
    host=os.getenv("DB_HOST", "95.217.3.61"),
    password=os.getenv("DB_PASSWORD", "s1149010"),
    port=os.getenv("DB_PORT", "5432")
)
cur = connectie.cursor()

# YouTube Data API key
api_key = 'AIzaSyBlqseSusm09cKpg8SHXZZFy6ou1UQ_wec'

# Categories mapping
categories = {
    1: "Film & Animation", 2: "Autos & Vehicles", 10: "Music", 15: "Pets & Animals", 17: "Sports",
    18: "Short Movies", 19: "Travel & Events", 20: "Gaming", 21: "Videoblogging", 22: "People & Blogs",
    23: "Comedy", 24: "Entertainment", 25: "News & Politics", 26: "Howto & Style", 27: "Education",
    28: "Science & Technology", 29: "Nonprofits & Activism", 30: "Movies", 31: "Anime/Animation",
    32: "Action/Adventure", 33: "Classics", 34: "Comedy", 35: "Documentary", 36: "Drama", 37: "Family",
    38: "Foreign", 39: "Horror", 40: "Sci-Fi/Fantasy", 41: "Thriller", 42: "Shorts", 43: "Shows",
    44: "Trailers"
}

# Set Amsterdam time zone
amsterdam_tz = pytz.timezone('Europe/Amsterdam')

def create_log_table():
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Log (
            log_id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            service_name VARCHAR(50),
            log_level VARCHAR(10),
            message TEXT
        );
    """)
    connectie.commit()

# Log message function
def log_message(service_name, log_level, message):
    cur.execute("""
        INSERT INTO Log (service_name, log_level, message)
        VALUES (%s, %s, %s);
    """, (service_name, log_level, message))
    connectie.commit()


# Function to create the necessary tables
def create_tables():
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Dim_Video (
            video_id VARCHAR(15) PRIMARY KEY,
            title TEXT NOT NULL,
            published_date TIMESTAMP NOT NULL,
            channel_id VARCHAR(50) NOT NULL,
            channel_name TEXT NOT NULL,
            duration TEXT NOT NULL,
            category_id INT NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS Dim_Category (
            category_id INT PRIMARY KEY,
            category TEXT NOT NULL
        );
    """)

    # Create a Dim_Date table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Dim_Date (
            date_id SERIAL PRIMARY KEY,
            video_id VARCHAR(15) REFERENCES Dim_Video(video_id),
            year INT NOT NULL,
            month INT NOT NULL,
            week INT NOT NULL,
            day INT NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS Fact_Video_Statistics (
            video_id VARCHAR(15) PRIMARY KEY,
            date_id INT REFERENCES Dim_Date(date_id),
            view_count INT,
            like_count INT,
            comment_count INT,
            timestamp TIMESTAMP NOT NULL,
            popularity_rating FLOAT DEFAULT NULL,
            sentiment VARCHAR(255) DEFAULT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS Dim_Transcript (
            transcript_id SERIAL PRIMARY KEY,
            video_id VARCHAR(15) REFERENCES Dim_Video(video_id),
            transcript_text TEXT NOT NULL,
            language VARCHAR(5) NOT NULL,
            version VARCHAR(50) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    connectie.commit()


# Function to get metadata for a single video
def get_video_metadata(video_id):
    url = f"https://www.googleapis.com/youtube/v3/videos?id={video_id}&part=snippet,contentDetails,statistics&key={api_key}"
    response = requests.get(url)

    if response.status_code == 200:
        log_message("video_weg_service", "INFO", f"Successfully retrieved metadata for video ID {video_id}")
        return response.json()
    else:
        log_message("video_weg_service", "ERROR", f"Failed to retrieve metadata for video ID {video_id}. Status code: {response.status_code}")
        return None


# Function to insert or update the video metadata into the database
def upsert_video_metadata(metadata):
    try:
        video_id = metadata['id']
        snippet = metadata['snippet']
        stats = metadata.get('statistics', {})
        category_id = int(snippet['categoryId'])
        published_date = snippet['publishedAt']

        # Insert/Update Dim_Category
        category = categories.get(category_id, "Unknown")
        cur.execute("""
            INSERT INTO Dim_Category (category_id, category)
            VALUES (%s, %s)
            ON CONFLICT (category_id) DO UPDATE
            SET category = EXCLUDED.category;
        """, (category_id, category))

        # Insert/Update Dim_Video
        cur.execute("""
            INSERT INTO Dim_Video (video_id, title, published_date, channel_id, channel_name, duration, category_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (video_id) DO UPDATE
            SET title = EXCLUDED.title,
                published_date = EXCLUDED.published_date,
                channel_id = EXCLUDED.channel_id,
                channel_name = EXCLUDED.channel_name,
                duration = EXCLUDED.duration,
                category_id = EXCLUDED.category_id;
        """, (video_id, snippet['title'], snippet['publishedAt'], snippet['channelId'], snippet['channelTitle'],
              metadata['contentDetails']['duration'], category_id))

        # Get current time in Amsterdam timezone for the `timestamp` column
        current_time_amsterdam = datetime.datetime.now(amsterdam_tz)

        # Parse the published date into year, month, week, and day
        parsed_date = datetime.datetime.strptime(published_date, '%Y-%m-%dT%H:%M:%SZ')
        year = parsed_date.year
        month = parsed_date.month
        week = parsed_date.isocalendar()[1]  # Get the ISO week number
        day = parsed_date.day

        # Insert into Dim_Date table and retrieve date_id
        cur.execute("""
            INSERT INTO Dim_Date (video_id, year, month, week, day)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING date_id;
        """, (video_id, year, month, week, day))
        date_id = cur.fetchone()[0]

        # Insert/Update Fact_Video_Statistics
        cur.execute("""
               INSERT INTO Fact_Video_Statistics (video_id, view_count, like_count, comment_count, timestamp, popularity_rating, sentiment)
               VALUES (%s, %s, %s, %s, %s, NULL, NULL)
               ON CONFLICT (video_id) DO UPDATE
               SET view_count = EXCLUDED.view_count,
                   like_count = EXCLUDED.like_count,
                   comment_count = EXCLUDED.comment_count,
                   timestamp = EXCLUDED.timestamp;
           """, (video_id, stats.get('viewCount', 0), stats.get('likeCount', 0), stats.get('commentCount', 0),
                 current_time_amsterdam))

        connectie.commit()
        log_message("video_weg_service", "INFO", f"Successfully inserted/updated metadata for video ID {video_id}")

    except Exception as e:
        log_message("video_weg_service", "ERROR", f"Failed to insert/update metadata for video ID {metadata['id']}: {e}")

# Function to fetch video files from the remote directory via SSH
# Fetch video files via SSH
def fetch_video_files_via_ssh():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ssh_host, username=ssh_username, password=ssh_password)

    sftp = ssh.open_sftp()
    try:
        video_files = sftp.listdir(remote_directory)
        return video_files
    finally:
        sftp.close()
        ssh.close()



# Function to extract metadata from video IDs in the remote directory
def extract_metadata_from_videos(video_files):
    for video_file in video_files:
        video_id = os.path.splitext(video_file)[0]  # Remove file extension
        print(f"Extracting metadata for video ID: {video_id}")

        metadata = get_video_metadata(video_id)

        if metadata and 'items' in metadata and len(metadata['items']) > 0:
            upsert_video_metadata(metadata['items'][0])
            connectie.commit()
        else:
            log_message("video_weg_service", "ERROR", f"No metadata found for video ID: {video_id}")



# Main function to run the script
def main():
    create_log_table()
    create_tables()  # Create the tables if they don't exist
    video_files = fetch_video_files_via_ssh()
    extract_metadata_from_videos(video_files)
    log_message("video_weg_service", "INFO", "Database updated with video metadata.")

if __name__ == "__main__":
    main()
