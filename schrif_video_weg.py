import os
import subprocess
import requests
import psycopg2
import paramiko
import datetime
import pytz
from dotenv import load_dotenv


load_dotenv()
# SSH connection details
ssh_host = os.getenv("SSH_HOST")
ssh_username = os.getenv("SSH_USERNAME")
ssh_password = os.getenv("SSH_PASSWORD")
remote_directory = os.getenv("REMOTE_DIRECTORY")

if not all([ssh_host, ssh_username, ssh_password, remote_directory]):
    raise ValueError("Missing one or more environment variables for SSH connection.")

# Database connection details
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_host = os.getenv("DB_HOST")
db_password = os.getenv("DB_PASSWORD")
db_port = os.getenv("DB_PORT")

if not all([db_name, db_user, db_host, db_password, db_port]):
    raise ValueError("Missing one or more environment variables for database connection.")

connectie = psycopg2.connect(
    database=db_name,
    user=db_user,
    host=db_host,
    password=db_password,
    port=db_port
)
cur = connectie.cursor()

# YouTube Data API key
api_key = os.getenv("YOUTUBE_API_KEY")

if not api_key:
    raise ValueError("Missing YouTube API key in environment variables")

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


# Function to create the necessary tables except Dim_Date
def create_tables():
    # Create Dim_Video table (if not already created)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Dim_Video (
            video_id VARCHAR(15) PRIMARY KEY,
            title TEXT NOT NULL,
            published_date TIMESTAMP NOT NULL,
            channel_id VARCHAR(50) NOT NULL,
            channel_name TEXT NOT NULL,
            duration TEXT NOT NULL,
            category_id INT NOT NULL,
            date_id INT REFERENCES Dim_Date(date_id),
            sentiment VARCHAR(15) DEFAULT NULL,  
        );
    """)

    # Create Dim_Category table (if not already created)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Dim_Category (
            category_id INT PRIMARY KEY,
            category TEXT NOT NULL
        );
    """)

    # Create Fact_Video_Statistics table (if not already created)
    cur.execute("""
            CREATE TABLE IF NOT EXISTS Fact_Video_Statistics (
                video_id VARCHAR(15),
                date_id INT REFERENCES Dim_Date(date_id),
                view_count INT,
                like_count INT,
                comment_count INT,
                timestamp TIMESTAMP NOT NULL,
                popularity_rating VARCHAR(15) DEFAULT NULL,  
                PRIMARY KEY (video_id, date_id)
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
        log_message("video_weg_service", "ERROR",
                    f"Failed to retrieve metadata for video ID {video_id}. Status code: {response.status_code}")
        return None


# Function to get the date_id from the Dim_Date table based on timestamp
def get_date_id_for_timestamp(timestamp):
    date = timestamp.date()
    cur.execute("""
        SELECT date_id FROM Dim_Date WHERE year = %s AND month = %s AND day = %s;
    """, (date.year, date.month, date.day))
    result = cur.fetchone()
    if result:
        return result[0]
    else:
        return None


# Function to insert or update the video metadata into the database
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

        # Get the timestamp and date_id for the Fact_Video_Statistics
        current_time_amsterdam = datetime.datetime.now(amsterdam_tz)
        date_id_fact = get_date_id_for_timestamp(current_time_amsterdam)

        # Get the date_id for Dim_Video based on the published_date
        published_datetime = datetime.datetime.strptime(published_date, '%Y-%m-%dT%H:%M:%SZ')
        date_id_video = get_date_id_for_timestamp(published_datetime)

        # Insert/Update Dim_Video with sentiment as NULL for now
        cur.execute("""
            INSERT INTO Dim_Video (video_id, title, published_date, channel_id, channel_name, duration, category_id, date_id, sentiment)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NULL)  -- Sentiment is NULL
            ON CONFLICT (video_id) DO UPDATE
            SET title = EXCLUDED.title,
                published_date = EXCLUDED.published_date,
                channel_id = EXCLUDED.channel_id,
                channel_name = EXCLUDED.channel_name,
                duration = EXCLUDED.duration,
                category_id = EXCLUDED.category_id,
                date_id = EXCLUDED.date_id,
                sentiment = EXCLUDED.sentiment; 
        """, (video_id, snippet['title'], snippet['publishedAt'], snippet['channelId'], snippet['channelTitle'],
              metadata['contentDetails']['duration'], category_id, date_id_video))

        # Continue with Fact_Video_Statistics as usual, without sentiment
        cur.execute("""
            SELECT COUNT(*) FROM Fact_Video_Statistics
            WHERE video_id = %s AND date_id = %s;
        """, (video_id, date_id_fact))

        exists = cur.fetchone()[0] > 0

        if exists:
            # Update the existing record
            cur.execute("""
                       UPDATE Fact_Video_Statistics
                       SET view_count = %s, like_count = %s, comment_count = %s, timestamp = %s
                       WHERE video_id = %s AND date_id = %s;
                   """, (stats.get('viewCount', 0), stats.get('likeCount', 0), stats.get('commentCount', 0),
                         current_time_amsterdam, video_id, date_id_fact))
        else:
            # Insert new record
            cur.execute("""
                      INSERT INTO Fact_Video_Statistics (video_id, view_count, like_count, comment_count, timestamp, popularity_rating, date_id)
                      VALUES (%s, %s, %s, %s, %s, NULL, %s);
                  """, (video_id, stats.get('viewCount', 0), stats.get('likeCount', 0), stats.get('commentCount', 0),
                        current_time_amsterdam, date_id_fact))

        connectie.commit()
        log_message("video_weg_service", "INFO", f"Successfully inserted/updated metadata for video ID {video_id}")

    except Exception as e:
        log_message("video_weg_service", "ERROR",
                    f"Failed to insert/update metadata for video ID {metadata['id']}: {e}")
        connectie.rollback()



# Function to fetch video files from the remote directory via SSH
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
    create_tables()  # Create the tables except Dim_Date
    video_files = fetch_video_files_via_ssh()
    extract_metadata_from_videos(video_files)
    log_message("video_weg_service", "INFO", "Database updated with video metadata.")
    print("schrif_video_weg_done")
    try:
        subprocess.run(['python', 'popularity_prediction.py'], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running popularity prediction: {e}")


if __name__ == "__main__":
    main()