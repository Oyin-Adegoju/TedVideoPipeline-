import os
import psycopg2
import paramiko
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.formatters import TextFormatter
import joblib  # For loading the models
import re  # For cleaning the text
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection details
connectie = psycopg2.connect(
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    host=os.getenv("DB_HOST"),
    password=os.getenv("DB_PASSWORD"),
    port=os.getenv("DB_PORT")
)
cur = connectie.cursor()

# SSH connection details
ssh_host = os.getenv("SSH_HOST")
ssh_username = os.getenv("SSH_USERNAME")
ssh_password = os.getenv("SSH_PASSWORD")
remote_directory = os.getenv("REMOTE_DIRECTORY")

if not all([ssh_host, ssh_username, ssh_password, remote_directory]):
    raise ValueError("Missing one or more environment variables for SSH connection.")

# Load the NLP and classification models
nlp_model = joblib.load('nlp_model.pkl')  # Bag of Words model
classificatie_model = joblib.load('classificatie_model.pkl')  # Naive Bayes model

# Function to check if a video transcript already exists in the database
def transcript_exists(video_id):
    cur.execute("SELECT COUNT(*) FROM Dim_Transcript WHERE video_id = %s;", (video_id,))
    result = cur.fetchone()
    return result[0] > 0  # Returns True if transcript exists, False otherwise

# Function to insert transcripts into the database
def insert_transcript(video_id, transcript_text, language, version):
    if transcript_exists(video_id):
        print(f"Transcript for video {video_id} already exists in the database. Skipping...")
        return  # Skip if the transcript already exists

    try:
        cur.execute("""
            INSERT INTO Dim_Transcript (video_id, transcript_text, language, version)
            VALUES (%s, %s, %s, %s);
        """, (video_id, transcript_text, language, version))
        connectie.commit()
        print(f"Transcript for video {video_id} successfully inserted into the database.")
    except Exception as e:
        print(f"Error inserting transcript for video {video_id}: {e}")

# Function to clean and prepare the transcript text
def clean_transcript(text):
    # Convert to lowercase
    text = text.lower()

    # Remove special characters, numbers, and extra spaces
    text = re.sub(r'[^a-z\s]', '', text)  # Keep only lowercase letters and spaces

    # Remove extra spaces
    text = re.sub(r'\s+', ' ', text).strip()

    return text

# Function to clean up and classify transcripts, and update sentiment in Dim_Video
# Function to check if sentiment is already filled in Dim_Video
def sentiment_exists(video_id):
    cur.execute("SELECT sentiment FROM Dim_Video WHERE video_id = %s;", (video_id,))
    result = cur.fetchone()
    return result and result[0] is not None  # Returns True if sentiment is not NULL

# Function to clean up and classify transcripts, and update sentiment in Dim_Video
def process_transcript(video_id, transcript_text):
    try:
        # Check if the sentiment is already filled in Dim_Video
        if sentiment_exists(video_id):
            print(f"Sentiment for video {video_id} already exists. Skipping sentiment update...")
            return

        # Clean the transcript text
        cleaned_transcript = clean_transcript(transcript_text)

        # Use the NLP model to transform the text into numerical features
        X_transformed = nlp_model.transform([cleaned_transcript])  # Wrap in a list

        # Predict the sentiment using the classification model
        sentiment_prediction = classificatie_model.predict(X_transformed)

        # Convert the prediction to a label ('positief' or 'negatief')
        sentiment = 'positief' if sentiment_prediction[0] == 1 else 'negatief'

        # Update the sentiment column in Dim_Video where sentiment is NULL
        cur.execute("""
            UPDATE Dim_Video
            SET sentiment = %s
            WHERE video_id = %s AND sentiment IS NULL;
        """, (sentiment, video_id))
        connectie.commit()

        print(f"Sentiment for video {video_id} successfully updated to '{sentiment}'.")
    except Exception as e:
        print(f"Error processing transcript for video {video_id}: {e}")
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

# Function to extract video IDs from filenames (assuming filenames represent video IDs)
def extract_video_ids(video_files):
    video_ids = [os.path.splitext(video_file)[0] for video_file in video_files]  # Strip file extensions
    return video_ids

# Function to fetch and insert transcripts into the database, and update sentiment
def fetch_transcripts_and_update_sentiment():
    # Step 1: Fetch video files via SSH
    video_files = fetch_video_files_via_ssh()

    # Step 2: Extract video IDs from the filenames
    video_ids = extract_video_ids(video_files)

    # Step 3: Fetch and insert transcripts into the database
    for video_id in video_ids:
        try:
            # Fetch the transcript for the given video ID
            transcript = YouTubeTranscriptApi.get_transcript(video_id, languages=['en'])

            # Format the transcript to plain text
            formatter = TextFormatter()
            transcript_text = formatter.format_transcript(transcript)

            # Insert the transcript into the database
            insert_transcript(video_id, transcript_text, "en", "manual")

            # Process the transcript to classify sentiment and update Dim_Video
            process_transcript(video_id, transcript_text)

        except Exception as e:
            print(f"Cannot fetch transcript for video {video_id}: {e}")

# Main function to run the script
def main():
    fetch_transcripts_and_update_sentiment()

if __name__ == "__main__":
    main()
