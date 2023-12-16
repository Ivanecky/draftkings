# Import libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError 
import os
from datetime import datetime as dt
import yaml 
import re

# Function to connect to S3

def connectToS3():
    # Status
    print("Connecting to S3...")

    # Read in YAML for AWS creds
    # Specify the path to your YAML file
    yaml_fp = '/Users/samivanecky/git/draftkings/secrets.yaml'

    # Read the YAML file
    with open(yaml_fp, 'r') as file:
        creds = yaml.safe_load(file)

    # Set working directory for data
    os.chdir('/Users/samivanecky/git/draftkings/data/')

    # Define S3 resource
    s3 = boto3.resource(
        service_name='s3',
        region_name=creds['region_name'],
        aws_access_key_id=creds['aws_access_key_id'],
        aws_secret_access_key=creds['aws_secret_access_key']
    )

    # Return S3 object
    return(s3)

# Function to get NFL lines
def getEplLinks(url):

    # Set URL
    dk_base = url

    # Get the basic HTML from the page
    try:
        dk_html = requests.get(dk_base)
    except:
        print(f"error getting data for {dk_base}")

    # Get content into BS4
    dk_soup = BeautifulSoup(dk_html.content, "lxml")

    # Get html to text form
    dk_txt = str(dk_soup.get_text)

    # Pattern for URLs
    pattern = r'\"url\":\"\s*(.*?)\"\s*'

    # Extract links
    lnks = re.findall(pattern, dk_txt)

    # Keep only those that are for events
    lnks = [l for l in lnks if 'http' in l and 'event' in l]

    return(lnks)

def getEventData(url):

    # Set URL
    dk_base = url

    # Get the basic HTML from the page
    try:
        dk_html = requests.get(dk_base)
    except:
        print(f"error getting data for {dk_base}")

    # Get content into BS4
    dk_soup = BeautifulSoup(dk_html.content, "lxml")

    # Get table content
    tbls = dk_soup.find_all("table")

    # # Extract tables to pandas
    dfs = pd.read_html(str(tbls))

    # Get the first table which has game odds
    game_odds = dfs[0]

    # Extract the event name
    evnt_name = dk_soup.find(class_ = "sportsbook-breadcrumb__end").text
    evnt_name = evnt_name.replace('\xa0', '').replace('/', '')

    # Add name to event data
    game_odds['event_name'] = evnt_name

    # Add a load timestamp
    game_odds['load_ts'] = dt.now().strftime("%Y-%m-%d_%H-%M-%S")

    # Return data
    return(game_odds)

# Function to write to S3 bucket
# Read in filepath for CSV
def uploadToS3(s3, fp):
    # Print status
    print("Uploading to S3...")

    # Define s3 bucket name
    bucket_name = 'sgi-dk'
    try:
        # Upload the CSV file to S3
        s3.Bucket(bucket_name).upload_file(Filename=fp, Key=fp)
        print("Upload to S3 was successful.")
    except:
        print(f"Error uploading {fp} to S3.")

# Define main function
def main():
    # Connect to S3
    s3 = connectToS3()

    # Set url lines
    url = "https://sportsbook.draftkings.com/leagues/football/nfl"

    # Get links for games
    lnks = getNflLinks(url)

    # Iterate over links and get game dataframes
    for l in lnks:
        print(f"Getting data for {l}")
        # Get game data
        try:
            gm_df = getEventData(l)
            # Concat data for overall data frame
            try:
                all_games = pd.concat(all_games, gm_df)
            except:
                all_games = gm_df
        except:
            print(f"Issue getting data for {l}")

    # Write data to CSV
    # Get current timestamp
    current_timestamp = dt.now().strftime("%Y-%m-%d_%H-%M-%S")

    # Write data to csv with timestamp name
    all_games.to_csv(f'data_{current_timestamp}.csv')

    # Set file path
    fp = 'data_' + str(current_timestamp) + '.csv'

    # Upload to S3
    uploadToS3(s3, fp)

# Basic run block
if __name__ == "__main__":
    main()

