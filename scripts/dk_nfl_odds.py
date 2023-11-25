# Import libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError 
import os
from datetime import datetime as dt
import yaml 

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
def getNflLines(url):

    # Print status
    print("Getting NFL odds...")

    # Set URL
    dk_base = url

    # Get the basic HTML from the page
    try:
        dk_html = requests.get(dk_base)
    except:
        print(f"error getting data for {dk_base}")

    # Get content into BS4
    dk_soup = BeautifulSoup(dk_html.content, "html.parser")

    # Get the body of the page
    dk_body = dk_soup.find(class_ = "sportsbook-wrapper__body")

    # Get tables
    tbls = dk_body.find_all("table")

    # Read into pandas
    dfs = pd.read_html(str(tbls))

    # Iterate over tables to keep only the necessary ones
    for i in range(0, len(dfs)):
        # Extract dataframe
        tmp_df = dfs[i]
        
        # Check number of columns
        if tmp_df.shape[1] > 3:

            # Extract the date from the column name
            line_dt = tmp_df.columns[0]

            # Rename columns using the dictionary
            tmp_df = tmp_df.set_axis(['team', 'spread', 'total', 'moneyline'], axis=1)

            # Assign date field
            tmp_df['line_dt'] = line_dt
            
            # Bind data to other dataframes
            try:
                res_df = pd.concat([res_df, tmp_df])
            except:
                res_df = tmp_df

        # Skip if wrong data format
        else:
            pass

    # Get current timestamp
    current_timestamp = dt.now().strftime("%Y-%m-%d_%H-%M-%S")

    # Add load date & timestamp to data
    res_df['load_ts'] = current_timestamp

    # Write data to csv with timestamp name
    res_df.to_csv(f'data_{current_timestamp}.csv')

    # Return local filepath for writing to S3
    return(f'data_{current_timestamp}.csv')

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

    # Get NFL betting lines from DK and write to CSV
    # Returns file path for writing to S3
    fp = getNflLines(url)

    # Upload to S3
    uploadToS3(s3, fp)

# Basic run block
if __name__ == "__main__":
    main()

