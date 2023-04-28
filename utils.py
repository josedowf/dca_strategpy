import json


# read credentials from creds.json file
def read_creds(filename):
    # Read JSON file to load credentials.
    # Store API credentials in a safe place.
    # If you use Git, make sure to add the file to .gitignore
    with open(filename) as f:
        creds = json.load(f)
    return creds
