import pandas as pd 
import numpy as np

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import json
import os
import pandas as pd
from bs4 import BeautifulSoup
from pymongo import MongoClient
from datetime import datetime
# from suppress_logging import SuppressLogging
from google.api_core.retry import Retry


with open('config.json', 'r') as json_file:
    config = json.load(json_file)

def create_firebase_client():
    cred = credentials.Certificate(config["firebase_cred"])
    app = firebase_admin.initialize_app(cred)
    
    db = firestore.client()
    
    return db, app  # Return both the Firestore client and app instance


def clean_content(html_text):
    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(html_text, 'html.parser')
    
    # Get the clean text by extracting only the text part
    clean_text = soup.get_text()
    
    return clean_text

def download_and_save_proposal(db, scan):
    print("#########Downloading intial proposals###########")
    retry_strategy = Retry()
    collection_name = 'ai_posts'
    collection_ref = db.collection(collection_name)    
    
    docs = collection_ref.order_by('created_at', direction='DESCENDING').stream(retry=retry_strategy)

    
    protocol_list = []
    docs_list = []
    for doc in docs:
        protocol = str(doc.id).split('--')[0]
        if protocol not in protocol_list:
            protocol_list.append(protocol)
        docs_list.append(doc.to_dict())
    
    # Create a single DataFrame to store all proposals
    all_proposals_df = pd.DataFrame(columns=['protocol', 'post_id', 'timestamp', 'title', 'description', "discussion_link"])
    
    for key in protocol_list:
        discourse_df = pd.DataFrame(columns=['protocol', 'post_id', 'timestamp', 'title', 'description', "discussion_link"])    
        
        for doc in docs_list: 
            try:
                if doc['post_type'] == 'snapshot_proposal':
                    df_row = []
                    if key in doc['house_id']:
                        post_id = doc['id']
                        protocol = key
                        timestamp = doc['created_at']
                        title = doc['title']
                        description = clean_content(doc['description'])
                        
                        try:
                            discussion_link = doc['post_url_link']
                        except Exception as e:
                            discussion_link = ''
                        
                        df_row = [protocol, post_id, timestamp, title, description, discussion_link]
                        
                        temp_df = pd.DataFrame([df_row], columns=discourse_df.columns)
                        
                        # with SuppressLogging():
                        discourse_df = pd.concat([discourse_df, temp_df], ignore_index=True)
            
            except Exception as e:
                # print("\n\n", doc)
                # print("\n\n", e)
                continue
        
        # Append the protocol's data to the main DataFrame
        all_proposals_df = pd.concat([all_proposals_df, discourse_df], ignore_index=True)
    
    return all_proposals_df



if __name__ == "__main__":
    db, app = create_firebase_client()
    proposals = download_and_save_proposal(db, scan=True)
    proposals.to_csv("ai_posts.csv")
    
    
    
    

    
    
    

    
    
