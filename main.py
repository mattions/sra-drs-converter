import os
import sys
import argparse

import pandas as pd
import requests
from pandas import DataFrame

from datetime import datetime
start_time = datetime.now()
    
class Processor:
    def __init__(self, verbose=False):
        self.verbose = verbose
        
    def get_drs_id_from_sra(self, sra: str):  # function that returns drs_id & drs_uri based on the input SRR id
        drs_id = ""
        drs_uri = ""
        r = requests.get(f"https://locate.be-md.ncbi.nlm.nih.gov/idx/v1/{sra}?submitted=true&etl=false")
        if r.status_code == 200:
            r_json = r.json()
            drs_id = r_json['response'][sra]['drs']
            drs_uri = f"{r_json['drs-base']}" + "/" + f"{r_json['response'][sra]['drs']}"
            if self.verbose:
                print(f"Retrieved drs_uri: {drs_uri}")
        return drs_id, drs_uri
    
    def split_list(self, col, ind: int):   # function that splits a list into individual items (used to separate drs_id and drs_uri into separate columns)
        col_idx = col[ind]
        return col_idx
    
    def count_offline(self, content: list):  # function to return # of offline files from contents 
        num_offline = 0
        ids = content['id']       
        # use ?expand = true to find all objects in a bundle. does it return other bundles as well or just blobs?
        r1 = requests.get(f"https://locate.be-md.ncbi.nlm.nih.gov/ga4gh/drs/v1/objects/{ids}?expand=true")     
        if r1.status_code == 409:                            #if status code is 409, drs_id must be blob and is offline
            num_offline = 1               
        return num_offline

    def get_drs_info(self, drs: str):  # function that returns content_id based on the previous DRS id
        name = ""
        is_bundle = ""
        is_online = ""
        num_offline = ""
        
        # situation 1: no drs_id returned
        if drs == "":
            return is_bundle, is_online, num_offline, name
        
        # use ?expand = true to find all objects in a bundle. does it return other bundles as well or just blobs?
        r1 = requests.get(f"https://locate.be-md.ncbi.nlm.nih.gov/ga4gh/drs/v1/objects/{drs}?expand=true")     
        if r1.status_code != 200:   
            #404 - the requested DrsObject wasn't found
            #400 - the request is malformed
            #401 - the request is unauthorized
            #403 - the requester is not authorized to perform this action
            #409 - The file has been moved offline.
            if r1.satus_code == 409:                            #if status code is 409, drs_id must be blob and is offline
                is_bundle = False
                is_online = False
                num_offline = 1

            if self.verbose == True:
                if r1.status_code == 404:
                    print("Error 404: the requested DrsObject wasn't found")
                elif r1.status_code == 401 or r1.status_code == 403:
                    print("Error 401 or 403: unauthorized request")
                elif r1.status_code == 409:
                    print("Error 409: file has been moved offline")
                else:
                    print("Unknown Error")
                    
            return is_bundle, is_online, num_offline, name
                    
        # r1.status_code == 200, file or bundle is online
        name = r1.json()['name']
        try:                             # if r1.json()["contents"] retunrs a value, then drs_id is a bundle
            r1.json()["contents"]
            is_bundle = True
        except:                          # if r1.json()["contents"] returns an error (does not exist), then drs_id is a blob and is online
            is_bundle = False
            is_online = True
        
        if is_bundle == True:  
            num_objects = len(r1.json()["contents"])
            
            # count num of online/offline blobs within a bundle.
            offline_blobs = list(map(lambda cell: self.count_offline(cell), r1.json()["contents"]))
            num_offline = sum(offline_blobs)
            if num_offline > 0:
                is_online = False
        
        return is_bundle, is_online, num_offline, name

    
    
    def run(self, df: DataFrame) -> DataFrame:

        df["drs_id_uri"] = df["Run"].apply(
            lambda cell: self.get_drs_id_from_sra(cell)
        )
        
        df["drs_id"] = df["drs_id_uri"].apply(
            lambda cell: self.split_list(cell, 0)
        )
        
        df["drs_uri"] = df["drs_id_uri"].apply(
            lambda cell: self.split_list(cell, 1)
        )
        
        df["drs_info"] = df["drs_id"].apply(
            lambda cell: self.get_drs_info(cell)
        )  # lambda function that adds a new content_id column in dataframe
        
        df["is_bundle"] = df["drs_info"].apply(
            lambda cell: self.split_list(cell, 0)
        )
        
        df["is_online"] = df["drs_info"].apply(
            lambda cell: self.split_list(cell, 1)
        )
        
        df["num_offline"] = df["drs_info"].apply(
            lambda cell: self.split_list(cell, 2)
        )
        
        df["name"] = df["drs_info"].apply(
            lambda cell: self.split_list(cell, 3)
        )
        # Remove the column from the end of the DataFrame and save it in a variable
        column_to_move = df.pop("drs_id")
        # Insert the column at the beginning of the DataFrame
        df.insert(0, "drs_id", column_to_move)
        
        column_to_move = df.pop("name")
        df.insert(0, "name", column_to_move)     

        column_to_move = df.pop("drs_uri")
        df.insert(0, "drs_uri", column_to_move)
        
        column_to_move = df.pop("is_bundle")
        df.insert(2, "is_bundle", column_to_move)
        
        column_to_move = df.pop("is_online")
        df.insert(3, "is_online", column_to_move)
        
        column_to_move = df.pop("num_offline")
        df.insert(4, "num_offline", column_to_move)

        df.drop(columns = ["drs_id_uri", "drs_info"], inplace=True)
        
        return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Retrieve DRS uri from SRA using IDX.')
    parser.add_argument('sraRunTable', help='the SRARunTable downloaded from trace SRA https://trace.ncbi.nlm.nih.gov/ . Must contains SRR run ids')
    parser.add_argument('--verbose', action="store_true", help='Runs in verbose mode')

    args = parser.parse_args()
    print("Starting DRS acquisition")
    processor = Processor(verbose=args.verbose)
    df = pd.read_csv(args.sraRunTable, dtype=str)
    df_processed = processor.run(df)
    print("Finished DRS acquisition.")
    
    target_filename = os.path.basename(args.sraRunTable).replace(".txt", "_updated.csv")
    df.to_csv(target_filename, index=False)                #production level - writes to output-files
 
    offline_blobs = sum(df['num_offline'])
    bundles = df['is_bundle'].sum()
    output_text = f"The input file had {len(df)} rows\n \
        Of those, {bundles} are bundles\n \
        A total of {offline_blobs} files in this dataset are offline\n"
    text_file = open("output_stats.txt", "w")
    n = text_file.write(output_text)
    text_file.close()
    
    print(f"A total of {offline_blobs} files in this dataset are offline")
    print (f"Updated file written into {target_filename}")
    
    end_time = datetime.now()
    print('Duration: {}'.format(end_time - start_time))
