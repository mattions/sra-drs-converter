import os
import sys
import argparse

import pandas as pd
import requests
from pandas import DataFrame


class Processor:
    def __init__(self, verbose=False):
        self.verbose = verbose
        
    def get_drs_uri_from_sra(self, sra: str):  # function that returns drs_id based on the input SRR id
        drs_uri = ""
        r = requests.get(f"https://locate.be-md.ncbi.nlm.nih.gov/idx/v1/{sra}?submitted=true&etl=false")
        if r.status_code == 200:
            r_json = r.json()
            drs_uri = f"{r_json['drs-base']}" + "/" + f"{r_json['response'][sra]['drs']}"
            if self.verbose:
                print(f"Retrieved drs_uri: {drs_uri}")
        return drs_uri

    def get_contents_id_from_drs(self, drs: str):  # function that returns content_id based on the previous DRS id
        if drs == "":
            return ""
        r1 = requests.get(f"https://locate.be-md.ncbi.nlm.nih.gov/ga4gh/drs/v1/objects/{drs}")
        if r1.status_code == 404:
            return ""
        return r1.json()["contents"][0]["id"]
    
    def get_drs_url_from_contents_id(self, id: str):  # function that returns drs_url based on the previous content_id
        if id == "":
            return ""
        r1 = requests.get(f"https://locate.be-md.ncbi.nlm.nih.gov/ga4gh/drs/v1/objects/{id}")
        if "status_code" in r1.json():
            if r1.json()["status_code"] == 404 or r1.json()["status_code"] == 409:
                return ""
        return r1.json()["self_url"]




    def run(self, df: DataFrame) -> DataFrame:

        df["drs_uri"] = df["Run"].apply(
            lambda cell: self.get_drs_uri_from_sra(cell)
        )

        df["id"] = df["drs_uri"].apply(
            lambda cell: self.get_contents_id_from_drs(cell)
        )  # lambda function that adds a new content_id column in dataframe

        df["url"] = df["id"].apply(
            lambda cell: self.get_drs_url_from_contents_id(cell)
        )  # lambda function that adds a new drs_url column in dataframe



        # Remove the column from the end of the DataFrame and save it in a variable
        column_to_move = df.pop("drs_uri")
        # Insert the column at the beginning of the DataFrame
        df.insert(0, "drs_uri", column_to_move)

        column_to_move = df.pop("id")
        # Insert the column at the beginning of the DataFrame
        df.insert(1, "id", column_to_move)

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
    target_filename = os.path.abspath(args.sraRunTable).replace(".", "_updated.")
    df.to_csv(target_filename, index=False)
    print (f"Updated file written into {target_filename}")
