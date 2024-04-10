import os
import sys
import argparse
import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
import requests
from datetime import datetime
import time

BASE_URL = "locate.be-md.ncbi.nlm.nih.gov"


class Processor:
    def __init__(
        self,
        logger,
        verbose=False,
        max_retries=3,
        retry_delay=5,
        flatten_structure=False,
        etl=True,
    ):
        self.logger = logger
        self.verbose = verbose
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.flatten_structure = flatten_structure
        self.etl = etl
        self.blob_status_cache = {}  # Add a cache for blob status
        self.drs_contents_cache = {}  # Cache for DRS object contents

    def send_request_with_retry(self, url):
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    return response
                elif response.status_code in [502, 503, 504]:
                    self.logger.warning(
                        f"Server error {response.status_code} for {url}. Attempt {attempt + 1} of {self.max_retries}. Retrying in {self.retry_delay} seconds."
                    )
                    time.sleep(self.retry_delay)
                else:
                    self.logger.error(
                        f"Non-retryable response for {url}: {response.status_code}."
                    )
                    break
            except requests.exceptions.RequestException as e:
                self.logger.warning(
                    f"Request exception for {url}: {e}. Attempt {attempt + 1} of {self.max_retries}. Retrying in {self.retry_delay} seconds."
                )
                time.sleep(self.retry_delay)
        self.logger.error(
            f"Failed to get response from {url} after {self.max_retries} attempts."
        )
        return None

    def get_drs_id_from_sra(self, sra: str):
        drs_id = ""
        drs_uri = ""
        url = f"https://{BASE_URL}/idx/v1/{sra}?submitted=true&etl={str(self.etl).lower()}"
        response = self.send_request_with_retry(url)
        if response:
            r_json = response.json()
            drs_id = r_json["response"][sra]["drs"]
            drs_uri = f"{r_json['drs-base']}/{drs_id}"
            if self.verbose:
                self.logger.debug(f"Retrieved DRS URI: {drs_uri}")
        else:
            self.logger.error(f"Failed to retrieve DRS URI for SRA: {sra}")
        return drs_id, drs_uri


    def get_drs_info(self, drs_id):
        """
        Retrieves and caches DRS object information, including its online status and contents.
        """
        # Check if we already have the DRS info in cache
        if drs_id in self.drs_contents_cache:
            response_json = self.drs_contents_cache[drs_id]
        else:
            response = self.send_request_with_retry(f"https://{BASE_URL}/ga4gh/drs/v1/objects/{drs_id}?expand=true")
            if response:
                response_json = response.json()
                self.drs_contents_cache[drs_id] = response_json  # Cache the response
            else:
                self.logger.error(f"Failed to get DRS info for {drs_id}: No response after retries.")
                return False, False, 0, "", 0  # Example error return, adjust as necessary

        name = response_json.get("name", "")
        is_bundle = "contents" in response_json
        num_objects = len(response_json["contents"]) if is_bundle else 1
        num_offline = sum(not self.is_blob_online(content["id"]) for content in response_json.get("contents", [])) if is_bundle else int(not self.is_blob_online(drs_id))
        is_online = num_offline < num_objects  # Considered online if at least one blob or the standalone blob is online

        return is_bundle, is_online, num_offline, name, num_objects


    def process_online_blobs(self, drs_id, name):
        """
        Identifies online blobs from a given DRS ID and processes them based on their extensions.
        Utilizes cached DRS contents to avoid redundant network requests.

        Args:
            drs_id (str): The DRS ID of the object being processed.
            name (str): The name associated with the DRS ID.

        Returns:
            A list of processed online blobs with their modified names for storage.
        """
        online_blobs = []

        # Check if the DRS contents are in the cache
        if drs_id not in self.drs_contents_cache:
            self.logger.error(f"No cached data for DRS ID: {drs_id}. Make sure to run get_drs_info first.")
            return online_blobs

        response_json = self.drs_contents_cache[drs_id]

        # Process the cached DRS object contents
        if "contents" in response_json:  # It's a bundle
            for content in response_json["contents"]:
                # Process only blobs, not sub-bundles
                if "contents" not in content:
                    blob_id = content["id"]
                    if self.is_blob_online(blob_id):
                        blob_uri = f"drs://{BASE_URL}/{blob_id}"
                        original_name = content.get('name', '')

                        # Determine modified name based on extension
                        modified_name = self.determine_modified_name(original_name)

                        # Use modified name for flat or hierarchical structure
                        hierarchical_name = f"{name}/{modified_name}" if not self.flatten_structure else f"DRS_Import/{modified_name}"
                        online_blobs.append([blob_id, blob_uri, hierarchical_name])
        else:  # It's a standalone blob
            blob_id = response_json["id"]
            if self.is_blob_online(blob_id):
                blob_uri = f"drs://{BASE_URL}/{blob_id}"
                original_name = response_json.get('name', blob_id)

                # Determine modified name based on extension
                modified_name = self.determine_modified_name(original_name)

                # Use modified name for flat structure
                hierarchical_name = f"DRS_Import/{modified_name}" if self.flatten_structure else modified_name
                online_blobs.append([blob_id, blob_uri, hierarchical_name])

        return online_blobs

    def determine_modified_name(self, original_name):
        """
        Determines the modified name of a file based on its original name and extension.

        Args:
            original_name (str): The original name of the file.

        Returns:
            The modified name with appropriate extensions.
        """
        if original_name.endswith('.lite'):
            return original_name[:-5] + '.sralite'  # Replace '.lite' with '.sralite'
        elif '.' not in original_name:
            return original_name + '.sra'  # Add '.sra' if there is no extension
        else:
            return original_name  # Retain the original name


    def is_blob_online(self, blob_id):
        """
        Checks if a blob is online.

        Args:
            blob_id (str): The ID of the blob to check.

        Returns:
            bool: True if the blob is online, False otherwise.
        """

        # Check cache first
        if blob_id in self.blob_status_cache:
            return self.blob_status_cache[blob_id]

        self.logger.debug(f"Checking if blob {blob_id} is online.")
        for attempt in range(self.max_retries):
            try:
                response = requests.get(f"https://{BASE_URL}/ga4gh/drs/v1/objects/{blob_id}")
                self.logger.debug(f"Received status code {response.status_code} for blob {blob_id}.")
                if response.status_code == 200:
                    self.blob_status_cache[blob_id] = True
                    self.logger.debug(f"Blob {blob_id} is online. Updating cache.")
                    return True
                elif response.status_code in [502, 503, 504]:
                    self.logger.warning(f"Server error {response.status_code} for blob {blob_id}. Retrying...")
                    time.sleep(self.retry_delay)
                else:
                    self.blob_status_cache[blob_id] = False
                    self.logger.error(f"Blob {blob_id} is considered offline or inaccessible: {response.status_code}")
                    return False
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request exception for blob {blob_id}: {e}. Retrying...")
                time.sleep(self.retry_delay)
        # If all retries fail, consider the blob as offline and update the cache accordingly
        self.blob_status_cache[blob_id] = False
        self.logger.error(f"Failed to determine online status of blob {blob_id} after {self.max_retries} attempts.")
        return False

    def run(self, df):
        all_data = []
        hot_data = []

        for _, row in df.iterrows():
            sra = row["Run"]
            drs_id, drs_uri = self.get_drs_id_from_sra(sra)
            is_bundle, is_online, num_offline, name, num_objects = self.get_drs_info(
                drs_id
            )

            # Append all DRS data including original SRARunTable data
            all_row = [
                drs_id,
                drs_uri,
                name,
                is_bundle,
                is_online,
                num_offline,
                num_objects,
            ]
            all_row.extend(row.tolist())
            all_data.append(all_row)

            # Process and include the DRS ID in hot.csv if it's online
            if is_online:
                self.logger.debug(f"Processing online blobs for DRS ID {drs_id}, which is marked online.")
                online_blobs = self.process_online_blobs(drs_id, name)
                for blob in online_blobs:
                    # Append blob data including original SRARunTable data
                    hot_row = blob[:]
                    hot_row.extend(row.tolist())
                    hot_data.append(hot_row)
            if not online_blobs:
                self.logger.debug(f"No online blobs processed for DRS ID {drs_id}.")

        # Define column names for all_df and hot_df
        all_columns = [
            "drs_id",
            "drs_uri",
            "name",
            "is_bundle",
            "is_online",
            "num_offline",
            "total_objects",
        ]
        all_columns.extend(df.columns.tolist())

        hot_columns = ["drs_id", "drs_uri", "name"]
        hot_columns.extend(df.columns.tolist())

        # Convert lists to DataFrames
        all_df = pd.DataFrame(all_data, columns=all_columns)
        hot_df = pd.DataFrame(hot_data, columns=hot_columns)

        # Logging statistics
        offline_blobs = all_df["num_offline"].sum()
        total_files = all_df["total_objects"].sum()
        online_files = total_files - offline_blobs
        self.logger.info(f"Total DRS files: {total_files}")
        self.logger.info(f"Offline files (blobs): {offline_blobs}")
        self.logger.info(f"Online files (blobs): {online_files}")

        return all_df, hot_df


if __name__ == "__main__":

    def main():
        start_time = datetime.now()

        parser = argparse.ArgumentParser(
            description="Retrieve DRS uri from SRA using IDX."
        )
        parser.add_argument(
            "sraRunTable",
            help="The SRARunTable downloaded from trace SRA https://trace.ncbi.nlm.nih.gov/. Must contain SRR run ids",
        )
        parser.add_argument(
            "--verbose", action="store_true", help="Runs in verbose mode"
        )
        parser.add_argument(
            "--max_retries",
            type=int,
            default=3,
            help="Maximum number of retries for each request",
        )
        parser.add_argument(
            "--retry_delay",
            type=int,
            default=5,
            help="Delay in seconds between retries",
        )
        parser.add_argument(
            "--flatten_structure",
            action="store_true",
            help="Flatten folder structure for online blobs",
        )
        parser.add_argument(
            "--etl", 
            dest="etl", 
            action="store_true", 
            help="Include ETL files in the response (default: True)."
        )

        
        args = parser.parse_args()

        log_basename = os.path.splitext(os.path.basename(args.sraRunTable))[0]
        log_file = f"{log_basename}_log.log"
        log_formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
        handler = RotatingFileHandler(
            log_file, mode="a", maxBytes=5 * 1024 * 1024, backupCount=2, delay=False
        )
        handler.setFormatter(log_formatter)
        app_log = logging.getLogger("root")
        app_log.setLevel(logging.INFO if not args.verbose else logging.DEBUG)
        app_log.addHandler(handler)

        app_log.info("Starting DRS acquisition")

        try:
            processor = Processor(
                logger=app_log,
                verbose=args.verbose,
                max_retries=args.max_retries,
                retry_delay=args.retry_delay,
                flatten_structure=args.flatten_structure,
                etl=args.etl,
            )
            df = pd.read_csv(args.sraRunTable, dtype=str)

            df_all, df_hot = processor.run(df)

            target_filename = f"{log_basename}_updated.csv"
            df_all.to_csv(target_filename, index=False)
            app_log.info(f"Updated file written into {target_filename}")

            hot_target_filename = f"{log_basename}_hot.csv"
            df_hot.to_csv(hot_target_filename, index=False)
            app_log.info(f"Hot storage files written into {hot_target_filename}")

        except Exception as e:
            app_log.error(f"An error occurred: {e}")
            sys.exit(1)

        end_time = datetime.now()
        app_log.info(f"Duration: {end_time - start_time}")

    main()
