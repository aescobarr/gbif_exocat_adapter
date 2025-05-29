from prefect import flow, task
from database import DataBaseFront
from datetime import datetime
import os
import logging
import settings
import csv

folder_name = None
folder_path = None
logger_python = logging.getLogger(__name__)

@task
def pre_setup():    
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)    

@task
def test_log():
    logging.basicConfig(filename=folder_name + '/myapp.log', level=logging.INFO, force=True)
    logger_python.info("************************")
    logger_python.info("folder name {0}".format(folder_name))
    logger_python.info("folder path {0}".format(folder_path))
    logger_python.info("INFO level log message.")
    logger_python.debug("DEBUG level log message.")
    logger_python.error("ERROR level log message.")
    logger_python.critical("CRITICAL level log message.")

@task
def create_species_list():
    filename = folder_name + '/species_list.csv'
    logger_python.info("*********************")
    logger_python.info("Loading species list")
    database = DataBaseFront(settings,logger_python)
    sp = database.load_especies_invasores()
    fields_to_write = ["id", "name"]
    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields_to_write, extrasaction='ignore')
        writer.writeheader()  # Write the header
        writer.writerows(sp)  # Write the rows
    logger_python.info("File created, {0} rows".format(len(sp)))
    return filename

@task
def create_reverse_cached_taxon_resolution_file():
    filename = folder_name + '/cached_taxon_resolution_results.csv'
    logger_python.info("*********************")
    logger_python.info("Creating resolution file")
    database = DataBaseFront(settings, logger_python)
    data = database.load_reverse_taxon_resolution_data()
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerows(data)
    logger_python.info("File created, {0} rows".format(len(data)))
    return filename

@task
def read_gbif_taxon_keys():
    filename = folder_name + '/species_list.csv'
    logger_python.info("*********************")
    logger_python.info("Reading taxon keys from species file")
    first_column = None
    with open(filename, "r") as f:
        reader = csv.reader(f)
        header = next(reader)  # Skip the header if present
        first_column = [str(row[0]) for row in reader]
    logger_python.info("Retrieved {0} values".format(len(first_column)))
    return first_column

@task
def create_blocks(all_gbif_taxon_keys, block_size):
    logger_python.info("*********************")
    logger_python.info("Creating taxon_key blocks for download, size {0}".format(block_size))
    blocks = []
    for i in range(0, len(all_gbif_taxon_keys), block_size):
        blocks.append(all_gbif_taxon_keys[i:i + block_size])
    logger_python.info("Created {0} blocks".format(len(blocks)))
    return blocks
    

@flow
def download_gbif_data():
    pre_setup()
    # test_log()
    species_file = create_species_list()
    species_resolution_cache = create_reverse_cached_taxon_resolution_file()
    all_gbif_taxon_keys = read_gbif_taxon_keys()
    blocks = create_blocks(all_gbif_taxon_keys,300)


if __name__ == "__main__":
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    folder_name = f"flow_{timestamp}"
    folder_path = os.path.join(os.getcwd(), folder_name)
    download_gbif_data()
