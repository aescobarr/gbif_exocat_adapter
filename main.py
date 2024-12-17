from time import sleep
import os
from pygbif import occurrences as occ
from pygbif import species
import csv
import requests
import settings
from database import DataBaseFront
import logging
from datetime import datetime

SIMPLER_POL = "POLYGON ((0.41748 40.446947, 1.043701 40.697299, 1.07666 41.004775, 2.219238 41.261291, 3.284912 41.836828, 3.240967 42.544987, 0.516357 42.932296, 0.527344 42.024814, 0.076904 40.971604, 0.131836 40.555548, 0.41748 40.446947))"
logger = logging.getLogger(__name__)


def config_logging(folder_path):
    logging.basicConfig(
        filename=os.path.join(folder_path, 'session.log'),
        level=logging.INFO,
        force=True,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logger.info("*********************")
    logger.info(f"Log ready")


def pre_setup(folder_path, folder_name):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        #logger.info(f"Folder '{folder_name}' created successfully.")
    else:
        pass
        #logger.info(f"Folder '{folder_name}' already exists.")
    return folder_name


def create_species_file():
    logger.info("*********************")
    logger.info("Creating species file")
    database = DataBaseFront(settings)
    sp = database.load_especies_invasores()
    fields_to_write = ["id", "name"]
    with open("species_list.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields_to_write, extrasaction='ignore')
        writer.writeheader()  # Write the header
        writer.writerows(sp)  # Write the rows
    logger.info("File created, {0} rows".format(len(sp)))


def read_gbif_taxon_keys():
    logger.info("*********************")
    logger.info("Reading taxon keys from species file")
    first_column = None
    with open("species_list.csv", "r") as f:
        reader = csv.reader(f)
        header = next(reader)  # Skip the header if present
        first_column = [str(row[0]) for row in reader]
    logger.info("Retrieved {0} values".format(len(first_column)))
    return first_column


def download_file(url, filename, max_retries=3, retry_delay=10):
    """
  Downloads a file from the given URL with retries.

  Args:
    url: The URL of the file to download.
    filename: The desired filename for the downloaded file.
    max_retries: The maximum number of retries to attempt.
    retry_delay: The number of seconds to wait between retries.

  Returns:
    True if the download was successful, False otherwise.
  """
    logging.info("Downloading file {0} to {1}".format(url,filename))
    for attempt in range(max_retries + 1):
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()  # Raise an exception for bad status codes

            logging.info("File downloaded, writing")
            with open(filename, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
            return True

        except requests.exceptions.RequestException as e:
            if attempt < max_retries:
                logging.info(
                    f"Download failed on attempt {attempt + 1}/{max_retries}. Retrying in {retry_delay} seconds. Error: {e}")
                sleep(retry_delay)
            else:
                logging.info(f"Download failed after {max_retries} retries. Error: {e}")
                return False


def download_all_data(download_keys, download_folder):
    logger.info("*********************")
    logger.info("Starting download ...")
    internal_log = []
    for key in download_keys:
        download_link = "https://api.gbif.org/v1/occurrence/download/request/{0}.zip".format(key)
        internal_log.append(download_file(download_link, os.path.join(download_folder, "{0}.zip".format(key)) ) )
    return len(internal_log) == len(download_keys) and all(internal_log)



def check_download_availability(download_keys):
    results = []
    for key in download_keys:
        logger.info("Checking download key {0}".format(key))
        meta = occ.download_meta(key=key)
        if meta['status'] == 'SUCCEEDED':
            logger.info("Download key {0} is available".format(key))
            results.append(True)
        else:
            logger.info("Download key {0} is NOT available".format(key))
    return len(results) == len(download_keys) and all(results)


def create_blocks(all_gbif_taxon_keys, block_size):
    logger.info("*********************")
    logger.info("Creating taxon_key blocks for download, size {0}".format(block_size))
    blocks = []
    for i in range(0, len(all_gbif_taxon_keys), block_size):
        blocks.append(all_gbif_taxon_keys[i:i + block_size])
    logger.info("Created {0} blocks".format(len(blocks)))
    return blocks

def main():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    folder_name = f"download_{timestamp}"
    folder_path = os.path.join(os.getcwd(), folder_name)
    pre_setup( folder_path, folder_name )
    config_logging(folder_path)
    create_species_file()

    all_gbif_taxon_keys = read_gbif_taxon_keys()
    blocks = create_blocks(all_gbif_taxon_keys,300)

    download_keys = {}

    logger.info("*********************")
    logger.info("Setting up download blocks...")
    for species_block in blocks:
        query = {
            "type": "and",
            "predicates": [
                {"type": "in", "key": "TAXON_KEY", "values": species_block},
                {"type": "equals", "key": "HAS_COORDINATE", "value": "true"},
                {"type": "equals", "key": "COUNTRY", "value": "ES"},
                {"type": "isNotNull", "parameter": "EVENT_DATE"},
                {
                    "type": "or",
                    "predicates": [
                        {
                            "type": "not",
                            "predicate": {"type": "equals", "key": "ESTABLISHMENT_MEANS", "value": "MANAGED"}
                        },
                        {
                            "type": "not",
                            "predicate": {"type": "isNotNull", "parameter": "ESTABLISHMENT_MEANS"}
                        }
                    ]
                },
                {
                    "type": "or",
                    "predicates": [
                        {
                            "type": "not",
                            "predicate": {"type": "equals", "key": "BASIS_OF_RECORD", "value": "LIVING_SPECIMEN"}
                        },
                        {
                            "type": "not",
                            "predicate": {"type": "isNotNull", "parameter": "BASIS_OF_RECORD"}
                        }
                    ]
                },
                {
                    "type": "or",
                    "predicates": [
                        {
                            "type": "not",
                            "predicate": {"type": "equals", "key": "BASIS_OF_RECORD", "value": "FOSSIL_SPECIMEN"}
                        },
                        {
                            "type": "not",
                            "predicate": {"type": "isNotNull", "parameter": "BASIS_OF_RECORD"}
                        }
                    ]
                },
                {
                    "type": "not",
                    "predicate": {"type": "equals", "key": "DATASET_KEY",
                                  "value": "50c9509d-22c7-4a22-a47d-8c48425ef4a7"}
                },
                {
                    "type": "not",
                    "predicate": {"type": "equals", "key": "DATASET_KEY",
                                  "value": "47cac062-7224-4570-8141-c822320627fd"}
                },
                {
                    "type": "not",
                    "predicate": {"type": "equals", "key": "DATASET_KEY",
                                  "value": "14d5676a-2c54-4f94-9023-1e8dcd822aa0"}
                },
                {
                    "type": "not",
                    "predicate": {"type": "equals", "key": "DATASET_KEY",
                                  "value": "7a3679ef-5582-4aaa-81f0-8c2545cafc81"}
                },
                {
                    "type": "within",
                    "geometry": SIMPLER_POL
                }
            ]
        }

        download_key = occ.download(query, user=settings.gbif_user, pwd=settings.gbif_pwd, email=settings.gbif_email)
        # # "('0036262-241126133413365', {'creator': 'a.escobar', 'notification_address': ['a.escobar@creaf.uab.cat'], 'sendNotification': True, 'predicate': {'type': 'and', 'predicates': [{'type': 'in', 'key': 'TAXON_KEY', 'values': ['3241387', '2227670', '2227000', '2685796', '5712056', '2225646', '7062200', '5207400', '2627503', '8215487', '2226990', '5207399', '5204019', '2337607', '2367919', '4286942', '2443002', '2350570', '3109086', '6157026', '2287072', '4286975', '2227300', '2433652']}, {'type': 'equals', 'key': 'HAS_COORDINATE', 'value': 'true'}, {'type': 'and', 'predicates': [{'type': 'not', 'predicate': {'type': 'equals', 'key': 'ESTABLISHMENT_MEANS', 'value': 'MANAGED'}}, {'type': 'not', 'predicate': {'type': 'isNotNull', 'parameter': 'ESTABLISHMENT_MEANS'}}]}, {'type': 'and', 'predicates': [{'type': 'not', 'predicate': {'type': 'equals', 'key': 'BASIS_OF_RECORD', 'value': 'LIVING_SPECIMEN'}}, {'type': 'not', 'predicate': {'type': 'isNotNull', 'parameter': 'BASIS_OF_RECORD'}}]}, {'type': 'and', 'predicates': [{'type': 'not', 'predicate': {'type': 'equals', 'key': 'BASIS_OF_RECORD', 'value': 'FOSSIL_SPECIMEN'}}, {'type': 'not', 'predicate': {'type': 'isNotNull', 'parameter': 'BASIS_OF_RECORD'}}]}, {'type': 'not', 'predicate': {'type': 'equals', 'key': 'DATASET_KEY', 'value': '50c9509d-22c7-4a22-a47d-8c48425ef4a7'}}, {'type': 'not', 'predicate': {'type': 'equals', 'key': 'DATASET_KEY', 'value': '47cac062-7224-4570-8141-c822320627fd'}}, {'type': 'not', 'predicate': {'type': 'equals', 'key': 'DATASET_KEY', 'value': '14d5676a-2c54-4f94-9023-1e8dcd822aa0'}}, {'type': 'not', 'predicate': {'type': 'equals', 'key': 'DATASET_KEY', 'value': '7a3679ef-5582-4aaa-81f0-8c2545cafc81'}}, {'type': 'within', 'geometry': 'POLYGON ((0.41748 40.446947, 1.043701 40.697299, 1.07666 41.004775, 2.219238 41.261291, 3.284912 41.836828, 3.240967 42.544987, 0.516357 42.932296, 0.527344 42.024814, 0.076904 40.971604, 0.131836 40.555548, 0.41748 40.446947))'}]}, 'format': 'SIMPLE_CSV'})"
        logger.info("Successfully obtained download key")
        logger.info("https://api.gbif.org/v1/occurrence/download/request/{0}.zip".format(download_key[0]))
        # status = occ.download_meta(download_key)
        # {'key': '0036571-241126133413365', 'doi': '10.15468/dl.hvkbbe', 'license': 'http://creativecommons.org/licenses/by-nc/4.0/legalcode', 'request': {'predicate': {'type': 'and', 'predicates': [{'type': 'in', 'key': 'TAXON_KEY', 'values': ['2443002', '2286732', '3084841', '7628502', '3189286', '3084187', '3241387', '3106738', '2775496', '3172548', '5397989', '5959143', '2479888', '2479893', '5228080', '2498384', '2480989', '2498344', '2498343', '2498393', '9752149', '2481104', '9476062', '9647688', '2498009', '2493633', '5420901', '4416161', '6100979', '2494303', '6100984', '6100996', '2489108', '6091856', '10968431', '2363097', '2394563', '5207399', '5207400', '2407845', '3189837', '3120275', '2434552', '7571130', '6894812', '1314429', '1748922', '1749886', '2070969', '11015342', '5231210', '3120284', '3120032', '3120333', '2985682', '2985683', '3189815', '2854755', '2767031', '2975132', '5188945', '5890148', '1030510', '5180091', '5200468', '4354950', '8331425', '2479106', '2766846', '2766962', '2766926', '2766502', '3085368', '9680062', '2264711', '4362895', '5109931', '5959231', '5844936', '2972983', '3647605', '2857697', '2857849', '2856504', '2856261', '2855999', '7668251', '2856457', '2330172', '2286069', '1029160', '2287072', '2502879', '2495165', '2856348', '2855823', '2856681', '8956987', '2777537', '9527904', '2777789', '2777514', '5190300', '5190298', '4562197', '7358948', '4449461', '5329889', '2777592', '2777626', '7445409', '2777724', '11488220', '3084958', '3084923', '3084949', '3933435', '2988222', '2988188', '2291253', '5200696', '5193449', '5406789', '7905507', '5548040', '6109611', '5384390', '5384396', '5384364', '9080185', '5384332', '2331573', '2285760', '2227000', '2226990', '4409643', '5384393', '5384359', '6109534', '5384401', '5384397', '8079858', '100008716', '5548391', '3188711', '9591826', '7225693', '5185730', '2330609', '7992320', '2504515', '5384334', '8002952', '3110686', '3110580', '8062584', '5831382', '3033261', '5371746', '2264530', '5727914', '2332062', '1882965', '2946427', '5371894', '8185812', '3703555', '3170442', '3114986', '3589175', '3041022', '8042568', '8373355', '3120677', '3121623', '3121581', '3120790', '2227300', '4343706', '2291334', '4417615', '3170241', '2768367', '2768885', '2768686', '2926055', '2768763', '3151558', '3151764', '5344663', '2769937', '4372686', '4417764', '2331954', '6531560', '10928934', '2876607', '5357407', '3151755', '5345603', '3083761', '3083680', '3083667', '2705290', '2970585', '4659890', '3188716', '3054110', '3033077', '7282135', '4154050', '2705924', '2650107', '3129663', '3129643', '7984279', '2953864', '3981543', '7300333', '2988224', '2141617', '4427567', '4486297', '2007665', '2012126', '5048768', '8376275', '2874718', '7870997', '7646641', '5567694', '5646095', '3053522', '5391802', '5391845', '5289670', '1873890', '2502810', '7068845', '5392174', '5391882', '5371870', '2704103', '5677204', '7861320', '2705857', '4451122', '2493654', '5371869', '2704120', '5384862', '3083508', '7323035', '3042799', '3042636', '3042658', '7903057', '3042624', '7262136', '2703723', '2703670', '2703717', '4433030', '2009189', '6306650', '5341758', '3173338', '3050364', '3034554', '3034558', '5391480', '2683936', '3042416', '9073641', '3042439', '5410411', '3172549', '4094436', '5281901', '3189935', '3138287', '3703606', '5304927', '2076260', '2007322', '9442731', '5361880', '3690997', '3189939', '2875818', '3084842', '7445032', '2957408', '2891932', '5284698', '5284702', '3085191', '7901652', '4532122', '2090121', '1223760', '1830823', '1589999', '4990338', '3172615', '3169830', '2984492', '2984481', '4109540', '3089549', '7298966', '2888796', '3085435', '3085438', '5356354', '3082279', '5353590', '2469028', '7642610', '5133088', '1095854', '1095848', '2086988', '1882905', '5074534', '2362868', '3083702', '8124374', '8927871', '3089561', '3129010', '2776589', '2776556', '9185677', '1878490', '2075983', '1095202', '3042751', '5289728', '2928791', '2928792', '3000679', '3066321', '3048203', '5568446', '5568442', '3757680', '3083941', '5289618', '5289620', '5188665', '5188667', '2013501', '2493599', '5568361', '5568339', '2774846', '3150814', '3141101', '2947311', '5392243', '8069132', '2874622', '2874621', '8077391', '5420957', '7279717', '7284676', '1045861', '3872424', '2925351', '2925366', '5330776', '2764179', '2764389', '3040913', '7324560', '3679569', '7521880', '7162908', '5348635', '7388745', '7271631', '3146791', '4213288', '5398027', '3133938', '3133950', '3034871', '3082263', '2704523', '2875968', '3098912', '153529651', '3025857', '153529738', '9178412', '3067891', '9491852', '2295309', '8029136', '1326634', '2080592', '5018723', '2767060', '3034646', '3026035', '153529543', '3026334', '3026162', '3025617', '3026105', '3025572', '7334487', '7396425', '5362058', '7333929', '3112455', '5217334', '7675714', '4998194', '3026074', '3026107', '8542672', '3112110', '5361969', '5362063', '5690583', '5689729', '3013739', '7455538', '7223177', '2874570', '3034825', '3042291', '10029214', '5200467', '10060153', '5178082', '2502805', '2766505', '5403541', '2874569', '2874515', '2874509', '2874508', '2683969', '2684070', '3610757', '2927530', '3952282', '8298495', '5192789', '2237923', '2225783', '7127810', '3169317', '5362215', '7282673', '6381277', '7282605', '3952070', '7331861', '3112345', '2714166', '2714656', '2714796', '2880539', '1047536', '5156102', '3112364', '8359568', '2715482', '2715206', '2715054', '2716311', '2716719', '2650827', '8332019', '2928598', '8898408', '1553384', '5170652', '8660938', '4998213', '4454311', '1335213', '2068870', '1314508', '1314773', '1043819', '1161901', '1161902', '4448925', '3641006', '2716226', '2715219', '2650829', '5354590', '7493561'], 'matchCase': False}, {'type': 'equals', 'key': 'HAS_COORDINATE', 'value': 'true', 'matchCase': False}, {'type': 'or', 'predicates': [{'type': 'not', 'predicate': {'type': 'equals', 'key': 'ESTABLISHMENT_MEANS', 'value': 'MANAGED', 'matchCase': False}}, {'type': 'not', 'predicate': {'type': 'isNotNull', 'parameter': 'ESTABLISHMENT_MEANS'}}]}, {'type': 'or', 'predicates': [{'type': 'not', 'predicate': {'type': 'equals', 'key': 'BASIS_OF_RECORD', 'value': 'LIVING_SPECIMEN', 'matchCase': False}}, {'type': 'not', 'predicate': {'type': 'isNotNull', 'parameter': 'BASIS_OF_RECORD'}}]}, {'type': 'or', 'predicates': [{'type': 'not', 'predicate': {'type': 'equals', 'key': 'BASIS_OF_RECORD', 'value': 'FOSSIL_SPECIMEN', 'matchCase': False}}, {'type': 'not', 'predicate': {'type': 'isNotNull', 'parameter': 'BASIS_OF_RECORD'}}]}, {'type': 'not', 'predicate': {'type': 'equals', 'key': 'DATASET_KEY', 'value': '50c9509d-22c7-4a22-a47d-8c48425ef4a7', 'matchCase': False}}, {'type': 'not', 'predicate': {'type': 'equals', 'key': 'DATASET_KEY', 'value': '47cac062-7224-4570-8141-c822320627fd', 'matchCase': False}}, {'type': 'not', 'predicate': {'type': 'equals', 'key': 'DATASET_KEY', 'value': '14d5676a-2c54-4f94-9023-1e8dcd822aa0', 'matchCase': False}}, {'type': 'not', 'predicate': {'type': 'equals', 'key': 'DATASET_KEY', 'value': '7a3679ef-5582-4aaa-81f0-8c2545cafc81', 'matchCase': False}}, {'type': 'within', 'geometry': 'POLYGON ((0.41748 40.446947, 1.043701 40.697299, 1.07666 41.004775, 2.219238 41.261291, 3.284912 41.836828, 3.240967 42.544987, 0.516357 42.932296, 0.527344 42.024814, 0.076904 40.971604, 0.131836 40.555548, 0.41748 40.446947))'}]}, 'sendNotification': True, 'format': 'SIMPLE_CSV', 'type': 'OCCURRENCE', 'verbatimExtensions': []}, 'created': '2024-12-16T14:08:22.013+00:00', 'modified': '2024-12-16T14:36:13.903+00:00', 'eraseAfter': '2025-06-16T14:08:21.982+00:00', 'status': 'SUCCEEDED', 'downloadLink': 'https://api.gbif.org/v1/occurrence/download/request/0036571-241126133413365.zip', 'size': 12924873, 'totalRecords': 130349, 'numberDatasets': 361}
        # print(f"Current status: {status['status']}")
        download_keys[download_key[0]] = "pending"

    available = False
    logger.info("*********************")
    logger.info("Checking download availability")
    while not available:
        # check availability
        available = check_download_availability(download_keys)
        # if all downloads are available
        if available == False:
            logger.info("Not all keys are available, waiting 10 minutes")
            sleep(600)
        else:
            logger.info("All keys available, starting download")
            success = download_all_data(download_keys, folder_path)
            if success:
                # TODO uncompress, read files and upload
                logger.info("All files downloaded")
                pass
            else:
                logger.info("Something went wrong with the download")


if __name__ == "__main__":
    main()
