"""
This module facilitates the connection to the DataForSEO API, reading and parsing data for API calls
and sending REST API requests for use with the Merchant API provided by DataForSEO.
"""
from pathlib import Path
from typing import Dict, List, Union, Tuple, Any
import pandas as pd
from client import RestClient
from time import sleep
import json
import os
import csv


DEFAULT_EMAIL = ""
DEFAULT_PWD = ""

PARAMETERS = dict(
        location_name = "Canada",
        language_name = "English",
        sort_by = "price_low_to_high",
        priority=2,
        price_min=0.5
)

SUCCESS_STATUS_CODE = 20000
TASK_CREATED_CODE = 20100

DATA_FILE = "product_data.xlsx"
TASK_IDS_FILE = "task_ids.dat"
RESULTS_FILE = "task_results.json"
OUTPUT_FILE = "results.csv"

# Waiting time for tasks to finish since they will be in the queue
TASK_WAIT = 360

def connect(e_id: str = DEFAULT_EMAIL, token: str = DEFAULT_PWD) -> RestClient:
    """
    Connects to DataForSEO and returns the RestClient object that is then
    used to send requests.
    Args:
        e_id (str, optional): The email id used when logging into the API.
                                Defaults to DEFAULT_EMAIL.
        token (str, optional): The password/API token used when logging into the API.
                                Defaults to DEFAULT_PWD.
    Returns:
        RestClient: The object used to send requests to DataForSEO.
    """
    if DEFAULT_EMAIL == '' or DEFAULT_PWD == '':
        print("Enter the login email:")
        e_id = input()
        print("Enter the login password:")
        token = input()
    client = RestClient(e_id, token)
    return client



def get_languages() -> List[Dict]:
    """Returns the list of languages supported by the google merchant API
    or an empty list if there is an error

    Returns:
        List[Dict]: A list of dictionaries containing
        "language_name": The name of the language
        "language_code": The corresponding ISO code of the language
    """
    client = connect()
    response = client.get("/v3/merchant/google/languages")
    if response["status_code"] == SUCCESS_STATUS_CODE:
        # print(response)
        return response["tasks"][0]["result"]
    status_code = response["status_code"]
    status_message = response["status_message"]
    print(f"error. Code: {status_code} Message: {status_message}")
    return [Dict()]



def get_locations() -> List[Dict]:
    """ Returns the list of languages supported by google merchant API
    or an empty list if there is an error
    Returns:
        List[Dict]: A list of dictionaries, each containing
          "location_code",
          "location_name",
          "location_name_parent",
          "country_iso_code",
          "location_type"
    """
    client = connect()
    response = client.get("/v3/merchant/google/locations")
    if response["status_code"] == SUCCESS_STATUS_CODE:
        # print(response)
        return response["tasks"][0]["result"]
    status_code = response["status_code"]
    status_message = response["status_message"]
    print(f"error. Code: {status_code} Message: {status_message}")
    return [Dict()]




def write_id_to_file(_id: str) -> None:
    """ Writes a task id to the file with the name task_ids.dat, or creates
    the file if it does not exist.

    Args:
        id (str): The id string of the task to be written
    """
    path = Path(TASK_IDS_FILE)
    if not path.is_file():
        print("Creating file...")
    with open(TASK_IDS_FILE, 'a+', encoding="utf-8") as file:
        file.write(_id)
        file.write('\n')
    


def read_xlsx(file_name: str) -> Tuple[List[Dict], Dict[str, int]]:
    """
    Reads the excel spreadsheet containing the product data, saves it to a csv file and
    returns the dictionary containing the data or an empty dictionary if there is an error
    Args:
        file_name (str): The path to the spreadsheet file
    """
    # TODO: Add the raw input option when passing the filename
    # Check and fix extension
    if ".xlsx" not in file_name:
        file_name = file_name + ".xlsx"
    product_data = dict()
    if not os.path.isfile(file_name):
        print("Invalid file")
        return product_data
    file = pd.read_excel(file_name)

    # Drop float NaN values
    file = file.dropna(subset=['Variant Barcode'])

    # Drop all rows that are non-numeric
    file = file[pd.to_numeric(file['Variant Barcode'], errors='coerce').notnull()]

    # file = file[file['Variant Barcode'].apply(lambda x: isinstance(x, str))]
    # Change UPC to an integer instead of a float
    file['Variant Barcode'] = file['Variant Barcode'].astype(int)
    # Store the product data as a dictionary
    # Each element in this product_data list corresponds to a dictionary with each column names
    # as keys, i.e. product_data[i].keys() will return the list of column names in
    # the excel spreadsheet
    product_data = file.to_dict('records')
    id_keyword = dict()
    for p in product_data:
        id_keyword[str(p["Title"])] = p["ID"], p["Variant Price"]
    return product_data, id_keyword



def set_task(file_name: str = DATA_FILE) -> Tuple[List[Dict[int, Dict]], Dict[str, int]]:
    """Sets the appropriate task information that will be sent
    Args:
        file_name (str): The name of the file containing the data
    Returns:
        The first return value is the data for the request and the second item is 
        the mapping of keywords to IDs
    """
    
    # We The maximum number of API calls per minute is 2000 and each API call
    # cannot exceed 100 tasks, hence why we need a list of dictionaries
    post_data = dict()
    # The product data from the excel file, converted into pandas records format
    product_data, id_keyword = read_xlsx(file_name)
    data_list = list()
    for product in product_data:
        if len(post_data) >= 100:
            data_list.append(post_data.copy())
            post_data = {}
        if not product["Variant Barcode"] == '' and product["Variant Barcode"]:
            post_data[len(post_data)] = dict(
                location_name=PARAMETERS["location_name"],
                language_name=PARAMETERS["language_name"],
                priority=PARAMETERS["priority"],
                sort_by=PARAMETERS["sort_by"],
                # UPC isn't a good keyword
                # keyword=str(product["Variant Barcode"]),
                keyword=str(product["Title"]),
                # price_min=PARAMETERS["price_min"]
                # Minimum price to filter out bad results
                price_min=PARAMETERS["price_min"]*product["Variant Price"]
                # Add tag to identify task??
                # tag=product["Variant Barcode"]
            )
    if len(post_data) > 0:
        data_list.append(post_data)
    return data_list, id_keyword


def write_json_file(data_list: List[Dict[int, Dict]], file_name: str) -> None:
    """ Write the data that is meant to be send via the POST request to the file

    Args:
        data_list (List[Dict[int, Dict]]): The data to be sent with the call
        file_name (str): Name of the file
    """
    new_data = dict()
    n_items = 0
    for i in data_list:
        for j in i:
            new_data[n_items] = i[j]
            n_items += 1
    with open(file_name, 'w+', encoding='utf-8') as file:
        json.dump(new_data, file, sort_keys=True, indent=4)


# def plot_price() -> None:
#     results = list()
#     client = connect()
#     with open("post_example_ids.dat", "r", encoding="utf-8") as f:
#         for line in f.readlines():
#             _id = line.strip()
#             print(f"The id is {_id}")
#             r = client.get("/v3/merchant/google/products/task_get/advanced/" + _id)
#             results.append(r)
#             break
#         # j = json.dumps(results[0], indent=4)
#         # with open("results.json", 'w+', encoding="utf-8") as r_file:
#         #     r_file.write(j)
#         data = list()
#         for i in results[0]["tasks"][0]["result"][0]["items"]:
#             if "price" in i and (isinstance(i["price"], int) or isinstance(i["price"], float)):
#                 data.append(i["price"])
#         print(len(data))
#         # density = gaussian_kde(data)
#         # plt.plot(data)
#         df = pd.DataFrame(data, columns= ['price'])
#         df.plot(kind = 'density')
#         # plt.figure(figsize = (5,5))
#         # sb.kdeplot(data, bw = 0.5 , fill = True)
#         plt.show()


def send_post(data_list: List[Dict[int, Dict]]) -> None:
    """ Sends the POST request to the DataForSEO server with the
    appropriate data and checks if the tasks were created properly.
    Args:
        data_list (List[Dict[int, Dict]]): The data for the request.
    """
    client = connect()
    response_list = list()
    for dat in data_list:
        res = client.post("/v3/merchant/google/products/task_post", dat)
        response_list.append(res)
        # Sleep for 50 milliseconds
        sleep(0.05)
    with open("post_responses.json", 'a+', encoding="utf-8") as file:
        for i in range(len(response_list)):
            json.dump(response_list[i], file, indent=4)
            file.write("\n")
    for response in response_list:
        if response["status_code"] == SUCCESS_STATUS_CODE:
            # print(response)
            # do something with result
            for task in response["tasks"]:
                if task["status_code"] == TASK_CREATED_CODE:
                    write_id_to_file(task["id"])
        else:
            print(f"Error. Code: {response['status_code']} Message: {response['status_message']}")


def get_task_by_ids(file_name: str = TASK_IDS_FILE) -> List[Dict[str, Union[str, int, List]]]:
    """ Reads the task ids from a file and sends a GET request to the DataForSEO
    API to get the results. The results are returned as a list of dictionaries,
    each dicionary with the same format.
    Args:
        file_name (str): The name of the file to read task ids from.

    Returns:
        List[Dict[str, Union[str, int, List]]]: The list of result dictionaries
    """
    client = connect()
    results: List[Dict[str, Union[str, int, List]]] = list()
    with open(file_name, 'r', encoding="utf-8") as file:
        for line in file.readlines():
            _id = line.strip()
            print(f"Reading and processing ID {_id}")
            res = client.get("/v3/merchant/google/products/task_get/advanced/" + _id)
            results.append(res)
    return results


def write_results_json(results: List[Dict[str, Union[str, int, List]]], file_name: str = RESULTS_FILE) -> None:
    """ Writes the results json to a json file.

    Args:
        file_name (str): The name of the json file.
        results(List[Dict[str, Union[str, int, List]]]): The results obtained from the call
    """
    with open(file_name, 'w+', encoding="utf-8") as file:
        for result in results:
            json.dump(result, file, indent=4)


def analyze_results(results: List[Dict[str, Union[str, int, List]]]) -> Dict[str, List[int]]:
    """ Given the resultss, collect all the prices of all the products and group it together

    Args:
        results (List[Dict[str, Union[str, int, List]]]): The results obtained from the call

    Returns:
        Dict[str, List[int]]: The prices of the products, where the keys are the product names
        and the value is the list of all the prices.
    """
    price_dict: Dict[str, List[Any]] = dict()
    for r in results:
        if r["status_code"] != SUCCESS_STATUS_CODE:
            print(f"ERROR: Status code {r['status_code']} when trying to fetch results")
        else:
           for t in r["tasks"]:
               if t["status_code"] == SUCCESS_STATUS_CODE:
                   keyword = t["data"]["keyword"]
                   for data in t["result"]:
                       for item in data["items"]:
                            price = item["price"]
                            url = item["url"]
                            t = (price, url)
                            if keyword not in price_dict:
                                    price_dict[keyword] = [t]
                            else:
                                    price_dict[keyword].append(t)
    return price_dict


def write_output_csv(price_dict: Dict[str, List[int]], id_keyword: Dict[str, int]) -> None:
    """ This function writes the names, ids and the price listings to a csv file defined in the header
    under OUTPUT_FILE

    Args:
        price_dict (Dict[str, List[int]]): The dictionary containing the keywords, i.e. names of products and lists of prices.
        id_keyword (Dict[str, int]): Mapping of keywords to ID numbers.
    """
    
    header = ['ID', 'Product Name', 'Current Price', 'Competitor Prices, URLs']
    with open(OUTPUT_FILE, 'w+', encoding='utf-8') as f:
        writer = csv.writer(f)
        # write the header
        writer.writerow(header)
        
        for p in price_dict:
            row_data = []
            p_id = id_keyword[p][0]
            curr_price = -1
            if p in id_keyword:
                curr_price = id_keyword[p][1]
            price_urls = []
            
            for i in price_dict[p]:
                price = i[0]
                url = i[1]
                price_urls.append(price)
                price_urls.append(url)
            # prices = price_dict[p][0]
            # urls = price_dict[p][1]
            
            row_data.append(p_id)
            row_data.append(p)
            row_data.append(curr_price)
            row_data.extend(price_urls)
            # write the data
            writer.writerow(row_data)


def cleanup() -> None:
    """ Cleans up all the files
    """
    print("Cleaning up files...")
    
    # Remove file with all the task IDs
    if os.path.isfile(TASK_IDS_FILE):
        print(f"Removing file {TASK_IDS_FILE}")
        try:
            os.remove(TASK_IDS_FILE)
        except OSError as file_e:
            print(f"Error: {file_e.filename} - {file_e.strerror}.")
    
    # Remove file with all the results from the call
    if os.path.isfile(RESULTS_FILE):
        print(f"Removing file {RESULTS_FILE}")
        try:
            os.remove(RESULTS_FILE)
        except OSError as file_e:
            print(f"Error: {file_e.filename} - {file_e.strerror}.")
    
    # Remove the output file with all the prices
    # if os.path.isfile(OUTPUT_FILE):
    #     print(f"Removing file {OUTPUT_FILE}")
    #     try:
    #         os.remove(OUTPUT_FILE)
    #     except OSError as file_e:
    #         print(f"Error: {file_e.filename} - {file_e.strerror}.")


if __name__ == '__main__':
    
    print("------------------------------------------------------")
    print("Running the program")
    print("------------------------------------------------------")
    
    
    # What this does is just use the script to get the data from the call if it was qeued and not retrieved.
    print("This option is for when you want to retrieve the tasks again if they were not finished on the last call:")
    read_from_id = input("Only get results from a previous call? Y/N (Default=N)  ")
    
    if read_from_id == "N" or read_from_id == '':
        
        print("Cleaning up previous files")
        print("------------------------------------------------------")
        cleanup()
        
        f_name = input("Input the name of the xlsx data file:  ")
        if not f_name:
            print(f"Reading data from {DATA_FILE}")
        else:
            DATA_FILE = f_name

        d, id_kw = set_task(DATA_FILE)
        # print(len(d))
        # write_json_file(d, 'task_data.txt')
        send_post(d)
        print(f"Task IDs written to file {TASK_IDS_FILE}")
        print("Sent the data to DataForSEO")
        print("Waiting for tasks to finish")
        sleep(TASK_WAIT)
        res = get_task_by_ids()
        write_results_json(res, RESULTS_FILE)
        p_dict = analyze_results(res)
        write_output_csv(p_dict, id_kw)
        print(f"Wrote the output to {OUTPUT_FILE}")
        
    elif read_from_id == "Y":
        
        print(f"Reading IDs from {TASK_IDS_FILE}")
        d, id_kw = set_task(DATA_FILE)
        res = get_task_by_ids()
        write_results_json(res, RESULTS_FILE)
        p_dict = analyze_results(res)
        write_output_csv(p_dict, id_kw)
        print(f"Wrote the output to {OUTPUT_FILE}")