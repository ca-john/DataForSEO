"""
This module facilitates the connection to the DataForSEO API, reading and parsing data for API calls
and sending REST API requests for use with the Merchant API provided by DataForSEO.
"""
from pathlib import Path
from typing import Dict, List
import pandas as pd
from client import RestClient
import matplotlib.pyplot as plt
import itertools
import json

DEFAULT_EMAIL = "bill.charters@werrv.com"
DEFAULT_PWD = "917100dfd344bf83"

PARAMETERS = dict(
        location_name = "Canada",
        language_name = "English",
        sort_by = "price_low_to_high",
        priority=2,
        price_min=5
)

SUCCESS_STATUS_CODE = 20000
TASK_CREATED_CODE = 20100

FILE_NAME = "task_ids.dat"

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
    path = Path(FILE_NAME)
    if not path.is_file():
        print("Creating file...")
    with open(FILE_NAME, 'a+', encoding="utf8") as file:
        file.write(_id)
        file.write('\n')
    print(f"Task id written to file {FILE_NAME}")


def read_xlsx(file_name: str) -> List[Dict]:
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
    # Check if file exists
    path = Path(file_name)
    if not path.is_file():
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
    return product_data




def set_task(file_name: str) -> List[Dict[int, Dict]]:
    """Sets the appropriate task information that will be sent
    Args:
        file_name (str): The name of the file containing the data
    Returns:
        Dict[int]: The data for the request
    """
    # We The maximum number of API calls per minute is 2000 and each API call
    # cannot exceed 100 tasks, hence why we need a list of dictionaries
    post_data = dict()
    # The product data from the excel file, converted into pandas records format
    product_data = read_xlsx(file_name)
    
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
                keyword=str(product["Variant Barcode"]),
                price_min=PARAMETERS["price_min"]
                # Add tag to identify task??
                # tag=product["Variant Barcode"]
            )
    if len(post_data) > 0:
        data_list.append(post_data)
    
    return data_list


def write_json_file(data_list: List[Dict[int, Dict]], file_name: str) -> None:
    """ Write the data that is meant to be send via the POST request to the file

    Args:
        data_list (List[Dict[int, Dict]]): The data to be sent with the call
        file_name (str): Name of the file
    """
    new_data = list(itertools.chain.from_iterable(data_list))
    with open(file_name, 'w+', encoding='utf8') as f:
        json.dump(new_data, f)



def plot_price() -> None:
    results = list()
    client = connect()
    with open("post_example_ids.dat", "r", encoding="utf8") as f:
        for line in f.readlines():
            _id = line.strip()
            print(f"The id is {_id}")
            r = client.get("/v3/merchant/google/products/task_get/advanced/" + _id)
            results.append(r)
            break
        # j = json.dumps(results[0], indent=4)
        # with open("results.json", 'w+', encoding="utf8") as r_file:
        #     r_file.write(j)
        data = list()
        for i in results[0]["tasks"][0]["result"][0]["items"]:
            if "price" in i and (isinstance(i["price"], int) or isinstance(i["price"], float)):
                data.append(i["price"])
        print(len(data))
        # density = gaussian_kde(data)
        # plt.plot(data)
        df = pd.DataFrame(data, columns= ['price'])
        df.plot(kind = 'density')
        # plt.figure(figsize = (5,5))
        # sb.kdeplot(data, bw = 0.5 , fill = True)
        plt.show()






# def send_post(data_list: List[Dict[int:Dict]]) -> None:
    


if __name__ == '__main__':
    print("------------------------------------------------------")
    d = set_task("data.xlsx")
    print(type(d[0]))
    # for i in d[0]:
    #     print(d[0][i]["keyword"])