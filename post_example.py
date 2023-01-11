from client import RestClient
from pathlib import Path
import os

if __name__ == '__main__':

    client = RestClient("bill.charters@werrv.com", "917100dfd344bf83")
    post_data = dict()
    # simple way to set a task
    post_data[len(post_data)] = dict(
        location_name="United States",
        language_name="English",
        keyword="iphone"
    )
    # after a task is completed, we will send a GET request to the address you specify
    # instead of $id and $tag, you will receive actual values that are relevant to this task
    post_data[len(post_data)] = dict(
        location_name="United States",
        language_name="English",
        keyword="iphone",
        price_min=10,
        sort_by="price_low_to_high",
        priority=2,
        tag="some_string_123",
        pingback_url="https://your-server.com/pingscript?id=$id&tag=$tag"
    )
    # after a task is completed, we will send a GET request to the address you specify
    # instead of $id and $tag, you will receive actual values that are relevant to this task
    post_data[len(post_data)] = dict(
        location_name="United States",
        language_name="English",
        keyword="iphone",
        postback_data="html",
        postback_url="https://your-server.com/postbackscript"
    )
    # POST /v3/merchant/google/products/task_post
    response = client.post("/v3/merchant/google/products/task_post", post_data)
    # you can find the full list of the response codes here https://docs.dataforseo.com/v3/appendix/errors
    if response["status_code"] == 20000:
        print(response)
        # do something with result
        path = Path("post_example_ids.dat")
        if path.is_file():
            os.remove("post_example_ids.dat")
        with open("post_example_ids.dat", 'a+', encoding="utf8") as f:
            for task in response["tasks"]:
                f.write(task["id"])
                f.write('\n')
    else:
        print("error. Code: %d Message: %s" % (response["status_code"], response["status_message"]))
