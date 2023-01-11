from client import RestClient
import json
import matplotlib.pyplot as plt
from scipy.stats import gaussian_kde
import pandas as pd
import seaborn as sb

if __name__ == '__main__':

    client = RestClient("bill.charters@werrv.com", "917100dfd344bf83")
    results = list()
    
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
        # df = pd.DataFrame(data, columns= ['price'])
        # df.plot(kind = 'density')
        plt.figure(figsize = (5,5))
        sb.kdeplot(data, bw = 0.5 , fill = True)
        plt.show()