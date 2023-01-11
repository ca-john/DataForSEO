from client import RestClient
# You can download this file from here https://cdn.dataforseo.com/v3/examples/python/python_Client.zip
client = RestClient("login", "password")
# get ad_url by the ad_aclk
# GET /v3/merchant/google/sellers/ad_url/$ad_aclk
# use the ad_aclk that you received upon a sellers task
ad_aclk = "DChcSEwiSl5TKpbPoAhVFmdUKHfa_B_wYABADGgJ3cw&sig"
response = client.get("/v3/merchant/google/sellers/ad_url/" + ad_aclk)
# you can find the full list of the response codes here https://docs.dataforseo.com/v3/appendix/errors
if response["status_code"] == 20000:
    print(response)
    # do something with result
else:
    print("error. Code: %d Message: %s" % (response["status_code"], response["status_message"])) 
