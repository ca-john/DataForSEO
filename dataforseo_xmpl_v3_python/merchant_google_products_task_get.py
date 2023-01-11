from client import RestClient
# You can download this file from here https://cdn.dataforseo.com/v3/examples/python/python_Client.zip
client = RestClient("login", "password")
# 1 - using this method you can get a list of completed tasks
# GET /v3/merchant/google/products/tasks_ready
response = client.get("/v3/merchant/google/products/tasks_ready")
# you can find the full list of the response codes here https://docs.dataforseo.com/v3/appendix/errors
if response['status_code'] == 20000:
    results = []
    for task in response['tasks']:
        if (task['result'] and (len(task['result']) > 0)):
            for resultTaskInfo in task['result']:
                # 2 - using this method you can get results of each completed task
				# GET /v3/merchant/google/products/task_get/advanced/$id
                if(resultTaskInfo['endpoint_advanced']):
                    results.append(client.get(resultTaskInfo['endpoint_advanced']))
                '''
                # 3 - another way to get the task results by id
                # GET /v3/merchant/google/products/task_get/advanced/$id              
                if(resultTaskInfo['id']):
                    results.append(client.get("/v3/merchant/google/products/task_get/advanced/" + resultTaskInfo['id']))
                '''
    print(results)
    # do something with result
else:
    print("error. Code: %d Message: %s" % (response["status_code"], response["status_message"])) 
