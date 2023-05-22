import datetime
import azure.functions as func
import azure.durable_functions as df
import logging
from bs4 import BeautifulSoup
import requests

app = func.FunctionApp()

# Learn more at aka.ms/pythonprogrammingmodel

# @app.function_name(name="mytimer")
# @app.schedule(schedule="0 */5 * * * *", arg_name="mytimer", run_on_startup=True,
#               use_monitor=False) 
# def test_function(mytimer: func.TimerRequest) -> None:
#     utc_timestamp = datetime.datetime.utcnow().replace(
#         tzinfo=datetime.timezone.utc).isoformat()

#     if mytimer.past_due:
#         logging.info('The timer is past due!')

#     logging.info('Python timer trigger function ran at %s', utc_timestamp)

#     url = 'https://www.manhuagui.com/comic/31550/'
#     result = requests.get(url)

#     soup = BeautifulSoup(result.content, 'html.parser')
#     filtered = '\n'.join(map(str, soup.find_all('a', {'class': 'status0'})))

#     logging.info(filtered)


app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# @app.durable_client_input(client_name="client")
# @app.schedule(schedule="0 */5 * * * *", arg_name="mytimer", run_on_startup=True,
#               use_monitor=False) 
# async def timer_start(mytimer: func.TimerRequest, client):
#     utc_timestamp = datetime.datetime.utcnow().replace(
#         tzinfo=datetime.timezone.utc).isoformat()
#     if mytimer.past_due:
#         logging.info('The timer is past due!')

#     instance_id = await client.start_new("orchestration_trigger")
#     logging.info(f'Python timer trigger function ran at {utc_timestamp}, instance id = {instance_id}')
    
# An HTTP-Triggered Function with a Durable Functions Client binding
@app.route(route="orchestrators/{functionName}")
@app.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client):
    function_name = req.route_params.get('functionName')
    instance_id = await client.start_new(function_name)
    response = client.create_check_status_response(req, instance_id)
    return response

# Orchestrator
@app.orchestration_trigger(context_name="context")
def hello_orchestrator(context):
    ids = [1, 2, 3]

    tasks = list(map(lambda id: context.call_activity("scrape", id), ids))
    results = yield context.task_all(tasks)
    return results

# Activity
@app.activity_trigger(input_name="mangaid")
def scrape(mangaid: int):
    return "Scraped " + str(mangaid)   


