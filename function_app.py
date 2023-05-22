import datetime
import azure.functions as func
import azure.durable_functions as df
from azure.data.tables import TableClient
import logging
from bs4 import BeautifulSoup
import requests
import os

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

@app.durable_client_input(client_name="client")
@app.schedule(schedule="0 */5 * * * *", arg_name="mytimer", run_on_startup=True,
              use_monitor=False) 
async def timer_start(mytimer: func.TimerRequest, client):
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    if mytimer.past_due:
        logging.info('The timer is past due!')

    instance_id = await client.start_new("hello_orchestrator")
    logging.info(f'Python timer trigger function ran at {utc_timestamp}, instance id = {instance_id}')

# Orchestrator
@app.orchestration_trigger(context_name="context")
def hello_orchestrator(context):
    table_client = TableClient.from_connection_string(
        conn_str=os.environ["AzureWebJobsStorage"],
        table_name="manga"
    )

    entities = table_client.query_entities("PartitionKey eq 'index'")

    ids = [entity['RowKey'] for entity in entities]

    tasks = [context.call_activity("scrape", id) for id in ids]

    logging.info((yield context.task_all(tasks)))
    
# An HTTP-Triggered Function with a Durable Functions Client binding
# @app.route(route="orchestrators/{functionName}")
# @app.durable_client_input(client_name="client")
# async def http_start(req: func.HttpRequest, client):
#     function_name = req.route_params.get('functionName')
#     instance_id = await client.start_new(function_name)
#     response = client.create_check_status_response(req, instance_id)
#     return response

# # Orchestrator
# @app.orchestration_trigger(context_name="context")
# def hello_orchestrator(context):
#     ids = [1, 2, 3]

#     tasks = [context.call_activity("scrape", id) for id in ids]

#     return (yield context.task_all(tasks))

# Activity
@app.activity_trigger(input_name="mangaid")
def scrape(mangaid: int):
    logging.info(f"task received mangaid {mangaid}")
    base_url = 'https://www.manhuagui.com/comic/'

    result = requests.get(f'{base_url}{mangaid}/')

    soup = BeautifulSoup(result.content, 'html.parser')
    return str(soup.find_all('a', {'class': 'status0'})[0])


