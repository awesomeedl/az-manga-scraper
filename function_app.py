import datetime
import json
import logging
import os

import azure.durable_functions as df
import azure.functions as func
from azure.data.tables import TableClient
import requests
from bs4 import BeautifulSoup

# Learn more at aka.ms/pythonprogrammingmodel

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

base_url = "https://www.manhuagui.com/comic/"


@app.durable_client_input(client_name="client")
@app.schedule(
    schedule="0 */5 * * * *", arg_name="mytimer", run_on_startup=True, use_monitor=False
)
async def timer_start(mytimer: func.TimerRequest, client: df.DurableOrchestrationClient):
    utc_timestamp = (
        datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    )
    if mytimer.past_due:
        logging.info("The timer is past due!")

    instance_id = await client.start_new("scrape_orchestrator")
    
    logging.info(
        f"Python timer trigger function ran at {utc_timestamp}, instance id = {instance_id}"
    )


# Orchestrator
@app.orchestration_trigger(context_name="context")
def scrape_orchestrator(context: df.DurableOrchestrationContext):
    table = yield context.call_activity("read_table", "index")

    scrape_tasks = [context.call_activity("scrape", manga) for manga in table]
    results: list[dict] = yield context.task_all(scrape_tasks)

    notify_tasks = [context.call_sub_orchestrator("notify_orchestrator", (table[i], results[i])) for i in range(len(table))]

    yield context.task_all(notify_tasks)

@app.orchestration_trigger(context_name='context')
def notify_orchestrator(context: df.DurableOrchestrationContext):
    input: tuple[dict, dict] = context.get_input() # type: ignore
    manga, result = input[0], input[1]


    

@app.activity_trigger(input_name="key")
def read_table(key):
    client: TableClient = TableClient.from_connection_string(os.environ['AzureWebJobsStorage'], 'manga')
    table = list(client.query_entities(f"PartitionKey eq '{key}'"))

    logging.info(f'query returned table: {table}')
    return table


@app.activity_trigger(input_name='entity')
def write_table(entity) -> None:
    client: TableClient = TableClient.from_connection_string(os.environ['AzureWebJobsStorage'], 'manga')
    client.upsert_entity(entity)
    
    logging.info(f'successfully updated table entity: {entity}')


@app.activity_trigger(input_name="manga")
def scrape(args: tuple[str, str | None]):
    # logging.info(f"task received manga {manga}")

    manga_id = args[0], latest_ep = args[1]  # type: ignore

    page = requests.get(f"{base_url}{manga_id}/")
    soup = BeautifulSoup(page.content, "html.parser")
    
    episodes: dict[int, str] = {}

    for a in soup.find_all("a", class_="status0"):
        # This following statement extracts the episode id,
        # i.e. 574591 out of the href link
        # href="/comic/37456/574591.html"
        episode_id = int(a["href"].split("/")[-1].split(".")[0])
        episodes[episode_id] = a["title"]

    # If the manga has been scraped before, we return the new delta
    if latest_ep:
        episodes = {ep_id: title for ep_id, title in episodes.items() if ep_id > latest_ep}
        return episodes
    # If the manga has not been scraped before, we simply return the single latest episode
    else:
        latest = max(episodes.keys())
        return {latest: episodes[latest]}

# @app.activity_trigger(input_name="new_manga_list")
# def notify(new_manga_list: list[dict]):
#     table_client = TableClient.from_connection_string(
#         conn_str=os.environ["AzureWebJobsStorage"], table_name="manga"
#     )

#     notify_url = os.environ["NotifyURL"]

#     # logging.info(results)
#     for manga in new_manga_list:
#         for manga_id, latest_ep in manga.items():
#             entity = table_client.get_entity('index', str(manga_id))
#             if entity.get('latest'):
#                 body = { "content": "new manga notification" }
#                 requests.post(notify_url, json=body )

#                 entity['latest'] = latest_ep

#                 table_client.update_entity()