import datetime
import logging
import os

import azure.durable_functions as df
import azure.functions as func
import requests
from azure.data.tables import TableClient
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
    manga_list = yield context.call_activity("generate_list")

    tasks = [context.call_activity("scrape", manga) for manga in manga_list.items()]

    results: list[dict] = yield context.task_all(tasks)

    yield context.call_activity("notify", results)

@app.activity_trigger(input_name="param")
def generate_list(param):
    table_client = TableClient.from_connection_string(
        conn_str=os.environ["AzureWebJobsStorage"], table_name="manga"
    )

    entities = table_client.query_entities("PartitionKey eq 'index'")

    return { int(entity["RowKey"]): int(entity["latest"]) if entity.get("latest") else None for entity in entities }

@app.activity_trigger(input_name="manga")
def scrape(manga: tuple[int: int | None]):
    logging.info(f"task received manga {manga}")

    manga_id, latest_ep = manga[0], manga[1]

    result = requests.get(f"{base_url}{manga_id}/")

    soup = BeautifulSoup(result.content, "html.parser")

    manga_episodes = {}

    for a in soup.find_all("a", class_="status0"):
        # This following statement extracts the episode id,
        # i.e. 574591 out of the href link
        # href="/comic/37456/574591.html"
        episode_id = int(a["href"].split("/")[-1].split(".")[0])
        manga_episodes[episode_id] = a["title"]

    if latest_ep:
        manga_episodes = {k: v for k, v in manga_episodes.items() if k > latest_ep}
        return manga_episodes
    else:
        latest = max(manga_episodes.keys())
        return {latest: manga_episodes[latest]}

@app.activity_trigger(input_name="new_manga_list")
def notify(new_manga_list: list[dict]):
    table_client = TableClient.from_connection_string(
        conn_str=os.environ["AzureWebJobsStorage"], table_name="manga"
    )

    notify_url = os.environ["NotifyURL"]

    # logging.info(results)
    for manga in new_manga_list:
        for manga_id, latest_ep in manga.items():
            entity = table_client.get_entity('index', str(manga_id))
            if entity.get('latest'):
                body = { "content": "new manga notification" }
                requests.post(notify_url, json=body )

                entity['latest'] = latest_ep

                table_client.update_entity()