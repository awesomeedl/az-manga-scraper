import datetime
import json
import logging
import os

import azure.durable_functions as df
import azure.functions as func
import requests
from bs4 import BeautifulSoup

# Learn more at aka.ms/pythonprogrammingmodel

app = df.DFApp()

base_url = "https://www.manhuagui.com/comic/"


@app.durable_client_input(client_name="client")
@app.schedule(schedule="0 0 */4 * * *", arg_name="mytimer", run_on_startup=False, use_monitor=False)
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
    
    table: list[dict] = yield context.call_activity("read_table")

    scrape_tasks = []
    
    for manga in table:
        manga_id = manga['RowKey']
        latest_ep = manga.get('latest')

        task = context.call_activity('scrape', (manga_id, latest_ep))
        scrape_tasks.append(task)
    
    results: list[dict[int, str]] = yield context.task_all(scrape_tasks)

    update_tasks = []
    notify_tasks = []

    for i, manga in enumerate(table):
        if not results[i]: continue

        if 'latest' in manga:
            notify_tasks.append(context.call_activity('notify', (manga, results[i])))
        
        manga['latest'] = max(results[i].keys())
        update_tasks.append(context.call_activity('write_table', manga))

    if update_tasks:
        yield context.task_all(update_tasks)
    if notify_tasks:
        yield context.task_all(notify_tasks)

    

@app.activity_trigger(input_name="info")
@app.table_input('table', 'MangaTableURL', 'manga', partition_key='index')
def read_table(table, info):
    logging.info(f'query returned table: {table}')

    return json.loads(table)


@app.activity_trigger(input_name='entity')
@app.table_output('table', 'MangaTableURL', 'manga', partition_key='index')
def write_table(table: func.Out[str], entity: dict) -> str:
    logging.info(f'write_table received param: {entity}')

    # We must set the ETag property like this in order to update the record
    entity['ETag'] = '*'
    table.set(json.dumps(entity))
    
    return f'successfully updated table entity: {entity}'


@app.activity_trigger(input_name="args")
def scrape(args: tuple[str, str]):
    manga_id, latest_ep = args[0], int(args[1]) if args[1] else None

    logging.info(f"scrape received args: id:{manga_id} latest:{latest_ep}")

    response = requests.get(f"{base_url}{manga_id}/")
    soup = BeautifulSoup(response.content, "html.parser")
    
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

@app.activity_trigger(input_name="args")
def notify(args: tuple[dict, dict[int, str]]) -> str:
    manga, episodes = args[0], args[1]

    logging.info(f"notify received args: name:{args[0]} episodes:{args[1]}")

    notify_url = os.environ["NotifyURL"]

    embed = {
        'title': manga['name'],
        'thumbnail': { 
            'url': manga['img'] 
        },
        'fields': [
            {
                'name': v, 
                'value': f'[Link]({base_url}{manga["RowKey"]}/{k}/)', 
                'inline': True
            } for k, v in episodes.items()
        ]
    }


    requests.post(notify_url, json={'embeds': [embed]})

    '''
    Activity trigger requires a return value, otherwise the orchestrator function will throw an exception
    '''
    return 'notified user of new manga update'