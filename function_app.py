import datetime
import azure.functions as func
import logging
from bs4 import BeautifulSoup
import requests

app = func.FunctionApp()

# Learn more at aka.ms/pythonprogrammingmodel

@app.function_name(name="mytimer")
@app.schedule(schedule="0 */5 * * * *", arg_name="mytimer", run_on_startup=True,
              use_monitor=False) 
def test_function(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    url = 'https://www.manhuagui.com/comic/31550/'
    result = requests.get(url)

    soup = BeautifulSoup(result.content, 'html.parser')
    filtered = '\n'.join(map(str, soup.find_all('a', {'class': 'status0'})))

    logging.info(filtered)



