import os
import requests
import pandas as pd
import pytz
from datetime import datetime, timedelta

from google.cloud import bigquery
from table2ascii import table2ascii

from queries import billing_query_services

def send_report_data_to_discord():
    results = query_bigquery()
    ascii_table = convert_results_to_ascii_table(results)
    discord_webhook_url = os.getenv('DISCORD_WEBHOOK_URL')
    send_content_to_discord(discord_webhook_url, ascii_table)


def query_bigquery():

    client = bigquery.Client()
    query_job = client.query(billing_query_services)
    results = query_job.result()

    return results


def convert_results_to_ascii_table(results, top_n = 5):

    rows = [dict(row) for row in results]
    data_df = pd.DataFrame(rows)
    data_lists = data_df.values.tolist()[:top_n]

    max_chars = 20

    for i in range(0, len(data_lists)):
        dl = data_lists[i]
        dl[0] =  dl[0][:(max_chars-2)] + ".." if len(dl[0]) > max_chars else dl[0]
        dl[1] =  str(round(dl[1]))
        dl[2] =  ("+" if (dl[2] > 0) else "-") + str(abs(round(dl[2]))) + "%"
        dl[3] =  str(round(dl[3]))
        dl[4] =  str(round(dl[4]))
        data_lists[i] = dl

    ascii_table = table2ascii(
        header=["Service", "Cost 1d", "% Avg", "Avg 7d", "Max 7d"],
        body=data_lists
    )

    return ascii_table


def send_content_to_discord(url, ascii_table):

    content = f"Top 5 serviços com maior gasto dia **{get_yesterday_formatted()}**"
    content += f"\n```{ascii_table}```"

    payload = {'content': content}
    response = requests.post(url, data=payload)

    if response.status_code >= 200 and response.status_code < 300:
        print('Request successful')
    else:
        print('Request failed')

def get_yesterday_formatted():
    timezone = pytz.timezone('America/Sao_Paulo')
    now = datetime.now(timezone)
    yesterday = now - timedelta(days=1)
    formatted_yesterday = yesterday.strftime('%d/%m/%Y')
    return formatted_yesterday


if __name__ == "__main__":
    send_report_data_to_discord()
