import functions_framework

from report import send_bq_report_data_to_discord

@functions_framework.http
def process_request(request):
    print(request.args)
    print(request.get_json(silent=True))

    return send_bq_report_data_to_discord()
