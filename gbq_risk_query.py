def get_gbq_data():
    import os
    import numpy as np
    import pandas as pd
    from datetime import datetime

    from sqlQueries.pnlQuery import pnl_query_string
    from sqlQueries.mrQuery import mr_query_string

    from google.cloud import bigquery
    from google.oauth2 import service_account

    key_path = os.path.expanduser("~/Repositories/Keys/gbq-risk-monitoring-361911-credentials.json")
    credentials = service_account.Credentials.from_service_account_file(key_path)
    project_id = "risk-monitoring-361911"

    client = bigquery.Client(credentials=credentials,project=project_id)

    timestampstr = datetime.now().strftime("%Y%m%d-%H%M")

    pnl_query_job = client.query(pnl_query_string)
    pnl_results = pnl_query_job.result().to_dataframe()
    pnl_results.to_csv(f"monitoring-data/positions-{timestampstr}.csv")

    mr_query_job = client.query(mr_query_string)
    mr_results = mr_query_job.result().to_dataframe()
    mr_results.to_csv(f"monitoring-data/positions-mr-{timestampstr}.csv")

    return timestampstr
