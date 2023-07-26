class GBQProjectWithKey:
    def __init__(self, project_id, key_path):
        from google.cloud import bigquery
        from google.oauth2 import service_account

        self.project_id = project_id
        self.key_path = key_path
        self.credentials = service_account.Credentials.from_service_account_file(key_path)
        self.client = bigquery.Client(credentials=self.credentials,project=project_id)
    
    def generic_query(self, query_string):
        pnl_query_job = self.client.query(query_string)
        query_results = pnl_query_job.result().to_dataframe()
        return query_results
    