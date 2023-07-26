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
    
class RiskMonitoringGBQ(GBQProjectWithKey):
    def __init__(self, version="mr"):
        import os

        project_id = "risk-monitoring-361911"
        key_path = os.path.expanduser("~/Repositories/Keys/gbq-risk-monitoring-361911-credentials.json")
        project = GBQProjectWithKey(project_id=project_id, key_path=key_path)

        GBQProjectWithKey.__init__(self,project_id=project_id,key_path=key_path)
        if version == "mr":
            from sqlQueries.mrQuery import mr_query_string
            self.query_str = mr_query_string
        elif version == "pnl":
            from sqlQueries.pnlQuery import pnl_query_string
            self.query_str = pnl_query_string
    def get_position_history(self, latest=True, start_datetime_str=None, end_datetime_str=None,
                             chain_id=None, margin_engine=None,
                             owner=None, tick_lower=None, tick_upper=None):
        
        query_str = self.query_str
        if latest:
            query_str += f" AND is_latest_block = true"
        elif start_datetime_str and end_datetime_str:
            query_str = f"""\n AND timestamp > UNIX_SECONDS(TIMESTAMP('{start_datetime_str}'))
                            AND timestamp < UNIX_SECONDS(TIMESTAMP('{end_datetime_str}'))"""

        if chain_id:
            query_str += f" AND chain_id = '{chain_id}'"
        if margin_engine:
            query_str += f" AND margin_engine = '{margin_engine}'"
        if owner:
            query_str += f" AND owner = '{owner}'"
        if tick_lower:
            query_str += f" AND tick_lower = '{tick_lower}'"
        if tick_upper:
            query_str += f" AND tick_upper = '{tick_upper}'"
        return self.generic_query(query_str)
