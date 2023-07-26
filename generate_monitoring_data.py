from datetime import datetime
import os

import gbq_tools
from sqlQueries.pnlQuery import pnl_query_string
from sqlQueries.mrQuery import mr_query_string

import solvency_statistics

project_id = "risk-monitoring-361911"
key_path = os.path.expanduser("~/Repositories/Keys/gbq-risk-monitoring-361911-credentials.json")
project = gbq_tools.GBQProjectWithKey(project_id=project_id, key_path=key_path)

timestampstr = datetime.now().strftime("%Y%m%d-%H%M")
pnl_results = project.generic_query(pnl_query_string)
pnl_results.to_csv(f"monitoring-data/positions-{timestampstr}.csv")

project = gbq_tools.RiskMonitoringGBQ(version="mr")
mr_results = project.get_position_history(latest=True)
mr_results.to_csv(f"monitoring-data/positions-mr-{timestampstr}.csv")

solvency_statistics.compute_summary_negative_statistics(timestampstr)