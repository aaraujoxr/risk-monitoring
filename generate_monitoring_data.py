import gbq_risk_query
from definitions import key_path

timestampstr = gbq_risk_query.get_gbq_data(key_path=key_path)

import solvency_statistics

solvency_statistics.compute_summary_negative_statistics(timestampstr)