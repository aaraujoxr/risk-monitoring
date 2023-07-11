import gbq_risk_query

timestampstr = gbq_risk_query.get_gbq_data()

import solvency_statistics

solvency_statistics.compute_summary_negative_statistics(timestampstr)