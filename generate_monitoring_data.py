from datetime import datetime
import os
import pandas as pd

import gbq_tools

def generate_monitoring_data(path_to_save):
    import monitoring_functions

    timestampstr = datetime.now().strftime("%Y%m%d-%H%M")

    project = gbq_tools.RiskMonitoringGBQ(version="mr")
    mr_results = project.get_position_history(latest=True)
    mr_results.to_csv(path_to_save + f"positions-mr-{timestampstr}.csv")

    pool_specs_df = pd.read_csv("pool-specs.csv", index_col=0)
    pool_specs_df["maturity_date"] = pool_specs_df["maturity_date"].apply(lambda x: datetime.strptime(x,"%m-%d-%Y %H:%M:%S"))
    pool_specs_df["inception_date"] = pool_specs_df["inception_date"].apply(lambda x: datetime.strptime(x,"%m-%d-%Y %H:%M:%S"))

    full_mr_table = monitoring_functions.complete_mr_table(mr_results, pool_specs_df)
    full_mr_table.to_csv(path_to_save + f"full-mr-{timestampstr}.csv")
    monitoring_functions.compute_summary_statistics(timestampstr,
                                                    pool_specs=pool_specs_df,
                                                    path_to_save=path_to_save)
    
    import solvency_statistics
    from sqlQueries.pnlQuery import pnl_query_string

    project_id = "risk-monitoring-361911"
    key_path = os.path.expanduser("~/Repositories/Keys/gbq-risk-monitoring-361911-credentials.json")
    project = gbq_tools.GBQProjectWithKey(project_id=project_id, key_path=key_path)

    pnl_results = project.generic_query(pnl_query_string)
    pnl_results.to_csv(path_to_save + f"positions-{timestampstr}.csv")

    solvency_statistics.compute_summary_negative_statistics(timestampstr, path_to_save=path_to_save)

path_to_save = os.path.expanduser("~/Dropbox\Voltz work\Risk\monitoring-data/")
generate_monitoring_data(path_to_save)