
def complete_mr_table(identifier,pool_specs_df):
    """
    This function starts from Alex' big query
    and generates the full risk monitoring one.
    """
    import numpy as np
    import pandas as pd
    from datetime import datetime

    positions_mr_df = pd.read_csv(f"monitoring-data/positions-mr-{identifier}.csv")
    
    positions_mr_df = positions_mr_df.sort_values("timestamp")
    positions_mr_df["timestamp"] = pd.to_datetime(positions_mr_df["timestamp"], unit="s")
    positions_mr_df = pd.merge(positions_mr_df, pool_specs_df, on=["chain_id","margin_engine"], how="left")
    positions_mr_df["notional_exposure"] = np.abs(positions_mr_df["variable_token_balance"]*(np.abs(positions_mr_df["variable_token_balance"])>positions_mr_df["min_size"]))
    positions_mr_df["liquidation_margin_bp"] = 10**4*(positions_mr_df["liquidation_margin_requirement"]/positions_mr_df["notional_exposure"]).replace(np.inf, None).replace(-np.inf, None)
    positions_mr_df["pool_term"] = (1/(365*24*3600))*(positions_mr_df["maturity_date"].apply(datetime.timestamp) - positions_mr_df["inception_date"].apply(datetime.timestamp))
    positions_mr_df["maturity"] = (1/(365*24*3600))*(positions_mr_df["maturity_date"].apply(datetime.timestamp) - positions_mr_df["timestamp"].apply(datetime.timestamp))
    positions_mr_df["fixed_rate"] = 1.0001**(-positions_mr_df["tick"])
    positions_mr_df["excess_fixed_tokens"] = positions_mr_df["fixed_token_balance"] + positions_mr_df["variable_token_balance"]*positions_mr_df["fixed_rate"]#*positions_mr_df["maturity"]/positions_mr_df["pool_term"]
    positions_mr_df["tot_pnl_curr"] = .01*positions_mr_df["excess_fixed_tokens"]*positions_mr_df["pool_term"]
    positions_mr_df["available_margin"] = positions_mr_df["position_margin"] + positions_mr_df["tot_pnl_curr"]*(positions_mr_df["tot_pnl_curr"]<0)

    positions_mr_df = positions_mr_df[["timestamp","chain_id","margin_engine","owner","tick_lower","tick_upper","tick","fixed_token_balance","variable_token_balance","notional_exposure",
                                       "fixed_rate","excess_fixed_tokens","tot_pnl_curr","accrued_cashflow","position_margin","available_margin","liquidation_margin_requirement","liquidation_margin_bp"
                                     ]]
    return positions_mr_df