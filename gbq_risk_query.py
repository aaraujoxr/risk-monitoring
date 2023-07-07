def get_gbq_data():
    import os
    import numpy as np
    import pandas as pd

    from datetime import datetime

    from google.cloud import bigquery
    from google.oauth2 import service_account

    key_path = os.path.expanduser("~/Repositories/Keys/gbq-risk-monitoring-361911-credentials.json")
    credentials = service_account.Credentials.from_service_account_file(key_path)
    project_id = "risk-monitoring-361911"

    client = bigquery.Client(credentials=credentials,project=project_id)

    query_job = client.query(

    """--- FINAL POOLS INSOLVENCY QUERY
    with pools as (
    select chainId, vamm, termEndTimestampInMS / 1000 as maturityTimestamp, rateOracle
    from `risk-monitoring-361911.voltz_v1_positions.Voltz V1 Pools Production 120623`
    where termEndTimestampInMS / 1000  > UNIX_SECONDS(CURRENT_TIMESTAMP())
    ),

    pools_details as (
    select chainId, vamm, maturityTimestamp, fixed_rate, liquidity_index
    from `risk-monitoring-361911.historical_rates_and_index.fixed_rates` f
    join pools
    on vamm_address = vamm and chainId = f.chain_id
    join `risk-monitoring-361911.historical_rates_and_index.variable_rates` v
    on rate_oracle_address = rateOracle and chainId = v.chain_id
    where f.timestamp = (
        select max(g.timestamp) from `risk-monitoring-361911.historical_rates_and_index.fixed_rates` g
        where g.vamm_address = vamm
        ) and
        v.timestamp = (
        select max(u.timestamp) from `risk-monitoring-361911.historical_rates_and_index.variable_rates` u
        where u.rate_oracle_address = rateOracle
        )
    ),

    details as (
    SELECT
        owneraddress,
        tickLower,
        tickUpper,
        p.vammAddress,
        p.chainId,
        netNotionalLocked,
        v.fixed_rate as cutoffFixedRate,
        netFixedRateLocked,
        UNIX_SECONDS(CURRENT_TIMESTAMP()) as cutoffTimestamp,
        v.maturityTimestamp,
        cashflowLiFactor,
        liquidity_index as liquidityIndexCutOff,
        cashflowTimeFactor,
        cashflowFreeTerm,
        realizedPnLFromFeesPaid,
        fixedTokenBalance,
        variableTokenBalance
    FROM `risk-monitoring-361911.voltz_v1_positions.Voltz V1 Positions Production 120623`  p
    join pools_details v
    on p.vammAddress = v.vamm and p.chainId = v.chainId
    ),

    margin_table as (
    SELECT
    ownerAddress as marginOwner,
    tickLower,
    tickUpper,
    chainId,
    vammAddress,
    sum(marginDelta) as margin 
    FROM `risk-monitoring-361911.voltz_v1_positions.Voltz V1 Margin Updates Production 120623`
    group by ownerAddress, tickLower, tickUpper, chainId, vammAddress
    ), 

    positions as (
    Select 
    netNotionalLocked * (cutoffFixedRate - netFixedRateLocked) * (maturityTimestamp - cutoffTimestamp)/ (86400 * 365) as unrealizedPnl, 
    cashflowLiFactor * liquidityIndexCutOff + cashflowTimeFactor * cutoffTimestamp / (86400 * 365) + cashflowFreeTerm as realizedPnl,
    realizedPnLFromFeesPaid,
    owneraddress,
    margin,
    m.tickLower,
    m.tickUpper,
    d.chainId,
    d.vammAddress,
    fixedTokenBalance,
    variableTokenBalance
    from details d
    left join margin_table m
    on marginOwner = owneraddress and m.tickUpper = d.tickUpper and m.tickLower = d.tickLower 
        and d.chainId = m.chainId and d.vammAddress = m.vammAddress
    ),

    lp_fees as (
    select accumulatedFees, ownerAddress, chainId, tickLower, tickUpper, vamm
    from `risk-monitoring-361911.voltz_v1_positions.Voltz V1 LP Fees 270623` f
    where updateTimestamp >= (select max(t.updateTimestamp) - 3600 from `risk-monitoring-361911.voltz_v1_positions.Voltz V1 LP Fees 270623` t)
    ),

    positions_with_lp_fees as (
    select 
    p.owneraddress,
    p.tickLower,
    p.tickUpper,
    p.chainId,
    p.vammAddress as vammAddress,
    unrealizedPnl, 
    realizedPnl,
    -realizedPnLFromFeesPaid as feesPaid,
    case 
        when accumulatedFees IS NULL then 0
        else accumulatedFees
        end as feesGained,
    margin as depositedMargin,
    case 
        when accumulatedFees IS NULL then margin + realizedPnLFromFeesPaid
        else margin + realizedPnLFromFeesPaid + accumulatedFees
    end as availableMargin,
    fixedTokenBalance,
    variableTokenBalance
    from positions p
    left join lp_fees f
    on f.ownerAddress = p.owneraddress and f.chainId = p.chainId and f.tickLower = p.tickLower and f.tickUpper = p.tickUpper and p.vammAddress = f.vamm
    ) 

    select *
    from positions_with_lp_fees 
    order by chainId, vammAddress, unrealizedPnl desc""")

    results = query_job.result().to_dataframe()
    timestampstr = datetime.now().strftime("%Y%m%d-%H%M")
    results.to_csv(f"monitoring-data/positions-{timestampstr}.csv")
    return timestampstr
