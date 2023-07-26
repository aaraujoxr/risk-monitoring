mr_query_string = """
  SELECT timestamp, chain_id, margin_engine, owner, tick_lower, tick_upper, tick, position_margin, fixed_token_balance, variable_token_balance, accrued_cashflow, initial_margin_requirement, liquidation_margin_requirement
  FROM (
    SELECT timestamp, chain_id, margin_engine, owner, tick_lower, tick_upper, tick, position_margin, fixed_token_balance, variable_token_balance, accrued_cashflow, position_requirement_initial as initial_margin_requirement, position_requirement_liquidation as liquidation_margin_requirement,block = (SELECT MAX(block) FROM `risk-monitoring-361911.risk_monitoring_scraper_v2.positions_info` WHERE positions_info.chain_id=chain_id) AS is_latest_block
    FROM `risk-monitoring-361911.risk_monitoring_scraper_v2.positions_info` AS positions_info
  )
  WHERE true
"""