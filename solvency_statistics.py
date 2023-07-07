def compute_summary_negative_statistics(identifier):
    import pandas as pd
    from definitions import pool_addresses

    positions_df = pd.read_csv(f"monitoring-data/positions-{identifier}.csv")
    
    positions_df = positions_df.rename(columns={"availableMargin":"Initial balance",
                                              "realizedPnl": "rPnL",
                                              "unrealizedPnl": "uPnL"
                                             }
                                    )
    positions_df["Real balance"] = positions_df["Initial balance"]+positions_df["rPnL"]
    positions_df["Final balance"] = positions_df["Real balance"]+positions_df["uPnL"]
    positions_df["Leverage"] = positions_df["variableTokenBalance"]/positions_df["Final balance"]

    positions = {chain:
             {pool: positions_df[positions_df["vammAddress"]==pool_addresses[chain][pool]]
              for pool in pool_addresses[chain]
             }
             for chain in pool_addresses.keys()
            }
    
    multiindex = []
    for chain in pool_addresses.keys():
        for pool in pool_addresses[chain].keys():
            multiindex.append((chain, pool))

    multiindex = pd.MultiIndex.from_tuples(multiindex)

    summary_df = pd.DataFrame(index=multiindex,
                            columns=["Number positions", "No. overdrafts", "Total overdrafts",
                                    "No. pos. on default", "Shortfall"])
    
    insolvent_positions = {}
    defaulted_positions = {}

    for pool in summary_df.index:
        dataset = positions[pool[0]][pool[1]]#[["Initial balance", "rPnL", "Balance after rPnL sttlm", "uPnL", "Final balance", "Leverage"]]
                                    
        summary_df.loc[pool] = [len(dataset), (dataset["Real balance"]<0).sum(),
                            (dataset["Real balance"][dataset["Real balance"]<0]).sum(),
                            (dataset["Final balance"]<0).sum(),
                            (dataset["Final balance"][dataset["Final balance"]<0]).sum()
                            ]
        insolvent_positions[pool] = dataset[dataset["Real balance"]<0]

        defaulted_positions[pool] = dataset[dataset["Final balance"]<0]

    summary_df.to_excel(f"monitoring-data/summary-{identifier}.xlsx")

    with pd.ExcelWriter(f"monitoring-data/overdrafts-{identifier}.xlsx") as writer:
        for pool in insolvent_positions.keys():
            insolvent_positions[pool].to_excel(writer, sheet_name=str(pool[1]))

    with pd.ExcelWriter(f"monitoring-data/defaults-{identifier}.xlsx") as writer:
        for pool in defaulted_positions.keys():
            defaulted_positions[pool].to_excel(writer, sheet_name=str(pool[1]))