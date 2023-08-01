def compute_summary_negative_statistics(identifier, path_to_save):
    import pandas as pd
    pool_specs = pd.read_csv("pool-specs.csv")

    positions_df = pd.read_csv(path_to_save + f"positions-{identifier}.csv")
    
    positions_df = positions_df.rename(columns={"availableMargin":"Initial balance",
                                              "realizedPnl": "rPnL",
                                              "unrealizedPnl": "uPnL"
                                             }
                                    )
    positions_df["Real balance"] = positions_df["Initial balance"]+positions_df["rPnL"]
    positions_df["Final balance"] = positions_df["Real balance"]+positions_df["uPnL"]
    positions_df["Leverage"] = positions_df["variableTokenBalance"]/positions_df["Final balance"]

    pools_dic = {}

    for item in positions_df.groupby(["chainId","vammAddress"]):
        try:
            ticker = pool_specs["ticker"][(pool_specs["chain_id"]==item[0][0])&
                                (pool_specs["vAMM_address"]==item[0][1])].values[0]
        except:
            ticker = item[0]
        pools_dic[ticker] = item[1]
    
    summary_df = pd.DataFrame(index=pools_dic.keys(),
                              columns=["Total notional exposure",
                                       "Total margin deposited",
                                       "Number positions",
                                       "No. overdrafts",
                                       "Total overdrafts",
                                       "No. pos. on default",
                                       "Shortfall"
                                      ])

    # insolvent_positions = {}
    # defaulted_positions = {}

    for pool in summary_df.index:
        dataset = pools_dic[pool]
        dataset = dataset.groupby(["owneraddress","tickLower","tickUpper"]).apply(lambda x: x[x["rowLastUpdatedTimestamp"] == x["rowLastUpdatedTimestamp"].max()].iloc[0])
          
        summary_df.loc[pool] = [dataset["netNotionalLocked"].sum(),
                                dataset["Initial balance"].sum(),
                                len(dataset),
                                (dataset["Real balance"]<0).sum(),
                                (dataset["Real balance"][dataset["Real balance"]<0]).sum(),
                                (dataset["Final balance"]<0).sum(),
                                (dataset["Final balance"][dataset["Final balance"]<0]).sum()
                               ]
        # insolvent_positions[pool] = dataset[dataset["Real balance"]<0]

        # defaulted_positions[pool] = dataset[dataset["Final balance"]<0]

    summary_df.to_excel(path_to_save + f"summary-{identifier}.xlsx")

    # with pd.ExcelWriter(path_to_save + f"overdrafts-{identifier}.xlsx") as writer:
    #     for pool in insolvent_positions.keys():
    #         insolvent_positions[pool].to_excel(writer, sheet_name=str(pool[1]))

    # with pd.ExcelWriter(path_to_save + f"defaults-{identifier}.xlsx") as writer:
    #     for pool in defaulted_positions.keys():
    #         defaulted_positions[pool].to_excel(writer, sheet_name=str(pool[1]))