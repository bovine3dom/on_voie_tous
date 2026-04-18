#!/bin/julia
# only used for splitting up the data into manageable chunks for poor little catboost uwu :((

import Arrow
using DataFrames, HivePartitioner
#]add https://github.com/bovine3dom/HivePartitioner.jl

df = Arrow.Table("sncf-big.arrow") |> DataFrame
writehivedir((path, table)->Arrow.write(path, table), "sncf-hive", df, [:station]; filename="part0.arrow")


# # todo: port this
# exponential decay on age. bugger should have added this in CH too
# max_ts = df["timestamp"].max()
# half_life = 60 * 60 * 24 * 30 # monthly-ish (nb: presumably everything will break on timezone change)
# decay = np.log(2) / half_life
# df = df.with_columns(
#     diff = (pl.col("timestamp").cast(pl.Float64) - float(max_ts))
# )
#
# df = df.with_columns(
#     weight = (pl.col("diff") * decay).exp()
# )
# df = df.drop("diff")

# full_pool = Pool(data=df.drop(["actualPlatform", "weight"]), label=df.select(["actualPlatform"]), cat_features=cat_features, weight=df["weight"])
