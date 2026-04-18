#!/bin/julia
# only used for splitting up the data into manageable chunks for poor little catboost uwu :((

import Arrow
using DataFrames, HivePartitioner
#]add https://github.com/bovine3dom/HivePartitioner.jl

df = Arrow.Table("sncf-big.arrow") |> DataFrame
writehivedir((path, table)->Arrow.write(path, table), "sncf-hive", df, [:station]; filename="part0.arrow")
