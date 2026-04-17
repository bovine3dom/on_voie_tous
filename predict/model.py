# uv run python
import polars as pl
import numpy as np
from catboost import Pool, CatBoostClassifier, cv

# 1. Load the data
df = pl.read_ipc('sncf-tiny.arrow', memory_map=True) # ... memory mapping fails for some reason
# df = pl.read_ipc('sncf-big.arrow', memory_map=True) # ... memory mapping fails for some reason

cat_features = [0, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]

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
full_pool = Pool(data=df.drop(["actualPlatform"]), label=df.select(["actualPlatform"]), cat_features=cat_features)
params = {
    'iterations': 100,
    'learning_rate': 0.1,
    'depth': 6,
    'loss_function': 'MultiClass',
    'eval_metric': 'Accuracy',
    'verbose': True,
    'random_seed': 1337
}

cv_data = cv(
    pool=full_pool,
    params=params,
    fold_count=5,
    shuffle=False, # preserve time order. nb: make sure you don't sort by station(!)
    stratified=False # ditto
)

pl.Config(tbl_rows=200)
print(cv_data.tail())

# 90% that'll do

model = CatBoostClassifier(iterations=70, learning_rate=0.1, random_seed=1337, verbose=True)
model.fit(full_pool)
model.save_model("sncf_model.cbm", format="cbm")
