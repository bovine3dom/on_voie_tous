# uv run python
import polars as pl
import numpy as np
from catboost import Pool, CatBoostClassifier#, cv

# 1. Load the data
df = pl.read_ipc('sncf-test.arrow')

# go through the table and for every day, group by train number and station, take the last platform and set it as the predicted platform
df.sort(["trainNumber", "station", "timestamp"])
THRESHOLD_SECONDS = 60 * 60 * 12 # 12 hour windows
df = df.with_columns(
    gap = pl.col("timestamp").diff().over("trainNumber", "station").fill_null(0),
    is_new_session = pl.col("timestamp").diff().over("trainNumber", "station").fill_null(0) > THRESHOLD_SECONDS
)
df = df.with_columns(
    session_id = pl.col("is_new_session").cum_sum().over("trainNumber", "station")
)
df = df.with_columns(
    actualPlatform = pl.col("predictedPlatform")
        .filter(
            (pl.col("predictedPlatform").is_not_null()) &
            (pl.col("predictedPlatform") != "")
        )
        .last()
        .over("trainNumber", "station", "session_id")
)
df = df.filter(pl.col("actualPlatform").is_not_null())
df = df.drop(["gap", "is_new_session", "session_id"]).fill_null("MISSING")

cat_features = [0, 4, 5, 6, 7, 8, 9, 10, 11, 12]

# exponential decay on age
max_ts = df["timestamp"].max()
half_life = 60 * 60 * 24 * 30 # monthly-ish (nb: presumably everything will break on timezone change)
decay = np.log(2) / half_life
df = df.with_columns(
    diff = (pl.col("timestamp").cast(pl.Float64) - float(max_ts))
)

df = df.with_columns(
    weight = (pl.col("diff") * decay).exp()
)
df = df.drop("diff")

full_pool = Pool(data=df.drop(["actualPlatform", "weight"]), label=df.select(["actualPlatform"]), cat_features=cat_features, weight=df["weight"])
# params = {
#     'iterations': 100,
#     'learning_rate': 0.1,
#     'depth': 6,
#     'loss_function': 'MultiClass',
#     'eval_metric': 'Accuracy',
#     'verbose': True,
#     'random_seed': 1337
# }
# 
# cv_data = cv(
#     pool=full_pool,
#     params=params,
#     fold_count=5,
#     shuffle=False, # preserve time order
#     stratified=False # ditto
# )
# 
# print(cv_data.tail())

# 87% that'll do

model = CatBoostClassifier(iterations=5, learning_rate=0.1, random_seed=1337, verbose=True)
model.fit(full_pool)
model.save_model("sncf_model.cbm", format="cbm")
