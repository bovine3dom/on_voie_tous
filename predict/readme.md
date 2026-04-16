# grab data


```
rsync -rlt --include="*.zst" --include='*/' --exclude='*' --info=progress2 the_server:/mnt/chungus/slowjects/datagrabber/data/sncf-gares-connexions/ .
```

# train

```
uv run model.py
```


# predict

```
uv run predict.py
```


except not yet. prediction interface to be finalised. probably don't really want to send the entire response, should only get the bits we want

also todo: convert utc times to local times and hour, minute, day of week, day of month etc
