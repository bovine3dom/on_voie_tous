-- clickhouse-local
describe table 'data/2026-03-30T20:18:13Z.jsonl.zst';
select ts, arrayJoin(data) from 'data/2026-03-30T20:18:13Z.jsonl.zst' limit 10

-- no idea why i need to do arrayJoin but i do
select station, parseDateTimeBestEffort(ts) timestamp, parseDateTimeBestEffort(d.actualTime) predictedTime, parseDateTimeBestEffort(d.scheduledTime) scheduledTime, concat(ifNull(d.platform.trackGroupTitle, ''), ifNull(d.platform.trackGroupValue, ''), ifNull(d.platform.track,'')) predictedPlatform, d.traffic.destination predictedDestination, d.traffic.origin predictedOrigin, d.traffic.oldDestination scheduledDestination, d.traffic.oldOrigin scheduledOrigin, d.trainLine trainLine, d.trainMode trainMode, d.trainNumber trainNumber, d.trainType trainType, d.informationStatus.trainStatus trainStatus
from (
    --select station, ts, arrayJoin(data) d from 'data/2026-03-30T20:18:13Z.jsonl.zst'
    select station, ts, arrayJoin(data) d from 'data/*.jsonl.zst'
    --where station = '0087756056'
)
into outfile 'sncf-big.arrow' truncate settings output_format_arrow_compression_method = 'none';
-- maybe it's worth deduplicating rows? only keeping the ... last or first? hard to know which is better?

-- -- huh, stops is always empty, weird.
-- select d.stops from (
--     select station, arrayJoin(data) d, d.stops from 'data/2026-03-30T20:18:13Z.jsonl.zst'
--     where length(d.stops) > 0
--     limit 100
-- )
select distinct on (station, predictedTime, scheduledTime, predictedPlatform, predictedDestination, predictedOrigin, scheduledDestination, scheduledOrigin, trainLine, trainMode, trainNumber, trainType, trainStatus) station, parseDateTimeBestEffort(ts) timestamp, parseDateTimeBestEffort(d.actualTime) predictedTime, parseDateTimeBestEffort(d.scheduledTime) scheduledTime,
if(d.platform.track is not null, arrayStringConcat(arrayFilter(x->x!='', [ifNull(d.platform.trackGroupTitle, ''), ifNull(d.platform.trackGroupValue, ''), ifNull(d.platform.track,'')]), ' '), '') predictedPlatform
    , d.traffic.destination predictedDestination, d.traffic.origin predictedOrigin, d.traffic.oldDestination scheduledDestination, d.traffic.oldOrigin scheduledOrigin, d.trainLine trainLine, d.trainMode trainMode, d.trainNumber trainNumber, d.trainType trainType, d.informationStatus.trainStatus trainStatus from (
    select station, ts, arrayJoin(data) d from 'data/2026-04-18T*Z.jsonl.zst'
    --where station = '0087756056'
)
-- : (in file/uri /home/olie/projects/on_voie_tous/predict/data/2026-04-18T04:51:13Z.jsonl.zst): While executing JSONEachRowRowInputFormat: While executing File. (CANNOT_READ_ARRAY_FROM_TEXT). -- cursed file. deleted for now

-- if accuracy is still rubbish, consider dumping cancelled trains
WITH base_data AS (
    SELECT DISTINCT ON (station, predictedTime, scheduledTime, predictedPlatform, predictedDestination, predictedOrigin, scheduledDestination, scheduledOrigin, trainLine, trainMode, trainNumber, trainType, trainStatus) 
        station, 
        parseDateTimeBestEffort(ts) timestamp, 
        parseDateTimeBestEffort(d.actualTime) predictedTime, 
        parseDateTimeBestEffort(d.scheduledTime) scheduledTime, 
        --if(d.platform.track is not null, arrayStringConcat(arrayFilter(x->x!='', [ifNull(d.platform.trackGroupTitle, ''), ifNull(d.platform.trackGroupValue, ''), ifNull(d.platform.track,'')]), ' '), '') predictedPlatform,
        ifNull(d.platform.track, '') predictedPlatform,
--        ifNull(d.platform.trackGroupTitle, 'None') predictedTrackGroupTitle, -- can we just ignore these for now? is that a bad idea?
--        ifNull(d.platform.trackGroupValue, 'None') predictedTrackGroupValue,
        d.traffic.destination predictedDestination, 
        d.traffic.origin predictedOrigin, 
        d.traffic.oldDestination scheduledDestination, 
        d.traffic.oldOrigin scheduledOrigin, 
        d.trainLine trainLine, 
        d.trainMode trainMode, 
        d.trainNumber trainNumber, 
        d.trainType trainType, 
        d.informationStatus.trainStatus trainStatus 
    FROM (
        SELECT station, ts, arrayJoin(data) d 
        FROM 'data/*.jsonl.zst'
--        WHERE station = '0087756056'
    )
),
session_gaps AS (
    SELECT *,
        toUnixTimestamp(timestamp) - toUnixTimestamp(
            ifNull(
                lagInFrame(timestamp) OVER (
                    PARTITION BY trainNumber, station 
                    ORDER BY timestamp ASC
                ), 
                timestamp
            )
        ) AS gap_seconds
    FROM base_data
),
session_ids AS (
    SELECT *,
        sum(if(gap_seconds > 60*60*12, 1, 0)) OVER ( -- 12 hour window
            PARTITION BY trainNumber, station 
            ORDER BY timestamp ASC 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS session_id
    FROM session_gaps
),
actual_platforms AS (
    SELECT *,
        argMax(
            predictedPlatform, 
            if(predictedPlatform != '', toUnixTimestamp(timestamp), -1)
        ) OVER (
            PARTITION BY trainNumber, station, session_id
        ) AS raw_actualPlatform
    FROM session_ids
)
SELECT 
    station,
    timestamp,
    predictedTime,
    scheduledTime,
    ifNull(predictedPlatform, '') AS predictedPlatform,
    ifNull(predictedDestination, '') AS predictedDestination,
    ifNull(predictedOrigin, '') AS predictedOrigin,
    ifNull(scheduledDestination, '') AS scheduledDestination,
    ifNull(scheduledOrigin, '') AS scheduledOrigin,
    ifNull(trainLine, '') AS trainLine,
    ifNull(trainMode, '') AS trainMode,
    ifNull(trainNumber, '') AS trainNumber,
    ifNull(trainType, '') AS trainType,
    ifNull(trainStatus, '') AS trainStatus,
    if(raw_actualPlatform = '', '!', raw_actualPlatform) AS actualPlatform
FROM actual_platforms
WHERE (trainStatus == 'SUPPRESSION_TOTALE' OR raw_actualPlatform != '')
--ORDER BY trainNumber, station, timestamp -- breaks cv stratification
ORDER BY timestamp
into outfile 'sncf-big.arrow' truncate settings output_format_arrow_compression_method = 'none';
