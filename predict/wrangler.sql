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
WITH parsed_data AS (
    SELECT 
        toLowCardinality(d.uic) AS station, -- don't trust our station, uic is what is used on frontend
        parseDateTimeBestEffort(ts) AS timestamp_raw, 
        parseDateTimeBestEffort(d.actualTime) AS predictedTime, 
        parseDateTimeBestEffort(d.scheduledTime) AS scheduledTime, 
        toLowCardinality(ifNull(d.platform.track, '')) AS predictedPlatform,
        toLowCardinality(ifNull(d.platform.trackGroupValue, '')) AS predictedTrackGroupValue,
        toLowCardinality(ifNull(d.platform.trackGroupTitle, '')) AS predictedTrackGroupTitle,
        toLowCardinality(ifNull(d.traffic.destination, '')) AS predictedDestination, 
        toLowCardinality(ifNull(d.traffic.origin, '')) AS predictedOrigin, 
        toLowCardinality(ifNull(d.traffic.oldDestination, '')) AS scheduledDestination, 
        toLowCardinality(ifNull(d.traffic.oldOrigin, '')) AS scheduledOrigin, 
        toLowCardinality(ifNull(d.trainLine, '')) AS trainLine, 
        toLowCardinality(ifNull(d.trainMode, '')) AS trainMode, 
        toLowCardinality(ifNull(d.trainNumber, '')) AS trainNumber, 
        toLowCardinality(ifNull(d.trainType, '')) AS trainType, 
        toLowCardinality(ifNull(d.informationStatus.trainStatus, '')) AS trainStatus 
    FROM (
        SELECT ts, arrayJoin(data) d 
        FROM 'data/*.jsonl.zst'
    )
),
base_data AS (
    SELECT 
        station, 
        arrayJoin(arrayDistinct([min(timestamp_raw), max(timestamp_raw)])) AS timestamp, 
        predictedTime, 
        scheduledTime, 
        predictedPlatform, 
        predictedTrackGroupValue, 
        predictedTrackGroupTitle,
        predictedDestination, 
        predictedOrigin, 
        scheduledDestination, 
        scheduledOrigin, 
        trainLine, 
        trainMode, 
        trainNumber, 
        trainType, 
        trainStatus 
    FROM parsed_data
    GROUP BY 
        station, 
        predictedTime, 
        scheduledTime, 
        predictedPlatform, 
        predictedTrackGroupValue, 
        predictedTrackGroupTitle,
        predictedDestination, 
        predictedOrigin, 
        scheduledDestination, 
        scheduledOrigin, 
        trainLine, 
        trainMode, 
        trainNumber, 
        trainType, 
        trainStatus
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
    CAST(station AS String) AS station,
    timestamp,
    predictedTime,
    scheduledTime,
    CAST(predictedPlatform AS String) AS predictedPlatform,
    CAST(predictedTrackGroupValue AS String) AS predictedTrackGroupValue,
    CAST(predictedTrackGroupTitle AS String) AS predictedTrackGroupTitle,
    CAST(predictedDestination AS String) AS predictedDestination,
    CAST(predictedOrigin AS String) AS predictedOrigin,
    CAST(scheduledDestination AS String) AS scheduledDestination,
    CAST(scheduledOrigin AS String) AS scheduledOrigin,
    CAST(trainLine AS String) AS trainLine,
    CAST(trainMode AS String) AS trainMode,
    CAST(trainNumber AS String) AS trainNumber,
    CAST(trainType AS String) AS trainType,
    CAST(trainStatus AS String) AS trainStatus,
    if(raw_actualPlatform = '', '!!!', CAST(raw_actualPlatform AS String)) AS actualPlatform
FROM actual_platforms
WHERE (trainStatus == 'SUPPRESSION_TOTALE' OR raw_actualPlatform != '')
ORDER BY timestamp
INTO OUTFILE 'sncf-big.arrow' TRUNCATE SETTINGS output_format_arrow_compression_method = 'none';
