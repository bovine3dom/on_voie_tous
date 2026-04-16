-- clickhouse-local
describe table 'data/2026-03-30T20:18:13Z.jsonl.zst';
select ts, arrayJoin(data) from 'data/2026-03-30T20:18:13Z.jsonl.zst' limit 10

-- no idea why i need to do arrayJoin but i do
select station, parseDateTimeBestEffort(ts) timestamp, parseDateTimeBestEffort(d.actualTime) predictedTime, parseDateTimeBestEffort(d.scheduledTime) scheduledTime, concat(ifNull(d.platform.trackGroupTitle, ''), ifNull(d.platform.trackGroupValue, ''), ifNull(d.platform.track,'')) predictedPlatform, d.traffic.destination predictedDestination, d.traffic.origin predictedOrigin, d.traffic.oldDestination scheduledDestination, d.traffic.oldOrigin scheduledOrigin, d.trainLine trainLine, d.trainMode trainMode, d.trainNumber trainNumber, d.trainType trainType
from (
    --select station, ts, arrayJoin(data) d from 'data/2026-03-30T20:18:13Z.jsonl.zst'
    select station, ts, arrayJoin(data) d from 'data/*.jsonl.zst'
    where station = '0087756056'
)
into outfile 'sncf-test.arrow' truncate settings output_format_arrow_compression_method = 'none';

-- -- huh, stops is always empty, weird.
-- select d.stops from (
--     select station, arrayJoin(data) d, d.stops from 'data/2026-03-30T20:18:13Z.jsonl.zst'
--     where length(d.stops) > 0
--     limit 100
-- )
