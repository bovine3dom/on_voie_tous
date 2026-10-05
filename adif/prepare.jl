module ADIF
using Arrow, CodecZstd, Dates, JSON3, Serialization, TimeZones

module Availability
include("../rfi/audit.jl")
end
module Training
include("../rfi/wrangler.jl")
end

const HORIZONS = Training.HORIZONS
const MADRID = tz"Europe/Madrid"
const Key = Tuple{String,Date,String,Int}
const PARSER_VERSION = 5
const Departure = Training.Departure

struct Scan
    availability::Availability.Scan
    runs::Dict{Key,Departure}
    statistics::Dict{String,Int}
end
Scan() = Scan(Availability.Scan(), Dict{Key,Departure}(), Dict{String,Int}())
count!(scan, key, n=1) = (scan.statistics[key] = get(scan.statistics, key, 0) + n)
text(object, key, default="") = isnothing(get(object, key, default)) ? default : string(get(object, key, default))
items(object, key) = get(object, key, nothing) isa AbstractVector ? object[key] : []

function instant(value)
    value isa AbstractString || return nothing
    match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$", value) === nothing && return nothing
    try
        at = Int64(datetime2unix(DateTime(value[1:19], dateformat"yyyy-mm-ddTHH:MM:SS")))
        endswith(value, "Z") && return at
        offset = last(value, 6)
        hours, minutes = parse(Int, offset[2:3]), parse(Int, offset[5:6])
        hours < 24 && minutes < 60 || return nothing
        at - (offset[1] == '+' ? 1 : -1) * (3600hours + 60minutes)
    catch error
        error isa ArgumentError || rethrow()
        nothing
    end
end

localtime(at) = astimezone(ZonedDateTime(unix2datetime(at), tz"UTC"), MADRID)
platform(value) = uppercase(strip(value)) in ("", "-", "--", "—", "?", "N/A", "ND", "N.D.", "SIN ASIGNAR") ? "" : strip(value)

function merge!(a, b)
    Availability.merge_scan!(a.availability, b.availability)
    Training.merge_departures!(a.runs, b.runs)
    for (key, value) in b.statistics
        count!(a, key, value)
    end
    a
end

function board!(scan, board, now, times, dates)
    if !(board isa AbstractDict)
        count!(scan, "invalid_payloads")
        return
    end
    settings = get(board, :station_settings, nothing)
    trains = get(board, :trains, nothing)
    if !(settings isa AbstractDict) || !(trains isa AbstractVector)
        count!(scan, "invalid_payloads")
        return
    end
    id = text(settings, :code)
    occursin(r"^[0-9]{1,12}$", id) || (count!(scan, "invalid_station_ids"); return)
    at = unix2datetime(now)
    station = get!(scan.availability.stations, id) do
        Availability.Station(text(settings, :name), at)
    end
    station.boards += 1
    station.first = min(station.first, at)
    if at >= station.last
        station.last = at
        station.name = text(settings, :name, station.name)
    end
    for train in trains
        station.rows += 1
        if !(train isa AbstractDict)
            station.unparsed += 1
            continue
        end
        for field in (:status, :class_stop, :traffic_type)
            count!(scan, string(field, ":", text(train, field)))
        end
        stop = text(train, :class_stop)
        if stop == "destination"
            count!(scan, "excluded_arrivals")
            continue
        elseif !(stop in ("origin", "intermediate"))
            count!(scan, "unsupported_stop_types")
            continue
        end
        number = text(train, :technical_number_planif_out)
        isempty(number) && (number = text(train, :technical_number_planif))
        scheduled_text = text(train, :departure_time)
        scheduled = get!(times, scheduled_text) do
            instant(scheduled_text)
        end
        if isempty(number) || isnothing(scheduled) || !haskey(train, :platform)
            station.unparsed += 1
            continue
        end
        lead = (scheduled - now) / 60
        delay = tryparse(Int, text(train, :delay_out, "0"))
        delay = something(delay, -1)
        early = any(h -> h <= lead <= h + Training.TOLERANCE, HORIZONS)
        cancelled = occursin(r"(?i)cancel|suprimid|anulad", text(train, :status) * " " * text(train, :observation))
        carrier = text(train, :company)
        category = join(sort!(unique([text(product, :product) for product in items(train, :commercial_id)])), "|")
        bus = text(train, :traffic_type) == "B" || startswith(uppercase(number), "BUS") ||
              uppercase(strip(text(train, :platform))) == "BUS" || Training.bus_service(carrier, category)
        day = get!(dates, scheduled) do
            Date(localtime(scheduled))
        end
        key = (id, day, number, scheduled)
        state = get!(Departure, scan.runs, key)
        Training.status!(state, now, cancelled, bus)
        if cancelled || bus
            station.cancelled += cancelled
            station.bus += bus
            continue
        end
        value = platform(text(train, :platform))
        known = !isempty(value)
        if early
            audit_state = get!(Availability.RunState, scan.availability.runs, key)
            Availability.observe!(audit_state, lead, known)
        end
        destination = join(sort!(unique([text(item, :code) for item in items(train, :destinations)])), "|")
        Training.snapshot!(state, Training.Snapshot(now, lead, number, destination, carrier, category, delay, value))
    end
end

function line!(scan, line, times, dates)
    envelope = JSON3.read(line)
    message = envelope.data
    now = instant(envelope.ts)
    isnothing(now) && error("Unsupported collection timestamp")
    payloads = get(message, :target, "") == "ReceiveMessage" ? get(message, :arguments, []) :
               get(message, :result, nothing) isa AbstractString ? [message.result] : []
    isempty(payloads) && (count!(scan, "control_messages"); return)
    for value in payloads
        if isnothing(value) || value == ""
            count!(scan, "empty_payloads")
            continue
        end
        value isa AbstractString || (count!(scan, "invalid_payloads"); continue)
        decoded = value
        for _ in 1:2
            decoded isa AbstractString && !isempty(decoded) || break
            decoded = JSON3.read(decoded)
        end
        if isnothing(decoded) || decoded == ""
            count!(scan, "empty_payloads")
            continue
        end
        board!(scan, decoded, now, times, dates)
    end
end

function scan_file(path, cache_dir)
    fingerprint = (abspath(path), filesize(path), mtime(path), string(VERSION), PARSER_VERSION)
    cache = joinpath(cache_dir, "v$(PARSER_VERSION)-" * basename(path) * ".jls")
    if isfile(cache)
        saved = deserialize(cache)
        saved.fingerprint == fingerprint && return saved.scan, true
    end
    scan = Scan()
    times = Dict{String,Union{Nothing,Int64}}()
    dates = Dict{Int64,Date}()
    open(path) do raw
        stream = ZstdDecompressorStream(raw; windowLogMax=Int32(28), bufsize=1 << 20)
        try
            for line in eachline(stream)
                line!(scan, line, times, dates)
            end
        finally
            close(stream)
        end
    end
    temporary = cache * ".tmp." * string(getpid())
    serialize(temporary, (; fingerprint, scan))
    mv(temporary, cache; force=true)
    scan, false
end

function revisions(scan, lead)
    index = only(findall(==(lead), HORIZONS))
    result = Dict{Tuple{String,String},Vector{Int}}()
    for ((id, day, _, _), state) in scan.runs
        label = Training.target(state)
        sample = state.early[index]
        isnothing(label) && continue
        isnothing(sample) && continue
        sample.timestamp >= label[1] && continue
        for period in ("all", Dates.format(day, dateformat"yyyy-mm"))
            n = get!(() -> zeros(Int, 3), result, (id, period))
            n[1] += 1
            if !isempty(sample.platform)
                n[2] += 1
                n[3] += sample.platform != label[2]
            end
        end
    end
    result
end

function rows(scan, selected)
    result = Dict{String,Vector{NamedTuple}}()
    for ((id, day, number, scheduled), state) in sort!(collect(scan.runs); by=first)
        id in selected || continue
        label = Training.target(state)
        isnothing(label) && continue
        received, value = label
        samples = Training.labelled_samples(state, received)
        isempty(samples) && continue
        local_at = localtime(scheduled)
        for (horizon, sample) in samples
            row = (station=id, departureId=join((day, number, scheduled), "|"), serviceDate=string(day),
                   timestamp=sample.timestamp, scheduledTime=scheduled, labelTimestamp=received,
                   horizon=horizon, predictedPlatform=isempty(sample.platform) ? "MISSING" : sample.platform,
                   trainNumber=number, predictedDestination=sample.destination, carrier=sample.carrier,
                   trainType=sample.category, delayMinutes=sample.delay, scheduledMinute=hour(local_at)*60+minute(local_at),
                   dayOfWeek=dayofweek(day), month=month(day), leadMinutes=sample.lead,
                   weight=1.0/length(samples), actualPlatform=value)
            push!(get!(() -> NamedTuple[], result, id), row)
        end
    end
    result
end

function reports(scan, files, output, lead, minimum, rate, elapsed, hits; minimums=Dict{String,Int}())
    mkpath(output)
    filter!(scan.availability.runs) do (key, _)
        state = scan.runs[key]
        !Training.excluded(state)
    end
    totals = Availability.counts(scan.availability)
    later = revisions(scan, lead)
    index = only(findall(==(lead), HORIZONS))
    lists = Dict(label => String[] for label in ("needs_predictions", "skip", "insufficient_data"))
    station_rows = []
    for id in sort!(collect(keys(scan.availability.stations)); by=x -> parse(Int, x))
        station = scan.availability.stations[id]
        periods = [(p, value) for ((s, p), value) in totals if s == id]
        station_minimum = get(minimums, id, minimum)
        label, basis = Availability.classify(periods, index, station_minimum, rate)
        reason = label == "needs_predictions" ? "missing_platforms" : ""
        if label == "skip" && station.unparsed > station.rows/100
            label, basis, reason = "insufficient_data", "", "parsing_failures"
        end
        push!(lists[label], id)
        n, m = get(totals, (id, "all"), (zeros(Int, 4), zeros(Int, 4)))
        labelled, published, changed = get(later, (id, "all"), zeros(Int, 3))
        push!(station_rows, vcat([id, station.name, label, reason, basis, station_minimum, station.boards, station.invalid,
                                 station.rows, station.cancelled, station.bus, station.unparsed],
                                Availability.metric_cells(n, m),
                                [labelled, published, changed, published == 0 ? "" : round(100changed/published; digits=3)]))
    end
    metrics = [string(field, "_", h) for h in HORIZONS for field in ("eligible", "missing", "missing_pct")]
    revision_fields = ["labelled_departures", "published_labelled", "platform_revisions", "revision_pct"]
    Availability.writecsv(joinpath(output, "stations.csv"),
        vcat(["station_id", "station_name", "classification", "reason", "evidence_period", "minimum_departures", "boards", "invalid_boards",
              "rows", "excluded_cancelled", "excluded_bus", "unparsed_rows"], metrics, revision_fields), station_rows)
    monthly = [vcat([id, scan.availability.stations[id].name, period], Availability.metric_cells(n, m),
                   get(later, (id, period), zeros(Int, 3)))
               for ((id, period), (n, m)) in sort!(collect(totals); by=first) if period != "all"]
    Availability.writecsv(joinpath(output, "monthly.csv"),
        vcat(["station_id", "station_name", "month"], metrics, revision_fields[1:3]), monthly)
    for (label, ids) in lists
        open(joinpath(output, label * ".txt"), "w") do io
            foreach(id -> println(io, id), ids)
        end
    end
    metadata = (; operator="adif", parser_version=PARSER_VERSION, lead_minutes=lead, tolerance_minutes=Training.TOLERANCE,
                 minimum_departures=minimum, minimum_overrides=minimums, missing_rate_threshold=rate,
                 archives=length(files), compressed_bytes=sum(filesize, files),
                 first_observation=isempty(scan.availability.stations) ? nothing : Availability.minimum_datetime(scan.availability),
                 last_observation=isempty(scan.availability.stations) ? nothing : Availability.maximum_datetime(scan.availability),
                 elapsed_seconds=round(elapsed; digits=2), cached_archives=hits,
                 classifications=Dict(k => length(v) for (k, v) in lists), statistics=scan.statistics,
                 service_dates=sort!(unique([string(key[2]) for key in keys(scan.availability.runs)])), files=basename.(files))
    open(joinpath(output, "audit.json"), "w") do io
        JSON3.pretty(io, JSON3.write(metadata))
    end
    println(JSON3.write(metadata.classifications))
    Set(lists["needs_predictions"])
end

function prepare(args=ARGS)
    isempty(args) && error("Usage: prepare.jl ARCHIVE_DIRECTORY [--min=100] [--minimums=PATH] [--rate=0.1] [--lead=30] [--output=PATH] [--limit=N]")
    options = Dict{String,String}()
    for argument in args[2:end]
        key, value = split(argument, '='; limit=2)
        key in ("--min", "--minimums", "--rate", "--lead", "--output", "--limit") || error("Unknown option: $key")
        options[key] = value
    end
    minimum = parse(Int, get(options, "--min", "100"))
    rate = parse(Float64, get(options, "--rate", "0.1"))
    lead = parse(Int, get(options, "--lead", "30"))
    minimum > 0 && 0 < rate <= 1 && lead in HORIZONS || error("Invalid selection thresholds")
    minimums_path = get(options, "--minimums", joinpath(@__DIR__, "minimum_departures.json"))
    haskey(options, "--minimums") && !isfile(minimums_path) && error("Override file does not exist")
    minimums = isfile(minimums_path) ? JSON3.read(read(minimums_path, String), Dict{String,Int}) : Dict{String,Int}()
    all(>(0), values(minimums)) || error("Minimum overrides must be positive")
    files = sort!(filter(x -> endswith(x, ".zst"), readdir(first(args); join=true)))
    limit = parse(Int, get(options, "--limit", string(length(files))))
    limit > 0 || error("Limit must be positive")
    files = files[1:min(limit, length(files))]
    isempty(files) && error("No archives found")
    output = get(options, "--output", @__DIR__)
    cache_dir = joinpath(@__DIR__, ".cache", "v$(PARSER_VERSION)")
    mkpath(cache_dir)
    workers = min(Threads.nthreads(), length(files))
    chunks = [Scan() for _ in 1:workers]
    done, hits = Threads.Atomic{Int}(0), Threads.Atomic{Int}(0)
    log_lock = ReentrantLock()
    started = time()
    Threads.@threads for worker in 1:workers
        for i in worker:workers:length(files)
            scan, cached = scan_file(files[i], cache_dir)
            merge!(chunks[worker], scan)
            cached && Threads.atomic_add!(hits, 1)
            completed = Threads.atomic_add!(done, 1) + 1
            lock(log_lock) do
                println(stderr, "[$completed/$(length(files))] $(basename(files[i])) $(cached ? "cached" : "scanned")")
                flush(stderr)
            end
        end
    end
    scan = Scan()
    foreach(chunk -> merge!(scan, chunk), chunks)
    selected = reports(scan, files, joinpath(output, "results"), lead, minimum, rate, time()-started, hits[]; minimums)
    station_rows = rows(scan, selected)
    hive = joinpath(output, "hive")
    mkpath(hive)
    for id in keys(scan.availability.stations)
        path = joinpath(hive, "station="*id, "part0.arrow")
        if haskey(station_rows, id)
            mkpath(dirname(path))
            Arrow.write(path*".tmp", station_rows[id]; file=true)
            mv(path*".tmp", path; force=true)
        elseif isfile(path)
            rm(path)
        end
    end
    metadata = (; operator="adif", schema_version=2, parser_version=PARSER_VERSION,
                 archives=basename.(files), selected_stations=sort!(collect(selected)),
                 cached_archives=hits[], elapsed_seconds=round(time()-started; digits=2), departures=length(scan.runs),
                 rows=sum(length, values(station_rows); init=0), station_rows=Dict(s => length(get(station_rows, s, [])) for s in selected),
                 horizons=HORIZONS, label_policy="last_known_platform")
    open(joinpath(hive, "dataset.json"), "w") do io
        JSON3.pretty(io, JSON3.write(metadata))
    end
    println(JSON3.write((; metadata.rows, labelled_stations=length(station_rows))))
end
end

abspath(PROGRAM_FILE) == (@__FILE__) && ADIF.prepare()
