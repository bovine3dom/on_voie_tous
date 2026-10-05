using Arrow, CodecZstd, Serialization
include("html.jl")

struct Snapshot
    timestamp::Int64
    lead::Float64
    number::String
    destination::String
    carrier::String
    category::String
    delay::Int
    platform::String
end

mutable struct Departure
    early::NTuple{4,Union{Nothing,Snapshot}}
    samples::Dict{Int64,Snapshot}
    status::Tuple{Int64,Bool,Bool}
end
Departure() = Departure((nothing, nothing, nothing, nothing), Dict{Int64,Snapshot}(), (typemin(Int64), false, false))
const Departures = Dict{Tuple{String,Date,String,Int},Departure}

excluded(state) = state.status[2] || state.status[3]
status!(state, at, cancelled, bus=false) = (state.status = max(state.status, (at, cancelled, bus)))
# Break equal-time ties consistently across archive order and worker partitions.
snapshot_key(x) = (x.timestamp, x.platform, x.number, x.destination, x.carrier, x.category, x.delay, x.lead)

function snapshot!(state, sample)
    old = get(state.samples, sample.timestamp, nothing)
    if isnothing(old) || snapshot_key(sample) > snapshot_key(old)
        state.samples[sample.timestamp] = sample
    end
    for (i, horizon) in enumerate(HORIZONS)
        horizon <= sample.lead <= horizon + TOLERANCE || continue
        old = state.early[i]
        if isnothing(old) || snapshot_key(sample) > snapshot_key(old)
            state.early = Base.setindex(state.early, sample, i)
        end
    end
end

function target(state)
    excluded(state) && return nothing
    known = [x for x in values(state.samples) if !isempty(x.platform)]
    isempty(known) && return nothing
    last = argmax(x -> x.timestamp, known)
    last.timestamp, last.platform
end

function labelled_samples(state, label_at)
    horizons = Dict(x.timestamp => h for (h, x) in zip(HORIZONS, state.early) if !isnothing(x))
    [(get(horizons, x.timestamp, -1), x) for x in sort!(collect(values(state.samples)); by=x -> x.timestamp)
     if x.timestamp < label_at]
end

function merge_departures!(a, b)
    for (key, incoming) in b
        state = get!(Departure, a, key)
        state.status = max(state.status, incoming.status)
        foreach(sample -> snapshot!(state, sample), values(incoming.samples))
    end
    a
end

function training_line!(runs, line, stations, times)
    header = match(ROOT, line)
    isnothing(header) && error("Unsupported archive envelope")
    ts, station = header.captures
    station in stations || return
    endswith(ts, "Z") || endswith(ts, "+00:00") || error("Collection time is not UTC")
    occursin("01/01/0001", line) && return
    occursin("bodyTabId", line) || return
    isempty(something(textcell(NAME, line), "")) && return
    at = DateTime(ts[1:19], dateformat"yyyy-mm-ddTHH:MM:SS")
    now = Int64(datetime2unix(at))
    local_now = astimezone(ZonedDateTime(at, tz"UTC"), ROME)
    today = Date(local_now)
    local_clock = hour(local_now) * 60 + minute(local_now)
    for row in eachmatch(ROW, line)
        body = row[1]
        number = textcell(NUMBER, body)
        isnothing(number) && continue
        isempty(number) && continue
        clock_match = match(CLOCK, body)
        isnothing(clock_match) && continue
        hour_value, minute_value = parse.(Int, clock_match.captures)
        0 <= hour_value < 24 && 0 <= minute_value < 60 || continue
        clock = 60hour_value + minute_value
        day = today + Day(clock - local_clock < -720 ? 1 : clock - local_clock > 720 ? -1 : 0)
        scheduled = scheduled_utc(day, clock, times)
        isfinite(scheduled) || continue
        delay_text = something(textcell(DELAY, body), "")
        is_cancelled = cancelled(delay_text)
        delay = isempty(delay_text) ? 0 : tryparse(Int, delay_text)
        lead = (scheduled - now) / 60
        carrier = something(textcell(CARRIER, body), "")
        category = something(textcell(CATEGORY, body), "")
        bus = bus_service(carrier, category)
        destination = something(textcell(DESTINATION, body), "")
        run = match(RUN, body)
        token = isnothing(run) ? join((number, carrier, destination), "|") : String(run[1])
        platform = textcell(PLATFORM, body)
        isnothing(platform) && !is_cancelled && continue
        state = get!(Departure, runs, (String(station), day, token, clock))
        status!(state, now, is_cancelled, bus)
        (is_cancelled || bus) && continue
        snapshot!(state, Snapshot(now, lead, number, destination, carrier, category,
                                  something(delay, -1), platform_known(platform) ? platform : ""))
    end
end

function training_file(path, stations, cache_dir)
    fingerprint = (abspath(path), filesize(path), mtime(path), string(VERSION), 3, sort!(collect(stations)))
    cache = joinpath(cache_dir, "v3-" * basename(path) * ".jls")
    if isfile(cache)
        saved = deserialize(cache)
        saved.fingerprint == fingerprint && return saved.runs, true
    end
    runs = Departures()
    times = Dict{Tuple{Date,Int},Float64}()
    open(path) do raw
        stream = ZstdDecompressorStream(raw; windowLogMax=Int32(28), bufsize=1 << 20)
        try
            for line in eachline(stream)
                training_line!(runs, line, stations, times)
            end
        finally
            close(stream)
        end
    end
    temporary = cache * ".tmp." * string(getpid())
    serialize(temporary, (; fingerprint, runs))
    mv(temporary, cache; force=true)
    runs, false
end

function training_rows(runs)
    rows = Dict{String,Vector{NamedTuple}}()
    for ((station, day, token, clock), state) in sort!(collect(runs); by=first)
        label = target(state)
        isnothing(label) && continue
        label_at, platform = label
        samples = labelled_samples(state, label_at)
        isempty(samples) && continue
        scheduled = Int64(scheduled_utc(day, clock, Dict{Tuple{Date,Int},Float64}()))
        departure_id, service_date = join((day, token, clock), "|"), string(day)
        for (horizon, sample) in samples
            row = (station=station, departureId=departure_id, serviceDate=service_date,
                   timestamp=sample.timestamp, scheduledTime=scheduled, labelTimestamp=label_at,
                   horizon=horizon, predictedPlatform=platform_known(sample.platform) ? sample.platform : "MISSING",
                   trainNumber=sample.number, predictedDestination=sample.destination,
                   carrier=sample.carrier, trainType=sample.category, delayMinutes=sample.delay,
                   scheduledMinute=clock, dayOfWeek=dayofweek(day), month=month(day),
                   leadMinutes=sample.lead, weight=1.0 / length(samples), actualPlatform=platform)
            push!(get!(() -> NamedTuple[], rows, station), row)
        end
    end
    rows
end

function wrangle(args=ARGS)
    isempty(args) && error("Usage: wrangler.jl ARCHIVE_DIRECTORY [--stations=PATH] [--output=PATH] [--limit=N]")
    options = Dict{String,String}()
    for argument in args[2:end]
        key, value = split(argument, '='; limit=2)
        key in ("--stations", "--output", "--limit") || error("Unknown option: $key")
        options[key] = value
    end
    stations = Set(strip.(readlines(get(options, "--stations", joinpath(@__DIR__, "stations.txt")))))
    !isempty(stations) && all(x -> occursin(r"^\d+$", x), stations) || error("Invalid station list")
    files = sort!(filter(x -> endswith(x, ".zst"), readdir(first(args); join=true)))
    limit = parse(Int, get(options, "--limit", string(length(files))))
    limit > 0 || error("Limit must be positive")
    files = files[1:min(limit, length(files))]
    isempty(files) && error("No archives found")
    cache_dir = joinpath(@__DIR__, ".cache", "training-v3")
    mkpath(cache_dir)
    workers = min(Threads.nthreads(), length(files))
    chunks = [Departures() for _ in 1:workers]
    done, hits = Threads.Atomic{Int}(0), Threads.Atomic{Int}(0)
    log_lock = ReentrantLock()
    started = time()
    Threads.@threads for worker in 1:workers
        for i in worker:workers:length(files)
            runs, cached = training_file(files[i], stations, cache_dir)
            merge_departures!(chunks[worker], runs)
            cached && Threads.atomic_add!(hits, 1)
            completed = Threads.atomic_add!(done, 1) + 1
            lock(log_lock) do
                println(stderr, "[$completed/$(length(files))] $(basename(files[i])) $(cached ? "cached" : "scanned")")
                flush(stderr)
            end
        end
    end
    runs = Departures()
    for chunk in chunks
        merge_departures!(runs, chunk)
        empty!(chunk)
    end
    empty!(chunks)
    GC.gc()
    rows = training_rows(runs)
    output = get(options, "--output", joinpath(@__DIR__, "hive"))
    mkpath(output)
    for station in stations
        directory = joinpath(output, "station=" * station)
        path = joinpath(directory, "part0.arrow")
        if haskey(rows, station)
            mkpath(directory)
            temporary = path * ".tmp"
            Arrow.write(temporary, rows[station]; file=true)
            mv(temporary, path; force=true)
        elseif isfile(path)
            rm(path)
        end
    end
    metadata = (; schema_version=2, archives=basename.(files), selected_stations=sort!(collect(stations)),
                 cached_archives=hits[], elapsed_seconds=round(time() - started; digits=2),
                 departures=length(runs), rows=sum(length, values(rows); init=0),
                 station_rows=Dict(s => length(get(rows, s, [])) for s in stations),
                 horizons=HORIZONS, parser_version=3, label_policy="last_known_platform")
    open(joinpath(output, "dataset.json"), "w") do io
        JSON3.pretty(io, JSON3.write(metadata))
    end
    println(JSON3.write((; metadata.rows, labelled_stations=length(rows))))
end

abspath(PROGRAM_FILE) == (@__FILE__) && wrangle()
