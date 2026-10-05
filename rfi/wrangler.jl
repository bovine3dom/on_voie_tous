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
    published::Bool
end

mutable struct Departure
    early::NTuple{4,Union{Nothing,Snapshot}}
    terminal::Vector{Tuple{Int64,String}}
    cancelled::Bool
    conflict::Bool
end
Departure() = Departure((nothing, nothing, nothing, nothing), Tuple{Int64,String}[], false, false)
const Departures = Dict{Tuple{String,Date,String,Int},Departure}

function terminal!(state, at, platform)
    same = findfirst(x -> x[1] == at, state.terminal)
    if !isnothing(same)
        state.conflict |= state.terminal[same][2] != platform
        return
    end
    push!(state.terminal, (at, platform))
    sort!(state.terminal; by=first, rev=true)
    resize!(state.terminal, min(2, length(state.terminal)))
end

function snapshot!(state, sample)
    for (i, horizon) in enumerate(HORIZONS)
        horizon <= sample.lead <= horizon + TOLERANCE || continue
        old = state.early[i]
        if isnothing(old) || sample.timestamp > old.timestamp ||
           (sample.timestamp == old.timestamp && sample.published)
            state.early = Base.setindex(state.early, sample, i)
        end
    end
end

function merge_departures!(a, b)
    for (key, incoming) in b
        state = get!(Departure, a, key)
        state.cancelled |= incoming.cancelled
        state.conflict |= incoming.conflict
        foreach(x -> terminal!(state, x...), incoming.terminal)
        for sample in incoming.early
            isnothing(sample) || snapshot!(state, sample)
        end
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
        early = any(h -> h <= lead <= h + TOLERANCE, HORIZONS)
        near = !isnothing(delay) && 0 <= delay < 720 && -10 <= lead + delay <= 15
        early || near || is_cancelled || continue
        carrier = something(textcell(CARRIER, body), "")
        category = something(textcell(CATEGORY, body), "")
        bus_service(carrier, category) && continue
        destination = something(textcell(DESTINATION, body), "")
        run = match(RUN, body)
        token = isnothing(run) ? join((number, carrier, destination), "|") : String(run[1])
        platform = textcell(PLATFORM, body)
        isnothing(platform) && !is_cancelled && continue
        state = get!(Departure, runs, (String(station), day, token, clock))
        state.cancelled |= is_cancelled
        is_cancelled && continue
        known = platform_known(platform)
        if near && known
            terminal!(state, now, platform)
        end
        if early
            snapshot!(state, Snapshot(now, lead, number, destination, carrier, category,
                                      something(delay, -1), known))
        end
    end
end

function training_file(path, stations, cache_dir)
    fingerprint = (abspath(path), filesize(path), mtime(path), string(VERSION), 1, sort!(collect(stations)))
    cache = joinpath(cache_dir, basename(path) * ".jls")
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
        if state.cancelled || state.conflict || length(state.terminal) < 2 ||
           state.terminal[1][2] != state.terminal[2][2]
            continue
        end
        samples = [(h, x) for (h, x) in zip(HORIZONS, state.early) if !isnothing(x) && !x.published]
        isempty(samples) && continue
        label_at, platform = state.terminal[1]
        scheduled = Int64(scheduled_utc(day, clock, Dict{Tuple{Date,Int},Float64}()))
        for (horizon, sample) in samples
            sample.timestamp < label_at || continue
            row = (station=station, departureId=join((day, token, clock), "|"), serviceDate=string(day),
                   timestamp=sample.timestamp, scheduledTime=scheduled, labelTimestamp=label_at,
                   horizon=horizon, trainNumber=sample.number, predictedDestination=sample.destination,
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
    cache_dir = joinpath(@__DIR__, ".cache", "training-v1")
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
    foreach(chunk -> merge_departures!(runs, chunk), chunks)
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
    metadata = (; schema_version=1, archives=basename.(files), selected_stations=sort!(collect(stations)),
                 cached_archives=hits[], elapsed_seconds=round(time() - started; digits=2),
                 departures=length(runs), rows=sum(length, values(rows); init=0),
                 station_rows=Dict(s => length(get(rows, s, [])) for s in stations),
                 horizons=HORIZONS, label_window_minutes=[-10, 15], label_observations=2)
    open(joinpath(output, "dataset.json"), "w") do io
        JSON3.pretty(io, JSON3.write(metadata))
    end
    println(JSON3.write((; metadata.rows, labelled_stations=length(rows))))
end

abspath(PROGRAM_FILE) == (@__FILE__) && wrangle()
