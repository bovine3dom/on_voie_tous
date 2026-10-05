using CodecZstd, Dates, JSON3, Serialization, TimeZones

const HORIZONS = (15, 30, 60, 120)
const TOLERANCE = 10
const ROME = tz"Europe/Rome"
const ROOT = r"""^\{"ts":"([^"]+)","station":"([^"]+)","data":"""
# Match the escaped HTML. Decode only small cells, not the embedded images.
const ROW = r"""<tr\b[^>]*name=\\["]treno\\["][^>]*>((?:[^<]++|<(?!/tr>))*)</tr>"""
const NAME = r"""<h1\b[^>]*id=\\["]nomeStazioneId\\["][^>]*>(.*?)</h1>"""
const RUN = r"""id=\\["]btn_(\d+)\\["]"""
const CLOCK = r"""<td\b[^>]*id=\\["]ROrario\\["][^>]*>\s*(\d{2}):(\d{2})\s*</td>"""
const PLATFORM = r"""<td\b[^>]*id=\\["]RBinario\\["][^>]*>(.*?)</td>"""
const NUMBER = r"""<td\b[^>]*id=\\["]RTreno\\["][^>]*>(.*?)</td>"""
const DESTINATION = r"""<td\b[^>]*id=\\["]RStazione\\["][^>]*>(.*?)</td>"""
const DELAY = r"""<td\b[^>]*id=\\["]RRitardo\\["][^>]*>(.*?)</td>"""
const CARRIER = r"""<td\b[^>]*id=\\["]RVettore\\["][^>]*>.*?<img\b[^>]*alt=\\["](.*?)\\["]"""
const CATEGORY = r"""<td\b[^>]*id=\\["]RCategoria\\["][^>]*>.*?<img\b[^>]*alt=\\["](.*?)\\["]"""

function textcell(pattern, text)
    m = match(pattern, text)
    isnothing(m) && return nothing
    value = JSON3.read("\"" * m[1] * "\"", String)
    value = replace(value, r"<[^>]*>" => " ", r"&(?:nbsp|#160|#x[Aa]0);" => " ",
                    "&amp;" => "&", "&#39;" => "'", "&quot;" => "\"")
    strip(replace(value, r"\s+" => " "))
end

mutable struct RunState
    best::NTuple{4,Float64}
    known::UInt8
end
RunState() = RunState(ntuple(_ -> Inf, 4), 0x00)

mutable struct Station
    name::String
    first::DateTime
    last::DateTime
    boards::Int
    invalid::Int
    rows::Int
    cancelled::Int
    bus::Int
    unparsed::Int
end
Station(name, at) = Station(name, at, at, 0, 0, 0, 0, 0, 0)

struct Scan
    stations::Dict{String,Station}
    runs::Dict{Tuple{String,Date,String,Int},RunState}
end
Scan() = Scan(Dict{String,Station}(), Dict{Tuple{String,Date,String,Int},RunState}())

function observe!(state, lead, known)
    for (i, horizon) in enumerate(HORIZONS)
        horizon <= lead <= horizon + TOLERANCE || continue
        bit = UInt8(1 << (i - 1))
        if lead < state.best[i]
            state.best = Base.setindex(state.best, lead, i)
            state.known = known ? state.known | bit : state.known & ~bit
        elseif lead == state.best[i] && known
            state.known |= bit
        end
    end
end

function merge_scan!(a, b)
    for (id, s) in b.stations
        if !haskey(a.stations, id)
            a.stations[id] = s
            continue
        end
        old = a.stations[id]
        if s.last >= old.last && !isempty(s.name)
            old.name = s.name
        end
        old.first = min(old.first, s.first)
        old.last = max(old.last, s.last)
        for field in (:boards, :invalid, :rows, :cancelled, :bus, :unparsed)
            setfield!(old, field, getfield(old, field) + getfield(s, field))
        end
    end
    for (key, state) in b.runs
        if !haskey(a.runs, key)
            a.runs[key] = state
        else
            for i in eachindex(HORIZONS)
                isfinite(state.best[i]) || continue
                observe!(a.runs[key], state.best[i], state.known & (1 << (i - 1)) != 0)
            end
        end
    end
    a
end

function scheduled_utc(day, clock, times)
    get!(times, (day, clock)) do
        local_time = DateTime(day) + Minute(clock)
        try
            datetime2unix(DateTime(ZonedDateTime(local_time, ROME), UTC))
        catch error
            error isa TimeZones.AmbiguousTimeError || error isa TimeZones.NonExistentTimeError || rethrow()
            NaN
        end
    end
end

function scan_line!(scan, line, times)
    header = match(ROOT, line)
    isnothing(header) && error("Unsupported archive envelope")
    ts, id = header.captures
    endswith(ts, "+00:00") || endswith(ts, "Z") || error("Collection time is not UTC")
    at = DateTime(ts[1:19], dateformat"yyyy-mm-ddTHH:MM:SS")
    station = get!(scan.stations, id) do
        Station(something(textcell(NAME, line), ""), at)
    end
    station.boards += 1
    station.first = min(station.first, at)
    if at >= station.last
        station.last = at
        station.name = something(textcell(NAME, line), station.name)
    end
    if isempty(station.name) || occursin("01/01/0001", line) || !occursin("bodyTabId", line)
        station.invalid += 1
        return
    end
    now = datetime2unix(at)
    local_now = astimezone(ZonedDateTime(at, tz"UTC"), ROME)
    today = Date(local_now)
    local_clock = hour(local_now) * 60 + minute(local_now)
    for row in eachmatch(ROW, line)
        number = textcell(NUMBER, row[1])
        if isnothing(number)
            station.unparsed += 1
            continue
        end
        isempty(number) && continue
        station.rows += 1
        clock_match = match(CLOCK, row[1])
        if isnothing(clock_match)
            station.unparsed += 1
            continue
        end
        clock = parse(Int, clock_match[1]) * 60 + parse(Int, clock_match[2])
        if !(0 <= clock < 1440) || parse(Int, clock_match[2]) >= 60
            station.unparsed += 1
            continue
        end
        # A run ID can carry the origin date of an overnight train.
        # Infer this station's departure date from the displayed local clock.
        day = today + Day(clock - local_clock < -720 ? 1 : clock - local_clock > 720 ? -1 : 0)
        lead = (scheduled_utc(day, clock, times) - now) / 60
        if !isfinite(lead)
            station.unparsed += 1
            continue
        end
        any(h -> h <= lead <= h + TOLERANCE, HORIZONS) || continue
        delay = something(textcell(DELAY, row[1]), "")
        carrier = something(textcell(CARRIER, row[1]), "")
        category = something(textcell(CATEGORY, row[1]), "")
        if occursin(r"(?i)cancellat|soppress|cancelled|canceled", delay)
            station.cancelled += 1
            continue
        end
        if occursin(r"(?i)\bbus|autobus|pullman|autoserv|autocors", carrier * " " * category)
            station.bus += 1
            continue
        end
        platform = textcell(PLATFORM, row[1])
        if isnothing(platform)
            station.unparsed += 1
            continue
        end
        run = match(RUN, row[1])
        token = isnothing(run) ? join((number, carrier, something(textcell(DESTINATION, row[1]), "")), "|") : String(run[1])
        key = (String(id), day, token, clock)
        state = get!(RunState, scan.runs, key)
        known = !(uppercase(platform) in ("", "-", "--", "—", "?", "N.D.", "ND", "NON DISPONIBILE"))
        observe!(state, lead, known)
    end
end

function scan_file(path, cache_dir)
    fingerprint = (abspath(path), filesize(path), mtime(path), string(VERSION), 3, HORIZONS, TOLERANCE)
    cache = joinpath(cache_dir, basename(path) * ".jls")
    if isfile(cache)
        stored = deserialize(cache)
        stored.fingerprint == fingerprint && return stored.scan, true
    end
    scan = Scan()
    times = Dict{Tuple{Date,Int},Float64}()
    open(path) do raw
        stream = ZstdDecompressorStream(raw; windowLogMax=Int32(28), bufsize=1 << 20)
        try
            for line in eachline(stream)
                scan_line!(scan, line, times)
            end
        finally
            close(stream)
        end
    end
    tmp = cache * ".tmp." * string(getpid())
    serialize(tmp, (; fingerprint, scan))
    mv(tmp, cache; force=true)
    scan, false
end

function counts(scan)
    result = Dict{Tuple{String,String},Tuple{Vector{Int},Vector{Int}}}()
    months = Dict{Date,String}()
    for ((id, day, _, _), state) in scan.runs
        month = get!(() -> Dates.format(day, dateformat"yyyy-mm"), months, day)
        for period in ("all", month)
            eligible, missing = get!(() -> (zeros(Int, 4), zeros(Int, 4)), result, (id, period))
            for i in eachindex(HORIZONS)
                isfinite(state.best[i]) || continue
                eligible[i] += 1
                missing[i] += state.known & (1 << (i - 1)) == 0
            end
        end
    end
    result
end

function classify(periods, index, minimum, threshold)
    supported = [(period, n[index], m[index]) for (period, (n, m)) in periods if n[index] >= minimum]
    isempty(supported) && return "insufficient_data", ""
    period, eligible, missing = argmax(x -> (x[3] / x[2], x[1]), supported)
    missing / eligible >= threshold && return "needs_predictions", period
    # Do not skip a station if a small period shows a substantial gap.
    for (p, (n, m)) in periods
        if p != "all" && n[index] < minimum && m[index] >= 10 && m[index] / n[index] >= threshold
            return "insufficient_data", ""
        end
    end
    "skip", period
end

csvcell(x) = x isa AbstractString ? "\"" * replace(x, "\"" => "\"\"") * "\"" : string(x)
function writecsv(path, header, rows)
    open(path, "w") do io
        println(io, join(header, ','))
        for row in rows
            println(io, join(csvcell.(row), ','))
        end
    end
end
function metric_cells(n, m)
    reduce(vcat, (Any[n[i], m[i], n[i] == 0 ? "" : round(100m[i] / n[i]; digits=3)] for i in eachindex(HORIZONS)))
end

function reports(scan, files, output, lead, minimum, threshold, elapsed, hits; minimums=Dict{String,Int}())
    index = something(findfirst(==(lead), HORIZONS))
    totals = counts(scan)
    mkpath(output)
    header = ["station_id", "station_name", "classification", "evidence_period", "minimum_departures",
              "boards", "invalid_boards", "rows", "excluded_cancelled", "excluded_bus", "unparsed_rows"]
    metrics = [string(field, "_", h) for h in HORIZONS for field in ("eligible", "missing", "missing_pct")]
    rows = []
    lists = Dict(label => String[] for label in ("needs_predictions", "skip", "insufficient_data"))
    for id in sort!(collect(keys(scan.stations)); by=x -> parse(Int, x))
        station = scan.stations[id]
        periods = [(p, value) for ((s, p), value) in totals if s == id]
        station_minimum = get(minimums, id, minimum)
        label, basis = classify(periods, index, station_minimum, threshold)
        if label == "skip" && station.unparsed > station.rows / 100
            label, basis = "insufficient_data", ""
        end
        push!(lists[label], id)
        n, m = get(totals, (id, "all"), (zeros(Int, 4), zeros(Int, 4)))
        push!(rows, vcat([id, station.name, label, basis, station_minimum, station.boards, station.invalid,
                         station.rows, station.cancelled, station.bus, station.unparsed], metric_cells(n, m)))
    end
    writecsv(joinpath(output, "stations.csv"), vcat(header, metrics), rows)
    monthly = [vcat([id, scan.stations[id].name, period], metric_cells(n, m))
               for ((id, period), (n, m)) in sort!(collect(totals); by=first) if period != "all"]
    writecsv(joinpath(output, "monthly.csv"), vcat(["station_id", "station_name", "month"], metrics), monthly)
    for (label, ids) in lists
        open(joinpath(output, label * ".txt"), "w") do io
            foreach(id -> println(io, id), ids)
        end
    end
    metadata = (; lead_minutes=lead, tolerance_minutes=TOLERANCE, minimum_departures=minimum,
                 minimum_overrides=minimums, missing_rate_threshold=threshold, archives=length(files), compressed_bytes=sum(filesize, files),
                 first_observation=minimum_datetime(scan), last_observation=maximum_datetime(scan),
                 threads=Threads.nthreads(), elapsed_seconds=round(elapsed; digits=2), cached_archives=hits,
                 classifications=Dict(k => length(v) for (k, v) in lists), files=basename.(files))
    open(joinpath(output, "audit.json"), "w") do io
        JSON3.pretty(io, JSON3.write(metadata))
        println(io)
    end
    println(JSON3.write(metadata.classifications))
end

minimum_datetime(scan) = string(minimum(s.first for s in values(scan.stations)))
maximum_datetime(scan) = string(maximum(s.last for s in values(scan.stations)))

function main(args=ARGS)
    isempty(args) && error("Usage: audit.jl ARCHIVE_DIRECTORY [--lead=30] [--min=100] [--minimums=PATH] [--rate=0.1] [--output=PATH] [--limit=N]")
    input = abspath(first(args))
    options = Dict{String,String}()
    for arg in args[2:end]
        startswith(arg, "--") && occursin('=', arg) || error("Expected --option=value")
        key, value = split(arg[3:end], '='; limit=2)
        key in ("lead", "min", "minimums", "rate", "output", "limit") || error("Unknown option: $key")
        options[key] = value
    end
    lead = parse(Int, get(options, "lead", "30"))
    minimum = parse(Int, get(options, "min", "100"))
    threshold = parse(Float64, get(options, "rate", "0.1"))
    lead in HORIZONS && minimum > 0 && 0 < threshold <= 1 || error("Invalid split threshold")
    minimums_path = get(options, "minimums", joinpath(@__DIR__, "minimum_departures.json"))
    haskey(options, "minimums") && !isfile(minimums_path) && error("Override file does not exist: $minimums_path")
    minimums = isfile(minimums_path) ? JSON3.read(read(minimums_path, String), Dict{String,Int}) : Dict{String,Int}()
    all(>(0), values(minimums)) || error("Minimum overrides must be positive")
    output = abspath(get(options, "output", joinpath(@__DIR__, "results")))
    files = sort!(filter(p -> endswith(p, ".zst"), readdir(input; join=true)))
    limit = parse(Int, get(options, "limit", string(length(files))))
    limit > 0 || error("Limit must be positive")
    files = files[1:min(limit, length(files))]
    isempty(files) && error("No .zst archives found")
    cache_dir = joinpath(@__DIR__, ".cache")
    mkpath(cache_dir)
    started = time()
    workers = min(Threads.nthreads(), length(files))
    chunks = [Scan() for _ in 1:workers]
    completed = Threads.Atomic{Int}(0)
    hits = Threads.Atomic{Int}(0)
    log_lock = ReentrantLock()
    Threads.@threads for worker in 1:workers
        for i in worker:workers:length(files)
            scan, cached = scan_file(files[i], cache_dir)
            merge_scan!(chunks[worker], scan)
            cached && Threads.atomic_add!(hits, 1)
            done = Threads.atomic_add!(completed, 1) + 1
            lock(log_lock) do
                println(stderr, "[$done/$(length(files))] $(basename(files[i])) $(cached ? "cached" : "scanned")")
                flush(stderr)
            end
        end
    end
    scan = Scan()
    foreach(chunk -> merge_scan!(scan, chunk), chunks)
    reports(scan, files, output, lead, minimum, threshold, time() - started, hits[]; minimums)
end

abspath(PROGRAM_FILE) == (@__FILE__) && main()
