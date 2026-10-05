using Test, Arrow, CodecZstd, Dates, JSON3
include("prepare.jl")

function board(; at="2026-04-01T10:00:00Z", source=at, departure="2026-04-01T12:30:00+02:00",
               platform="", id="123", number="00123", stop="origin", status="scheduled",
               observation="", traffic="L", delay=0, platform_cell=true, station="51003")
    train = Dict("id"=>id, "technical_number_planif"=>number, "technical_number_planif_out"=>"",
                 "class_stop"=>stop, "status"=>status, "observation"=>observation, "traffic_type"=>traffic,
                 "departure_time"=>departure, "company"=>"Renfe", "delay_out"=>delay,
                 "commercial_id"=>[Dict("product"=>"AVE")], "destinations"=>[Dict("code"=>"71801")])
    platform_cell && (train["platform"] = platform)
    payload = (station_settings=(code=station, name="Sevilla Santa Justa", data_time=source), trains=[train])
    JSON3.write((ts=at, station="es-adif", data=(type=1, target="ReceiveMessage", arguments=[JSON3.write(payload)])))
end

function parsed(lines...)
    scan = ADIF.Scan()
    times = Dict{String,Union{Nothing,Int64}}()
    dates = Dict{Int64,Date}()
    foreach(line -> ADIF.line!(scan, line, times, dates), lines)
    scan
end
late(value="2EST"; kwargs...) = board(at="2026-04-01T10:25:00Z", platform=value; kwargs...)
latest(value="2EST"; kwargs...) = board(at="2026-04-01T10:30:00Z", platform=value; kwargs...)

@testset "ADIF departure identity, official inputs, and later labels" begin
    scan = parsed(board(platform="20B"), late(id="999"), latest(id="999"))
    rows = ADIF.rows(scan, Set(["51003"]))["51003"]
    @test length(rows) == 1
    @test rows[1].trainNumber == "00123"
    @test rows[1].predictedPlatform == "20B"
    @test rows[1].actualPlatform == "2EST"
    @test rows[1].predictedDestination == "71801"
    @test rows[1].trainType == "AVE"
    @test rows[1].scheduledMinute == 750
    @test rows[1].leadMinutes == 30
    @test rows[1].timestamp < rows[1].labelTimestamp
    @test ADIF.rows(parsed(board(), late(), latest()), Set(["51003"]))["51003"][1].predictedPlatform == "MISSING"
    @test isempty(ADIF.rows(scan, Set(["99999"])))
    @test ADIF.revisions(scan, 30)[("51003", "all")] == [1, 1, 1]
    @test ADIF.rows(parsed(latest(), board(platform="20B"), late()), Set(["51003"])) == ADIF.rows(scan, Set(["51003"]))
    merged = ADIF.merge!(parsed(board(platform="20B"), late()), parsed(latest()))
    @test ADIF.rows(merged, Set(["51003"])) == ADIF.rows(scan, Set(["51003"]))
    function quoted(line)
        envelope = JSON3.read(line)
        JSON3.write((ts=envelope.ts, station=envelope.station,
                     data=(type=1, target="ReceiveMessage", arguments=[JSON3.write(envelope.data.arguments[1])])))
    end
    encoded = parsed(quoted(board(platform="20B")), quoted(late()), quoted(latest()))
    @test ADIF.rows(encoded, Set(["51003"])) == ADIF.rows(scan, Set(["51003"]))
end

@testset "Distinct source updates, cancellation, and invalid observations" begin
    @test isempty(ADIF.rows(parsed(board(), late(), latest(source="2026-04-01T10:25:00Z")), Set(["51003"])))
    @test isempty(ADIF.rows(parsed(board(), late("1"), latest("2")), Set(["51003"])))
    @test isempty(ADIF.rows(parsed(board(), late(), latest(), latest("3")), Set(["51003"])))
    @test isempty(ADIF.rows(parsed(board(), late(), latest(), board(observation="Tren cancelado")), Set(["51003"])))
    @test isempty(parsed(board(stop="destination")).runs)
    @test isempty(ADIF.rows(parsed(board(), late(), latest(), latest("BUS")), Set(["51003"])))
    @test only(values(parsed(board(traffic="B")).runs)).bus
    @test only(values(parsed(board(number="BUS00123")).runs)).bus
    @test only(values(parsed(board(platform="BUS")).runs)).bus
    @test isempty(parsed(board(platform_cell=false)).runs)
    @test isempty(parsed(board(departure="2026-04-01T12:30:00")).runs)
    @test isempty(parsed(board(source="2026-04-01T09:49:59Z")).runs)
    @test isempty(parsed(board(source="2026-04-01T10:01:00Z")).runs)
    control = JSON3.write((ts="2026-04-01T10:00:00Z", station="ECM-51003", data=(type=3, result=nothing)))
    @test parsed(control).statistics["control_messages"] == 1
    empty_payload = JSON3.write((ts="2026-04-01T10:00:00Z", station="es-adif",
                                data=(type=1, target="ReceiveMessage", arguments=[JSON3.write("")])))
    @test parsed(empty_payload).statistics["empty_payloads"] == 1
    @test ADIF.instant("2026-04-01T12:30:30.123+02:00") == ADIF.instant("2026-04-01T10:30:30Z")
    @test ADIF.instant("2026-10-25T02:30:00+01:00") - ADIF.instant("2026-10-25T02:30:00+02:00") == 3600
end

@testset "Delay, sampling weights, and archive cache" begin
    scan = parsed(board(delay=15), board(at="2026-04-01T09:30:00Z", platform="20B"),
                  board(at="2026-04-01T10:35:00Z", delay=15, platform="2EST"),
                  board(at="2026-04-01T10:45:00Z", delay=15, platform="2EST"))
    rows = ADIF.rows(scan, Set(["51003"]))["51003"]
    @test length(rows) == 2
    @test sum(row.weight for row in rows) == 1
    @test only(row.delayMinutes for row in rows if row.horizon == 30) == 15
    mktempdir() do directory
        path = joinpath(directory, "sample.zst")
        cache = joinpath(directory, "cache")
        mkpath(cache)
        open(path, "w") do raw
            stream = ZstdCompressorStream(raw; windowLog=Int32(28))
            write(stream, join((board(platform="20B"), late(), latest()), '\n'), '\n')
            close(stream)
        end
        first, cached = ADIF.scan_file(path, cache)
        @test !cached
        second, cached = ADIF.scan_file(path, cache)
        @test cached
        @test ADIF.rows(first, Set(["51003"])) == ADIF.rows(second, Set(["51003"]))
        selected = ADIF.reports(first, [path], joinpath(directory, "results"), 30, 1, 0.1, 0.01, 1, 0.0, 0)
        @test selected == Set(["51003"])
        for excluded in (parsed(board(), late(), latest("BUS")), parsed(board(), late(), latest(status="cancelled")))
            ADIF.reports(excluded, [path], joinpath(directory, "excluded"), 30, 1, 0.1, 0.01, 1, 0.0, 0)
            @test isempty(excluded.availability.runs)
        end
        @test ADIF.reports(first, [path], joinpath(directory, "overrides"), 30, 100, 0.1, 0.01, 1,
                           0.0, 0; minimums=Dict("51003"=>1)) == selected
        control = parsed(JSON3.write((ts="2026-04-01T10:00:00Z", station="es-adif",
                                    data=(type=1, target="ReceiveMessage", arguments=[nothing]))))
        @test control.statistics["empty_payloads"] == 1
        @test isempty(ADIF.reports(control, [path], joinpath(directory, "empty"), 30, 100, 0.1, 0.01, 5, 0.0, 0))
        arrow = joinpath(directory, "part0.arrow")
        Arrow.write(arrow, ADIF.rows(first, selected)["51003"]; file=true)
        @test collect(Arrow.Table(arrow).predictedPlatform) == ["20B"]
    end
end
