using Test
include("wrangler.jl")

function board(; at="2026-04-01T10:00:00Z", clock="12:30", platform="", delay="",
               run="20260331100123", number="123", station="1728", category="REG",
               platform_cell=true)
    binario = platform_cell ? "<td id=\"RBinario\">$platform</td>" : ""
    html = """<h1 id="nomeStazioneId">TEST</h1><tbody id="bodyTabId">
    <tr name="treno"><td id="RVettore"><img alt="TRENITALIA"></td>
    <td id="RCategoria"><img alt="Categoria $category"></td>
    <td id="RTreno">$number</td><td id="RStazione">ROMA &amp; TEST</td>
    <td id="ROrario">$clock</td><td id="RRitardo">$delay</td>$binario
    <td><button id="btn_$run"></button></td></tr></tbody>"""
    JSON3.write((ts=at, station=station, data=html))
end

function parsed(lines...)
    runs = Departures()
    times = Dict{Tuple{Date,Int},Float64}()
    foreach(line -> training_line!(runs, line, Set(["1728"]), times), lines)
    runs
end
late(platform="2EST") = board(at="2026-04-01T10:25:00Z"; platform)
latest(platform="2EST") = board(at="2026-04-01T10:30:00Z"; platform)

@testset "Missing-platform features and stable later targets" begin
    runs = parsed(board(), late(), latest())
    rows = training_rows(runs)["1728"]
    @test length(rows) == 1
    @test rows[1].actualPlatform == "2EST"
    @test rows[1].predictedDestination == "ROMA & TEST"
    @test rows[1].scheduledMinute == 750
    @test rows[1].dayOfWeek == 3
    @test rows[1].month == 4
    @test rows[1].horizon == 30
    @test rows[1].timestamp < rows[1].labelTimestamp
    @test isempty(training_rows(parsed(board(platform="20B"), late(), latest())))
    @test isempty(training_rows(parsed(board(), latest())))
    @test isempty(training_rows(parsed(board(), late("1"), latest("2"))))
    @test isempty(training_rows(parsed(board(), latest(), latest())))
    @test isempty(training_rows(parsed(board(), late(), latest(), board(delay="Cancellato"))))
    @test isempty(parsed(board(category="AUTOBUS")))
    @test isempty(parsed(board(number="")))
    @test isempty(parsed(board(platform_cell=false)))
    @test isempty(parsed(board(station="8888")))
    @test isempty(parsed(board(clock="29:30")))
    @test training_rows(parsed(latest(), board(), late())) == training_rows(runs)
    merged = merge_departures!(parsed(board(), late()), parsed(latest()))
    @test training_rows(merged) == training_rows(runs)
    @test isempty(training_rows(parsed(board(), late(), latest(), latest("3"))))
end

@testset "Delay, midnight, DST, and point weights" begin
    delayed = parsed(board(delay="15"),
                     board(at="2026-04-01T10:35:00Z", delay="15", platform="20B"),
                     board(at="2026-04-01T10:45:00Z", delay="15", platform="20B"))
    @test training_rows(delayed)["1728"][1].actualPlatform == "20B"
    @test training_rows(delayed)["1728"][1].delayMinutes == 15
    night = parsed(board(at="2026-04-01T21:55:00Z", clock="00:30"),
                   board(at="2026-04-01T22:25:00Z", clock="00:30", platform="1"),
                   board(at="2026-04-01T22:30:00Z", clock="00:30", platform="1"))
    @test training_rows(night)["1728"][1].serviceDate == "2026-04-02"
    @test isempty(parsed(board(at="2026-10-25T00:00:00Z", clock="02:30")))
    points = parsed(board(at="2026-04-01T09:30:00Z"), board(), late(), latest())
    rows = training_rows(points)["1728"]
    @test length(rows) == 2
    @test sum(x.weight for x in rows) == 1
    @test length(unique(x.departureId for x in rows)) == 1
end

@testset "Archive cache and Arrow interoperability" begin
    mktempdir() do directory
        path = joinpath(directory, "sample.zst")
        cache = joinpath(directory, "cache")
        mkpath(cache)
        open(path, "w") do raw
            stream = ZstdCompressorStream(raw; windowLog=Int32(28))
            write(stream, join((board(), late(), latest()), '\n'), '\n')
            close(stream)
        end
        runs, cached = training_file(path, Set(["1728"]), cache)
        @test !cached
        second, cached = training_file(path, Set(["1728"]), cache)
        @test cached
        @test training_rows(runs) == training_rows(second)
        other, cached = training_file(path, Set(["8888"]), cache)
        @test !cached
        @test isempty(other)
        arrow = joinpath(directory, "part0.arrow")
        Arrow.write(arrow, training_rows(runs)["1728"]; file=true)
        table = Arrow.Table(arrow)
        @test collect(table.actualPlatform) == ["2EST"]
        @test collect(table.horizon) == [30]
    end
end
