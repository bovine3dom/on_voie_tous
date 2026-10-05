using Test
include("audit.jl")

function fixture(; at="2026-04-01T10:00:00+00:00", clock="12:30", platform="",
                 delay="", category="REG", run="20260401100123", station="1728",
                 platform_cell=true, number="123")
    button = isempty(run) ? "" : "<button id=\"btn_$run\"></button>"
    binario = platform_cell ? "<td id=\"RBinario\"><div>\n$platform\n</div></td>" : ""
    html = """<h1 id="nomeStazioneId">TEST &amp; STATION</h1><tbody id="bodyTabId">
    <tr name="treno"><td id="RVettore"><img alt="TRENITALIA"></td>
    <td id="RCategoria"><img alt="Categoria $category"></td>
    <td id="RTreno">$number</td><td id="RStazione">ROMA</td>
    <td id="ROrario">$clock</td><td id="RRitardo">$delay</td>
    $binario<td id="RDettagli">$button</td></tr></tbody>"""
    JSON3.write((ts=at, station=station, data=html))
end

function scanned(lines...)
    scan = Scan()
    times = Dict{Tuple{Date,Int},Float64}()
    foreach(line -> scan_line!(scan, line, times), lines)
    scan
end

@testset "Availability, exclusions, and identities" begin
    scan = scanned(fixture())
    n, m = counts(scan)[("1728", "all")]
    @test n == [0, 1, 0, 0]
    @test m == [0, 1, 0, 0]
    @test scan.stations["1728"].name == "TEST & STATION"
    for platform in ("2EST", "20B", "1", "A")
        @test counts(scanned(fixture(; platform)))[("1728", "all")][2][2] == 0
    end
    @test counts(scanned(fixture(platform="&nbsp;")))[("1728", "all")][2][2] == 1
    @test isempty(scanned(fixture(delay="Cancellato")).runs)
    @test isempty(scanned(fixture(category="AUTOBUS")).runs)
    @test isempty(scanned(fixture(platform_cell=false)).runs)
    @test scanned(fixture(platform_cell=false)).stations["1728"].unparsed == 1
    @test isempty(scanned(fixture(clock="29:30")).runs)
    @test scanned(fixture(number="")).stations["1728"].rows == 0
    @test scanned(fixture(number="")).stations["1728"].unparsed == 0
    @test_throws ErrorException scanned("not an archive envelope")
end

@testset "Use the closest earlier snapshot, never a later platform" begin
    early = fixture(at="2026-04-01T09:55:00+00:00")
    at_target = fixture(platform="8")
    too_late = fixture(at="2026-04-01T10:05:00+00:00", platform="8")
    @test counts(scanned(early, at_target))[("1728", "all")][2][2] == 0
    @test counts(scanned(early, too_late))[("1728", "all")][2][2] == 1
    @test counts(scanned(at_target, early))[("1728", "all")][2][2] == 0
    @test counts(scanned(early, early))[("1728", "all")][1][2] == 1
    combined = merge_scan!(scanned(early), scanned(at_target))
    @test counts(combined) == counts(scanned(early, at_target))
    @test combined.stations["1728"].boards == 2
    @test isempty(scanned(fixture(at="2026-04-01T09:49:00+00:00")).runs)
end

@testset "Local dates, midnight, and DST" begin
    midnight = fixture(at="2026-04-01T21:55:00+00:00", clock="00:30", run="20260402100123")
    fallback = fixture(at="2026-04-01T21:55:00+00:00", clock="00:30", run="")
    @test counts(scanned(midnight))[("1728", "all")][1][2] == 1
    @test counts(scanned(fallback))[("1728", "all")][1][2] == 1
    overnight = fixture(at="2026-04-01T21:55:00+00:00", clock="00:30", run="20260401100123")
    @test counts(scanned(overnight))[("1728", "all")][1][2] == 1
    winter = fixture(at="2026-01-01T11:00:00+00:00", run="20260101100123")
    @test counts(scanned(winter))[("1728", "all")][1][2] == 1
    ambiguous = fixture(at="2026-10-25T00:00:00+00:00", clock="02:30", run="20261025100123")
    @test isempty(scanned(ambiguous).runs)
    @test scanned(ambiguous).stations["1728"].unparsed == 1
end

@testset "Station split" begin
    data(n, m) = ([0, n, 0, 0], [0, m, 0, 0])
    @test classify([("all", data(100, 10))], 2, 100, 0.1)[1] == "needs_predictions"
    @test classify([("all", data(100, 9))], 2, 100, 0.1)[1] == "skip"
    @test classify([("all", data(99, 90))], 2, 100, 0.1)[1] == "insufficient_data"
    @test classify([("all", data(1000, 20)), ("2026-10", data(100, 20))], 2, 100, 0.1) ==
        ("needs_predictions", "2026-10")
    @test classify([("all", data(1000, 20)), ("2026-10", data(20, 10))], 2, 100, 0.1)[1] ==
        "insufficient_data"
    @test classify([], 2, 100, 0.1)[1] == "insufficient_data"
    @test classify([("all", data(5, 1))], 2, 5, 0.1)[1] == "needs_predictions"
    @test classify([("all", data(5, 0))], 2, 5, 0.1)[1] == "skip"
    @test classify([("all", data(4, 3))], 2, 5, 0.1)[1] == "insufficient_data"
end

@testset "256 MiB archives, cache, and CSV" begin
    mktempdir() do dir
        path = joinpath(dir, "sample.txt.zst")
        cache = joinpath(dir, "cache")
        mkpath(cache)
        open(path, "w") do raw
            stream = ZstdCompressorStream(raw; level=3, windowLog=Int32(28))
            try
                write(stream, fixture(), '\n')
            finally
                close(stream)
            end
        end
        scan, cached = scan_file(path, cache)
        @test !cached
        second, cached = scan_file(path, cache)
        @test cached
        @test counts(scan) == counts(second)
        @test counts(scan)[("1728", "all")][2][2] == 1
        csv = joinpath(dir, "test.csv")
        writecsv(csv, ["name", "value"], [["A, \"B\"", 1]])
        @test read(csv, String) == "name,value\n\"A, \"\"B\"\"\",1\n"
        targets = [fixture(run=string(20260401100000 + i), station=id)
                   for i in 1:5 for id in ("1728", "8888")]
        output = joinpath(dir, "reports")
        reports(scanned(targets...), [path], output, 30, 100, 0.1, 0.0, 0;
                minimums=Dict("1728" => 5))
        @test readlines(joinpath(output, "needs_predictions.txt")) == ["1728"]
        @test readlines(joinpath(output, "insufficient_data.txt")) == ["8888"]
    end
end
