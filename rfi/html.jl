using Dates, JSON3, TimeZones

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

platform_known(value) = !(uppercase(value) in ("", "-", "--", "—", "?", "N.D.", "ND", "NON DISPONIBILE"))
cancelled(value) = occursin(r"(?i)cancellat|soppress|cancelled|canceled", value)
bus_service(carrier, category) = occursin(r"(?i)\bbus|autobus|pullman|autoserv|autocors", carrier * " " * category)
