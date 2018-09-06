using Dates
using BusinessDays

export BusinessDay

struct BusinessDay <: DatePeriod
    calendar::Symbol
    value::Int
end

units( bd::BusinessDay ) = string(bd.calendar) * " bday" * (abs(bd.value) == 1 ? "" : "s")

Base.string( bd::BusinessDay ) = string(bd.value) * " " * units(bd)

Base.show( io::IO, bd::BusinessDay ) = print( io, string(bd) )

struct BusinessDayRange <: OrdinalRange{Date, BusinessDay}
    start::Date
    step::BusinessDay
    stop::Date
end

Base.:(:)( start::Date, step::BusinessDay, stop::Date ) = BusinessDayRange( start, step, stop )

Base.string( r::BusinessDayRange ) = string(r.start) * ":(" * string(r.step) * "):" * string(r.stop)

Base.print( io::IO, r::BusinessDayRange ) = print( io, string(r) )

Base.show( io::IO, r::BusinessDayRange ) = print( io, string(r) )

Base.iterate( r::BusinessDayRange, d::Date = advancebdays( r.step.calendar, r.start, 0 ) ) =
    d > r.stop ? nothing : (d, advancebdays( r.step.calendar, d, r.step.value))

Base.length( r::BusinessDayRange ) = bdays( r.step.calendar, r.start, r.stop ).value

function Base.collect( range::BusinessDayRange )
    n = length(range)
    v = fill( first(range), n )
    i = 0
    d = iterate( range )
    while d != nothing
        i += 1
        v[i] = d[1]
        d = iterate( range, d[2] )
    end
    @assert( i == n )
    return v
end
