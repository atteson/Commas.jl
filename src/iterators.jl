using Dates
using BusinessDays

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

Base.string( r::BusinessDayRange ) = string(r.start) * ":(" * string(r.step) * "):" * string(r.stop)

Base.print( io::IO, r::BusinessDayRange ) = print( io, string(r) )

Base.show( io::IO, r::BusinessDayRange ) = print( io, string(r) )

Base.iterate( r::BusinessDayRange, d::Date = advancebdays( r.step.calendar, r.start, 0 ) ) =
    let d1 = advancebdays( r.step.calendar, d, r.step.value )
        d1 > r.stop ? nothing : d1
    end

