using Dates
using BusinessDays

struct BusinessDay <: DatePeriod
    calendar::Symbol
    value::Int
end

(Base.+)( d::Date, bd::BusinessDay ) = 