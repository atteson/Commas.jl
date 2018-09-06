using Commas
using Dates

step = BusinessDay( :USNYSE, 1 )
[BusinessDay( :USNYSE, i ) for i in 1:10]

startdate = Date( 2017, 1, 1 )
enddate = Date( 2018, 1, 1 )
range = startdate:step:enddate

@assert( length(collect(range)) == 251 )

ranges = [startdate:BusinessDay( :USNYSE, i ):enddate for i in 1:10]
