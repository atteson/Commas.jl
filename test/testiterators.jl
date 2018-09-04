using Commas
using Dates

delta = Commas.BusinessDay( :USNYSE, 1 )
[Commas.BusinessDay( :USNYSE, i ) for i in 1:10]

startdate = Date( 2017, 1, 1 )
enddate = Date( 2018, 1, 1 )
range = Commas.BusinessDayRange( startdate, delta, enddate )
ranges = [Commas.BusinessDayRange( startdate, Commas.BusinessDay( :USNYSE, i ), enddate ) for i in 1:10]

