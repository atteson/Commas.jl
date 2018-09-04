using Commas
using Dates

delta = Commas.BusinessDay( :USNYSE, 1 )
range = Commas.BusinessDayRange( Date( 2017, 1, 1 ), delta, Date( 2018, 1, 1 ) )

