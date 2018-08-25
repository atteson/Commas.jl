using Commas
using Dates

dir = "/home/atteson/data/options/2004/07/02/SPX_20040717"

df = Commas.readcomma( dir )

row = Commas.DataRow( df, 17 )
@assert( row[:bid] .== df.bid[17] )

function update( row, moments )
    for i in 1:length(moments)
        moments[i] += row[:price]^(i-1)
    end
end

dir = "/home/atteson/data/SPX"

df = Commas.readcomma( dir )

moments = zeros(Float64,3)
caller = DataCaller( df, Function[row -> update( row, moments )] )
@time runcallbacks( caller )

@assert( abs( moments[1] - length(df.price) ) == 0.0 )
avg = moments[2]/moments[1]
sqrt(moments[3]/moments[1] - avg^2)

filter = FilterCaller( caller, row -> Commas.getdate( row ) >= Date( 2004, 7, 2 ) )
moments = zeros( Float64, 3 )
@time runcallbacks( filter )

@assert( abs( moments[1] - sum(df.date .>= Date(2004,7,2)) ) == 0.0 )
avg = moments[2]/moments[1]
sqrt(moments[3]/moments[1] - avg^2)
