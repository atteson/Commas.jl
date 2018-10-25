using Commas
using Dates
using Profile
using ProfileView
using BusinessDays
using OptionTrading

dir = "/home/atteson/data/SPX"
df = Commas.readcomma( dir );

sumsquared( sumsq::Vector{Float32}, row::DataRow{NT} ) where {NT} = sumsq[1] += Commas.getprice( row )^2

function f1( callbacks, nt )
    sumsq = [0f0]
    row = DataRow( nt, 1 )
    for i = 1:length(nt.price)
        row.row = i
        if Commas.getdate( row ) >= Date(2004,1,1)
            for callback in callbacks
                callback( sumsq, row )
            end
        end
    end
    return sumsq[1]
end

@time f1( [sumsquared], df )
@time f1( [sumsquared], df )
@time f1( [sumsquared], df )

sumsq = [0f0]
dc1 = DataCaller( df, [row -> sumsquared(sumsq, row)] );
fc1 = FilterCaller( dc1, row -> Commas.getdate(row) >= Date(2004,1,1) );
@time runcallbacks( fc1 )
sumsq[1] = 0f0
@time runcallbacks( fc1 )


startdate = Date(2004,7,2)
enddate = Date(2004,12,31)

format = DateFormat( "yyyy/mm/dd" )
yyyymmdd = DateFormat( "yyyymmdd" )

startdir = "/home/atteson/data/options/"
currdate = advancebdays( :USNYSE, startdate, 0 )
expiration = OptionTrading.nextexpiration( currdate, Dates.Month )
dirs = String[]
while currdate <= enddate
    push!( dirs, startdir * Dates.format( currdate, format ) * "/SPX_" * Dates.format( expiration, yyyymmdd ) )
    global currdate = advancebdays( :USNYSE, currdate, 1 )
    if expiration < currdate
        global expiration = OptionTrading.nextexpiration( currdate, Dates.Month )
    end
end

m = 3
function count( row, sums )
    for i = 1:m
        sums[i] += Commas.getbid( row )^(i-1)
    end
end
sums = zeros(m)
caller = HcatCaller( dirs, Function[row -> count(row, sums)] );
sums[1:m] = zeros(m)
@time runcallbacks( caller )
sums[1:m] = zeros(m)
@time runcallbacks( caller )

function f()
    sums = zeros(m)
    for dir in dirs
        df = Commas.readcomma(dir)
        for i = 1:m
            sums[i] += mapreduce( x->x^(i-1), +, df.bid )
        end
    end
    return sums
end

@time sums2 = f()
@time sums2 = f()


