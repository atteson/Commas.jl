using Commas

dir = "/home/atteson/data/options/2004/07/02/SPX_20040717"

df = Commas.readcomma( dir )

row = Commas.DataRow( df, 17 )
@assert( row[:bid] .== df.bid[17] )

function update( row, moments )
    for i in 1:length(moments)
        moments[i] += getprice( row )^(i-1)
    end
end

dir = "/home/atteson/data/SPX"

df = Commas.readcomma( dir )

callbacks = DataCallbacks( df, [row -> update( row, zeros(3) )] )
@time runcallbacks( callbacks )

function f3( df::NamedTuple, moments )
    moments = zeros(Float32,3)
    for j = 1:length(df[1])
        for i = 1:3
            moments[i] += df.price[j]^(i-1)
        end
    end
    return moments
end

@time moments = f3( df, moments )

Base.getindex( row::DataRow, field::Symbol )::Float32 = getfield( row.df, field )[row.row]

function f4( df::NamedTuple, moments )
    moments = zeros(Float32,3)
    row = DataRow( df, 1 )
    for j = 1:length(df[1])
        row.row = j
        for i = 1:3
            moments[i] += row[:price]^(i-1)
        end
    end
    return moments
end

@time moments = f4( df, moments )
Profile.clear()
@profile moments = f4( df, zeros(Float32,3) )
Profile.print()
