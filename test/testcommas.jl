using Commas

dir = "/home/atteson/data/options/2004/07/02/SPX_20040717"

df = Commas.readcomma( dir )

row = Commas.DataRow( df, 17 )
@assert( row[:bid] .== df.bid[17] )

function update( row, moments )
    for i in 1:length(moments)
        moments[i] += Commas.getprice( row )^(i-1)
    end
end

dir = "/home/atteson/data/SPX"

df = Commas.readcomma( dir )

moments = zeros(Float32,3)
callbacks = DataCallbacks( df, [row -> update( row, moments )] )
@time runcallbacks( callbacks )

function f3( df::NamedTuple )
    moments = zeros(Float32,3)
    for j = 1:length(df[1])
        for i = 1:3
            moments[i] += df.price[j]^(i-1)
        end
    end
    return moments
end

@time moments = f3( df )

function f4( df )
    moments = zeros(Float32,3)
    callback = row ->update( row, moments )
    row = DataRow( df, 1 )
    for j = 1:length(df[1])
        row.row = j
        for i = 1:3
            callback( row )
        end
    end
    return moments
end

@time moments = f4( df )

function f5( df, callback )
    row = DataRow( df, 1 )
    for j = 1:length(df[1])
        row.row = j
        for i = 1:3
            callback( row )
        end
    end
    return moments
end

moments = zeros(Float32,3)
callback = row ->update( row, moments )
@time moments = f5( df, callback )

mutable struct T6{T,U,V <: Function}
    df::NamedTuple{T,U}
    callbacks::Vector{V}
end

function f6( t6 )
    df = t6.df
    row = DataRow( df, 1 )
    for j = 1:length(df[1])
        row.row = j
        for i = 1:3
            for callback in t6.callbacks
                callback( row )
            end
        end
    end
    return moments
end

moments = zeros(Float32,3)
callback = row ->update( row, moments )
t6 = T6( df, [callback] )
@time moments = f6( t6 )
