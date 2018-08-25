using Commas

dir = "/home/atteson/data/SPX"

df = Commas.readcomma( dir )

n = length(df.price)
t = eltype(df.price)
nt = ( x = randn(t, n), )

sumsquared1( sumsq::Vector{Float32}, x::Float32 ) = sumsq[1] += x^2

function f1( nt )
    sumsq = [0f0]
    for i = 1:length(nt.x)
        sumsquared1( sumsq, nt.x[i] )
    end
    return sumsq[1]
end

@time f1( nt )
@time f1( nt )
# 0.084s

mutable struct Row{NT <: NamedTuple}
    nt::NT
    row::Int
end

Base.getindex( row::Row, field::Symbol ) = getfield( row.nt, field )[row.row]

sumsquared2( sumsq::Vector{Float32}, row::Row ) = sumsq[1] += row[:x]^2

function f2( nt )
    sumsq = [0f0]
    row = Row( nt, 1 )
    for i = 1:length(nt.x)
        row.row = i
        sumsquared2( sumsq, row )
    end
    return sumsq[1]
end

@time f2( nt )
@time f2( nt )
# 0.084s

function f3( nt )
    sumsq = [0f0]
    callbacks = [sumsquared2]
    row = Row( nt, 1 )
    for i = 1:length(nt.x)
        row.row = i
        for callback in callbacks
            callback( sumsq, row )
        end
    end
    return sumsq[1]
end

@time f3( nt )
@time f3( nt )
# 0.086s

callbacks = [sumsquared2]

function f4( callbacks, nt )
    sumsq = [0f0]
    row = Row( nt, 1 )
    for i = 1:length(nt.x)
        row.row = i
        for callback in callbacks
            callback( sumsq, row )
        end
    end
    return sumsq[1]
end

@time f4( callbacks, nt )
@time f4( callbacks, nt )
# 0.089s

sumsquared5( sumsq::Vector{Float32}, row::Row ) = sumsq[1] += row[:price]^2

callbacks = [sumsquared5]

function f5( callbacks, nt )
    sumsq = [0f0]
    row = Row( nt, 1 )
    for i = 1:length(nt.price)
        row.row = i
        for callback in callbacks
            callback( sumsq, row )
        end
    end
    return sumsq[1]
end

@time f5( callbacks, df )
@time f5( callbacks, df )
# 2.96s

getprice( row::Row{NT} ) where NT <: NamedTuple = row.nt.price[row.row]

sumsquared6( sumsq::Vector{Float32}, row::Row ) = sumsq[1] += getprice( row )^2

callbacks = [sumsquared6]

@time f5( callbacks, df )
@time f5( callbacks, df )
# 0.086s

function f7( nt )
    sumsq = [0f0]
    row = Row( nt, 1 )
    for i = 1:length(nt.price)
        row.row = i
        sumsquared5( sumsq, row )
    end
    return sumsq[1]
end

@time f7( df )
@time f7( df )

nt2 = ( price=nt.x, )
@time f7( nt2 )
@time f7( nt2 )

sumsquared8( sumsq::Vector{Float32}, row::DataRow ) = sumsq[1] += Commas.getprice( row )^2

function f8( nt )
    sumsq = [0f0]
    row = DataRow( nt, 1 )
    for i = 1:length(nt.price)
        row.row = i
        sumsquared8( sumsq, row )
    end
    return sumsq[1]
end

@time f8( df )
@time f8( df )
# 0.084s

callbacks = [sumsquared8]

function f9( callbacks, nt )
    sumsq = [0f0]
    row = DataRow( nt, 1 )
    for i = 1:length(nt.price)
        row.row = i
        for callback in callbacks
            callback( sumsq, row )
        end
    end
    return sumsq[1]
end

@time f9( callbacks, df )
@time f9( callbacks, df )
# 0.10s
