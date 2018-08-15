module Commas

using JSON
using Dates
using Mmap
using Formatting

export DataRow, DataCallbacks, runcallbacks

mutable struct DataRow{T,U}
    nt::NamedTuple{T,U}
    row::Int
end

eltypes( nt::NamedTuple{U,T} ) where {U,T} = T

function makegetters( nt )
    names = keys(nt)
    getternames = Symbol.("get" .* string.(names))
    types = eltypes( nt )
    for i = 1:length(names)
         eval( quote
             $(getternames[i])( row::DataRow{$names,$types} ) = row.nt.$(names[i])[row.row]
         end )
    end
end

# required transformations to move from 0.6 to 1.0
transformtypes = Dict(
    "Date" => "Commas.Dates.Date",
    "DateTime" => "Commas.Dates.DateTime",
    "Base.Dates.Time" => "Commas.Dates.Time",
)

function readcomma( dir::String )
    (cols,types) = JSON.parsefile( joinpath( dir, ".metadata.json" ) )
    coldata = Vector[]
    for i = 1:length(cols)
        transformedtype = get( transformtypes, types[i], types[i] )
        datatype = Base.eval(Main, Meta.parse(transformedtype))
        
        filename = joinpath( dir, cols[i] )
        filesize = stat( filename ).size
        n = Int(filesize/sizeof(datatype))
        
        col = Mmap.mmap( filename, Vector{datatype}, n )
        push!( coldata, col )
    end
    df = NamedTuple{(Symbol.(cols)...,)}( coldata )
    makegetters( df )
    return df
end

formats = Dict(
    Dates.Date => DateFormat( "mm/dd/yyyy" ),
    Dates.Time => DateFormat( "HH:MM:SS.sss" ),
    Int64 => "%d",
    Float32 => "%0.2f",
    UInt32 => "%d",
    Dates.DateTime => DateFormat( "mm/dd/yyyy HH:MM:SS.sss" ),
)

format( d::Dates.TimeType ) = Dates.format( d, formats[typeof(d)] )
format( x::Number ) = sprintf1( formats[typeof(x)], x )
format( c::NTuple{N,UInt8} where {N} ) = string(c)

align( d::Dates.TimeType ) = rpad
align( x::Number ) = lpad
align( c::NTuple{N,UInt8} where {N} ) = rpad

function Base.show(
    io::IO,
    df::NamedTuple{T,U};
    toprows::Int = div(displaysize(stdout)[1], 2) - 3,
    bottomrows::Int = toprows,
    termwidth::Int = displaysize(stdout)[2],
) where {T,U <: NTuple{N,Vector} where {N}}
    columns = Vector{String}[]
    totallength = 0
    for k in keys(df)
        col = getfield(df,k)

        top = format.(col[1:toprows])
        bottom = format.(col[end-bottomrows+1:end])
        colstrings = [string(k); top; "⋯"; bottom]
        
        collength = maximum(length.(colstrings))
        totallength += collength
        totallength > termwidth && break
        
        colstrings = align( col[1] ).( colstrings, collength )
        push!( columns, colstrings .* ' ' )
    end
    println( join( .*( columns... ), '\n' ) )
end

Base.show( io::IO, tuple::NTuple{N,UInt8} where {N} ) = print( io, String([tuple...]) )

mutable struct DataCallbacks{T,U,V <: Function}
    df::NamedTuple{T,U}
    callbacks::Vector{V}
end

function runcallbacks( data::DataCallbacks )
    df = data.df
    row = DataRow( df, 1 )
    while row.row <= length(df[1])
        for callback in data.callbacks
            callback( row )
        end
        row.row += 1
    end
end

end # module
