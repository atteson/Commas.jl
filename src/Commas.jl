module Commas

using JSON
using Dates
using Mmap
using Formatting
using DataFrames

export DataRow, CharN

gccount( gc) = gc.malloc + gc.realloc + gc.poolalloc + gc.bigalloc
gctic() = gccount( Base.gc_num() )
gctoc( tic ) = gccount( Base.gc_num() ) - tic

abstract type AbstractDataRow end

mutable struct DataRow{NT <: NamedTuple} <: AbstractDataRow
    nt::NT
    row::Int
end

eltypes( nt::NamedTuple{U,T} ) where {U,T} = eltype.([nt...])

seen = Set()

makegetters( nt::NT ) where {NT <: NamedTuple} = makegetters( getmetadata( nt ) )

function makegetters( metadata::Vector )
    if !( metadata in seen )
        push!( seen, metadata )
        names = metadata[1]
        getternames = Symbol.("get" .* names)
        t = getnamedtupletype( metadata )
        for i = 1:length(names)
            eval( quote
                  $(getternames[i])( row::DataRow{$t} ) = row.nt.$(Symbol(names[i]))[row.row]
                  end )
        end
    end
end

# required transformations to move from 0.6 to 1.0
transformtypes = Dict(
    "Date" => "Commas.Dates.Date",
    "DateTime" => "Commas.Dates.DateTime",
    "Base.Dates.Time" => "Commas.Dates.Time",
)

const metadataname = ".metadata.json"

getmetadata( nt::NamedTuple{T,U} ) where {T,U} =
    [string.(T),string.(eltypes(nt))]

function getnamedtupletype( metadata )
    names = "(:" * join( metadata[1], ",:" ) * ")"
    transformedtypes = [get( transformtypes, t, t ) for t in metadata[2]]
    types = "Tuple{Vector{" * join( transformedtypes, "},Vector{" ) * "}}"
    Base.eval(Main, Meta.parse( "NamedTuple{$names,$types}" ) )
end

writemetadata( dir::String, metadata ) =
    write( joinpath( dir, metadataname ), JSON.json( metadata ) )

writecolumn( dir::String, name::Symbol, data::Vector ) = write( joinpath( dir, string(name) ), data )

function addcolumn( dir::String, nt::NamedTuple{T,U}, name::Symbol, data::Vector ) where {T,U}
    writecolumn( dir, name, data )
    metadata = getmetadata( nt )
    indices = findall( metadata[1] .!= string(name) )
    writemetadata( dir, [(metadata[1][indices]..., name), [metadata[2][indices]; eltype(data)]] )
end

function writecomma( dir::String, data::DataFrame )
    mkpath( dir )
    types = Type[]
    for (name,array) in eachcol(data)
          missings = ismissing.(array)
          @assert( !any(missings) )
          push!( types, Missings.T(eltype(array)) )
          writecolumn( dir, name, Vector{types[end]}(array) )
    end
    writemetadata( dir, [string.(names(data)), string.(types)] )
end

readmetadata( dir::String ) =
    JSON.parsefile( joinpath( dir, metadataname ) )

init( dir::String ) = makegetters( readmetadata( dir ) )

function readcomma( dir::String )
    (cols,types) = readmetadata( dir )
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
    Float64 => "%0.2f",
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
    toprows::Int = div(displaysize(io)[1], 2) - 3,
    bottomrows::Int = toprows,
    termwidth::Int = displaysize(io)[2],
) where {T,U <: NTuple{N,Vector} where {N}}
    toprows = min(toprows, length(df[1]))
    bottomrows = min(bottomrows, length(df[1]) - toprows)
    
    columns = Vector{String}[]
    totallength = 0
    for k in keys(df)
        col = getfield(df,k)

        top = format.(col[1:toprows])
        bottom = format.(col[end-bottomrows+1:end])
        colstrings = [string(k); top]
        if bottomrows > 0
            colstrings = [colstrings; "..."; bottom]
        end
            
        
        collength = maximum(length.(colstrings)) + 1
        totallength += collength
        totallength > termwidth && break
        
        colstrings = align( col[1] ).( colstrings, collength )
        push!( columns, colstrings )
    end
    print( io, join( .*( columns... ), '\n' ) )
end

const CharN{N} = NTuple{N,UInt8}

Base.convert( ::Type{CharN{N}}, x::AbstractString ) where {N} = convert( CharN{N}, (rpad(x,N)...,) )

Base.convert( ::Type{String}, x::CharN{N} ) where {N} = strip(String([x...]))

# This data structure doesn't support strings so this is the alternative for now
Base.show( io::IO, tuple::NTuple{N,UInt8} ) where {N} = print( io, String(UInt8[tuple...]) )

end # module
