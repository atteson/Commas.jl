using JSON
using Dates
using Mmap
using Formatting
using JSON

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

function makegetters( nt )
    t = typeof(nt)
    if !( t in seen )
        push!( seen, t )
        names = keys(nt)
        getternames = Symbol.("get" .* string.(names))
        for i = 1:length(names)
            eval( quote
                  $(getternames[i])( row::DataRow{$(typeof(nt))} ) = row.nt.$(names[i])[row.row]
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
    [T,string.(eltypes(nt))]

writemetadata( dir::String, metadata ) =
    write( joinpath( dir, metadataname ), JSON.json( metadata ) )

function addcolumn( dir::String, nt::NamedTuple{T,U}, name::Symbol, data::Vector ) where {T,U}
    write( joinpath( dir, string(name) ), data )
    metadata = getmetadata( nt )
    indices = findall( metadata[1] .!= name )
    writemetadata( dir, [(metadata[1][indices]..., name), [metadata[2][indices]; eltype(data)]] )
end

readmetadata( dir::String ) =
    JSON.parsefile( joinpath( dir, metadataname ) )

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
Base.show( io::IO, tuple::NTuple{N,UInt8} ) where {N} = print( io, String([tuple...]) )

