module Commas

using Dates
using Mmap
using Formatting
using DataFrames

export DataRow, CharN, append, groupby

gccount( gc) = gc.malloc + gc.realloc + gc.poolalloc + gc.bigalloc
gctic() = gccount( Base.gc_num() )
gctoc( tic ) = gccount( Base.gc_num() ) - tic

abstract type AbstractDataRow end

mutable struct DataRow{NT <: NamedTuple} <: AbstractDataRow
    nt::NT
    row::Int
end

eltypes( nt::NamedTuple{U,T} ) where {U,T} = eltype.([nt...])

# required transformations to move from 0.6 to 1.0
transformtypes = Dict(
    "Date" => "Commas.Dates.Date",
    "DateTime" => "Commas.Dates.DateTime",
    "Base.Dates.Time" => "Commas.Dates.Time",
)

function writecolumn( dir::String, name::String, data::Vector{T}; append::Bool = false ) where {T}
    io = open( joinpath( dir, name * "_$T" ), write=true, append=append )
    write( io, data )
    close( io )
end

Base.names( comma::NamedTuple{T,U} ) where {T,U} = string.(keys(comma))

function writecomma( dir::String, data::Union{DataFrame,NamedTuple{T,U}}; append::Bool = false ) where {T,U}
    mkpath( dir )
    for name in names(data)
        array = data[:,name]
        missings = ismissing.(array)
        @assert( !any(missings) )
        writecolumn( dir, name, array, append=append )
    end
end

function readcomma( dir::String )
    names = readdir( dir )
    matches = match.( r"^(.*)_([A-z0-9,{} ]*)$", names )
    if any( matches .== nothing )
        error( "Couldn't work out type(s) of:\n" * join( joinpath.( dir, names[matches.==nothing] ), "\n" ) * "\n" )
    end

    captures = getfield.( matches, :captures )
    cols = getindex.( captures, 1 )
    types = getindex.( captures, 2 )

    coldata = Vector[]
    for i = 1:length(cols)
        transformedtype = get( transformtypes, types[i], types[i] )
        datatype = Base.eval(Main, Meta.parse(transformedtype))

        filename = joinpath( dir, names[i] )
        filesize = stat( filename ).size
        n = Int(filesize/sizeof(datatype))
        
        col = Mmap.mmap( filename, Vector{datatype}, n )
        push!( coldata, col )
    end
    df = NamedTuple{(Symbol.(cols)...,)}( coldata )
    return df
end

Base.size( comma::NamedTuple{T,U} ) where {T,U} = (length(comma[1]), length(comma))

Base.size( comma::NamedTuple{T,U}, i::Int ) where {T,U} = size(comma)[i]

struct SubComma{T,U,V <: AbstractVector{Int}}
    comma::NamedTuple{T,U}
    indices::V
end

Base.size( subcomma::SubComma{T,U,V} ) where {T,U,V} = (length(subcomma.indices), length(subcomma.comma))

Base.size( subcomma::SubComma{T,U,V}, i::Int ) where {T,U,V} = size(subcomma)[i]

append( comma::Union{NamedTuple{T,U}, SubComma{T,U,W}}, pair::Pair{Symbol,V} ) where {T,U,V<:AbstractVector,W} =
    NamedTuple{(T...,pair[1])}( (values(comma)...,pair[2]) )

Base.getindex( comma::NamedTuple{T,U}, ::Colon, column::String ) where {T,U} = comma[Symbol(column)]

Base.getindex( comma::NamedTuple{T,U}, ::Colon, column::Symbol ) where {T,U} = comma[column]

DataFrames.DataFrame( comma::Union{NamedTuple{T,U}, SubComma{T,U,V}} ) where {T,U,V} =
    DataFrame( [values(comma)...], [keys(comma)...] )

Base.getindex( comma::NamedTuple{T,U}, columns::AbstractVector{Symbol} ) where {T,U} =
    NamedTuple{(columns...,)}(getfield.( [comma], columns ))

Base.getindex( comma::NamedTuple{T,U}, keep::BitVector, column::Symbol ) where {T,U} = comma[keep,[column]]

Base.getindex( comma::NamedTuple{T,U}, keep::BitVector, columns::AbstractVector{Symbol} ) where {T,U} =
    SubComma( NamedTuple{(columns...,)}( [getfield(comma,c) for c in columns] ), findall(keep) )

Base.getindex( comma::NamedTuple{T,U}, keep::BitVector, ::Colon ) where {T,U} = comma[keep, collect(keys(comma))]

Base.getindex( comma::NamedTuple{T,U}, indices::AbstractVector{Int}, columns::AbstractVector{Symbol} ) where {T,U} =
    SubComma( NamedTuple{(columns...,)}( [getfield(comma,c) for c in columns] ), indices )

Base.getindex( comma::NamedTuple{T,U}, indices::AbstractVector{Int}, ::Colon ) where {T,U} = comma[indices, collect(keys(comma))]

Base.lastindex( comma::NamedTuple{T,U}, args... ) where {T,U} = length(comma[1])

Base.keys( subcomma::SubComma{T,U,V} ) where {T,U,V} = keys( subcomma.comma )
Base.values( subcomma::SubComma{T,U,V} ) where {T,U,V} = getindex.( values( subcomma.comma ), [subcomma.indices] )

struct SubCommaColumn{T,U<:AbstractVector{Int}} <: AbstractVector{T}
    v::Vector{T}
    indices::U
end

Base.getindex( subcomma::SubComma{T,U,V}, col ) where {T,U,V} = SubCommaColumn( subcomma.comma[col], subcomma.indices )

Base.getindex( col::SubCommaColumn{T,U}, i::Int ) where {T,U} = col.v[col.indices[i]]

Base.length( col::SubCommaColumn{T,U} ) where {T,U} = length(col.indices)

Base.size( col::SubCommaColumn{T,U} ) where {T,U} = (length(col),)

function lexicographic( vs... )
    function lt( x, y )
        map(vs) do v
            if v[x] < v[y]
                return true
            elseif v[x] > v[y]
                return false
            end
        end
        return false
    end
    return lt
end

function Base.sort( comma::NamedTuple{T,U}, keys::Vararg{Symbol} ) where {T,U}
    indices = collect(1:size(comma,1))
    sort!( indices, lt=lexicographic( getindex.( [comma], keys )... ) )
    return SubComma( comma, indices )
end

function groupby( comma::NamedTuple{T,U}, v::Vector{V} ) where {T,U,V}
    changes = [0;findall(v[1:end-1].!=v[2:end]);length(v)]
    return [comma[changes[i]+1:changes[i+1],:] for i in 1:length(changes)-1]
end

formats = Dict(
    Dates.Date => DateFormat( "mm/dd/yyyy" ),
    Dates.Time => DateFormat( "HH:MM:SS.sss" ),
    Float32 => "%0.2f",
    Float64 => "%0.2f",
    Dates.DateTime => DateFormat( "mm/dd/yyyy HH:MM:SS.sss" ),
)

format( d::Dates.TimeType ) = Dates.format( d, formats[typeof(d)] )
format( x::Number ) = sprintf1( formats[typeof(x)], x )
format( x ) = string(x)
format( x::Integer ) = string(x)

align( d::Dates.TimeType ) = rpad
align( x::Number ) = lpad
align( c::NTuple{N,UInt8} where {N} ) = rpad

function showcomma(
    io::IO,
    df;
    toprows::Int = div(displaysize(io)[1], 2) - 3,
    bottomrows::Int = toprows,
    termwidth::Int = displaysize(io)[2],
)
    toprows = min(toprows, length(df[1]))
    bottomrows = min(bottomrows, length(df[1]) - toprows)
    
    columns = Vector{String}[]
    totallength = 0
    for k in keys(df)
        col = df[k]

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

Base.show( io::IO, df::NamedTuple; kwargs... ) = showcomma( io, df; kwargs... )
Base.show( io::IO, df::SubComma; kwargs... ) = showcomma( io, df; kwargs... )

const CharN{N} = NTuple{N,UInt8}

Base.convert( ::Type{CharN{N}}, x::AbstractString ) where {N} = convert( CharN{N}, (rpad(x,N)...,) )

Base.convert( ::Type{String}, x::CharN{N} ) where {N} = strip(String([x...]))

# This data structure doesn't support strings so this is the alternative for now
Base.show( io::IO, tuple::NTuple{N,UInt8} ) where {N} = print( io, String(UInt8[tuple...]) )

end # module
