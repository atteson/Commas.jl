module Commas

using Dates
using Mmap
using Formatting
using DataFrames

export CharN, Comma, CommaColumn

abstract type AbstractComma{T,U}
end

mutable struct Comma{S,T,U,V <: Union{NamedTuple{T,U},AbstractComma{T,U}},W <: AbstractVector{Int}} <: AbstractComma{T,U}
    comma::V
    indices::W
end

Comma( comma::NamedTuple{T,U}, v::V = 1:length(comma[1]) ) where {T,U,V <: AbstractVector{Int}} =
    Comma{(),T,U,NamedTuple{T,U},V}( comma, v )
Comma( comma::V, v::W = 1:size(comma,1) ) where {T,U,V <: AbstractComma{T,U}, W<: AbstractVector{Int}} =
    Comma{(),T,U,V,W}( comma, v )

function Comma( df::DataFrame )
    ks = Symbol.(names(df))
    nt = NamedTuple{(ks...,)}( eachcol(df) )
    return Comma( nt )
end

struct CommaColumn{T,U<:AbstractVector{T},V<:AbstractVector{Int}} <: AbstractVector{T}
    v::U
    indices::V
end

Base.length( col::CommaColumn ) = length(col.indices)
Base.size( col::CommaColumn ) = (length(col),)
Base.getindex( col::CommaColumn, i::Int ) = col.v[col.indices[i]]

Base.getindex( comma::Comma, column::Symbol ) = CommaColumn( comma.comma[column], comma.indices )

CommaColumn( v::AbstractVector, indices::V = 1:size(comma,1) ) where {V} = CommaColumn( v, indices )

# required transformations to move from 0.6 to 1.0
transformtypes = Dict(
    "Date" => "Commas.Dates.Date",
    "DateTime" => "Commas.Dates.DateTime",
    "Base.Dates.Time" => "Commas.Dates.Time",
)

function Base.write( filename::String, data::CommaColumn{T,U,V}; append::Bool = false ) where {T,U,V}
    io = open( joinpath( filename * "_$T" ), write=true, append=append )
    for i = 1:length(data)
        write( io, data[i] )
    end
    close( io )
end

function Base.read( filename::String, ::Type{CommaColumn{T}} ) where {T}
    filesize = stat( filename ).size
    n = Int(filesize/sizeof(T))
        
    return CommaColumn( Mmap.mmap( filename, Vector{T}, n ), 1:n )
end

Base.names( comma::Comma{S,T,U,NamedTuple{T,U}} ) where {S,T,U} = string.(keys(comma.comma))
Base.names( comma::Comma ) = names(comma.comma)

Base.getindex( comma::AbstractComma{T,U}, column::String ) where {T,U} =
    CommaColumn( comma.comma[Symbol(column)], comma.indices )

function Base.write( dir::String, data::AbstractComma{T,U}; append::Bool = false ) where {T,U}
    mkpath( dir )
    for name in names(data)
        write( joinpath( dir, name ), data[name] )
    end
end

function Base.read( dir::String, ::Type{Comma} )
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
        push!( coldata, read( filename, CommaColumn{datatype} ) )
    end
    return Comma( NamedTuple{(Symbol.(cols)...,)}( coldata ) )
end

Base.size( comma::Comma{S,T,U,NamedTuple{T,U}} ) where {S,T,U} = (length(comma.comma[1]), length(comma.comma))
Base.size( comma::AbstractComma{T,U} ) where {T,U} = (length(comma.indices), size(comma.comma,2))

Base.size( comma::AbstractComma{T,U}, i::Int ) where {T,U} = size(comma)[i]

Base.keys( subcomma::AbstractComma{T,U} ) where {T,U} = keys( subcomma.comma )
Base.values( comma::Comma{S,T,U,NamedTuple{T,U}} ) where {S,T,U} = values(comma.comma)
Base.values( subcomma::AbstractComma{T,U} ) where {T,U} = getindex.( values( subcomma.comma ), [subcomma.indices] )

DataFrames.DataFrame( comma::AbstractComma{T,U} ) where {T,U} =
    DataFrame( Any[values(comma)...], [keys(comma)...] )

Base.getindex( comma::AbstractComma{T,U}, ::Colon, column::Union{String,Symbol} ) where {T,U} = comma[column]

Base.getindex( comma::Comma{S,T,U,NamedTuple{T,U}}, columns::AbstractVector{Symbol} ) where {S,T,U} =
    Comma( NamedTuple{(columns...,)}(getfield.( [comma.comma], columns )) )
Base.getindex( comma::AbstractComma{T,U}, columns::AbstractVector{Symbol} ) where {T,U} =
    Comma( comma.comma[columns], comma.indices )

Base.getindex( comma::AbstractComma{T,U}, i::Int, column::Symbol ) where {T,U} = comma.comma[column][comma.indices[i]]
Base.getindex( comma::AbstractComma{T,U}, i::Int, column::String ) where {T,U} = comma[i,Symbol(column)]

Base.getindex( comma::AbstractComma{T,U}, keep::BitVector, columns::AbstractVector{Symbol} ) where {T,U} =
    Comma( comma[columns], findall(keep) )
    
Base.getindex( comma::AbstractComma{T,U}, keep::BitVector, column::Symbol ) where {T,U} = comma[keep,[column]]

Base.getindex( comma::AbstractComma{T,U}, keep::BitVector, ::Colon ) where {T,U} =
    comma[keep, collect(keys(comma))]

Base.getindex( comma::AbstractComma{T,U}, indices::AbstractVector{Int}, columns::AbstractVector{Symbol} ) where {T,U} =
    Comma( comma[columns], indices )

Base.getindex( comma::AbstractComma{T,U}, indices::AbstractVector{Int}, ::Colon ) where {T,U} =
    comma[indices, collect(keys(comma))]

Base.lastindex( comma::AbstractComma{T,U}, args... ) where {T,U} = length(comma.indices)

Base.getindex( col::CommaColumn{T,U,V}, i::Int ) where {T,U,V} = col.v[col.indices[i]]

Base.length( col::CommaColumn{T,U,V} ) where {T,U,V} = length(col.indices)

Base.size( col::CommaColumn{T,U,V} ) where {T,U,V} = (length(col),)

lexicographic( vs... ) = i -> getindex.( vs, i )

function Base.sort( comma::Comma{S,T,U,V,W}, ks::Vararg{Symbol} ) where {S,T,U,V,W}
    indices = collect(1:size(comma,1))
    sort!( indices, by=lexicographic( getindex.( [comma], ks )... ) )
    return Comma{ks,T,U,Comma{S,T,U,V,W},Vector{Int}}( comma, indices )
end

Base.getindex( comma::AbstractComma{T,U}, i::Int, S::Tuple{} ) where {T,U} = getindex.( (comma,), i, T )
Base.getindex( comma::AbstractComma{T,U}, i::Int, S::NTuple{N,Symbol} ) where {T,U,N} = getindex.( (comma,), i, S )

Base.length( comma::Comma{NTuple{0,Symbol}} ) = size(comma,1)

function Base.iterate( comma::Comma{S,T,U,V,W}, i::Int = 1 ) where {S,T,U,V,W}
    if i > size(comma,1)
        return nothing
    else
        tuple = comma[i,S]
        j = i + 1
        while j < size(comma,1) && comma[j,S] == tuple
            j += 1
        end
        return (Comma( comma, i:j-1 ), j)
    end
end

function Base.length( comma::Comma )
    l = 0
    for group in comma
        l += 1
    end
    return l
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

function Base.show(
    io::IO,
    df::AbstractComma{T,U};
    toprows::Int = div(displaysize(io)[1], 2) - 3,
    bottomrows::Int = toprows,
    termwidth::Int = displaysize(io)[2],
) where {T,U}
    n = size(df,1)
    toprows = min(toprows, n)
    bottomrows = min(bottomrows, n - toprows)
    
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

const CharN{N} = NTuple{N,UInt8}

Base.convert( ::Type{CharN{N}}, x::AbstractString ) where {N} = convert( CharN{N}, (rpad(x,N)...,) )

Base.convert( ::Type{String}, x::CharN{N} ) where {N} = strip(String([x...]))

# This data structure doesn't support strings so this is the alternative for now
Base.show( io::IO, tuple::NTuple{N,UInt8} ) where {N} = print( io, String(UInt8[tuple...]) )

end # module
