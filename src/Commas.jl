module Commas

using Dates
using Mmap
using Formatting
using DataFrames

export CharN, groupby

abstract type AbstractComma{T,U}
end

struct Comma{T,U} <: AbstractComma{T,U}
    comma::NamedTuple{T,U}
end

mutable struct SubComma{T,U,V <: AbstractComma{T,U}, W <: AbstractVector{Int}} <: AbstractComma{T,U}
    comma::V
    indices::W
end

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

Base.names( comma::Comma{T,U} ) where {T,U} = string.(keys(comma.comma))
Base.names( comma::SubComma{T,U} ) where {T,U} = names(comma.comma)

function writecomma( dir::String, data::Union{DataFrame,AbstractComma{T,U}}; append::Bool = false ) where {T,U}
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
    return Comma( df )
end

Base.size( comma::Comma{T,U} ) where {T,U} = (length(comma.comma[1]), length(comma.comma))
Base.size( comma::SubComma{T,U,V,W} ) where {T,U,V,W} = (length(comma.indices), size(comma.comma,2))

Base.size( comma::AbstractComma{T,U}, i::Int ) where {T,U} = size(comma)[i]

struct SubCommaColumn{T,U<:AbstractVector{T},V<:AbstractVector{Int}} <: AbstractVector{T}
    v::U
    indices::V
end

Base.keys( subcomma::Union{Comma{T,U},SubComma{T,U,V,W}} ) where {T,U,V,W} = keys( subcomma.comma )
Base.values( comma::Comma{T,U} ) where {T,U} = values(comma.comma)
Base.values( subcomma::SubComma{T,U,V,W} ) where {T,U,V,W} = getindex.( values( subcomma.comma ), [subcomma.indices] )
DataFrames.DataFrame( comma::Union{NamedTuple{T,U}, SubComma{T,U,V}} ) where {T,U,V} =
    DataFrame( [values(comma)...], [keys(comma)...] )

Base.getindex( comma::Comma{T,U}, column::String ) where {T,U} = comma.comma[Symbol(column)]
Base.getindex( comma::Comma{T,U}, column::Symbol ) where {T,U} = comma.comma[column]
Base.getindex( comma::Comma{T,U}, ::Colon, column::Union{String,Symbol} ) where {T,U} = comma[column]
Base.getindex( comma::SubComma{T,U,V,W}, ::Colon, column::Union{Symbol,String} ) where {T,U,V,W} = comma.comma[column]

Base.getindex( comma::Comma{T,U}, columns::AbstractVector{Symbol} ) where {T,U} =
    Comma( NamedTuple{(columns...,)}(getfield.( [comma.comma], columns )) )
Base.getindex( comma::SubComma{T,U,V,W}, columns::AbstractVector{Symbol} ) where {T,U,V,W} =
    SubComma( comma.comma[columns], comma.indices )

Base.getindex( comma::Comma{T,U}, i::Int, column::Symbol ) where {T,U} = comma.comma[column][i]
Base.getindex( comma::Comma{T,U}, i::Int, column::String ) where {T,U} = comma.comma[Symbol(column)][i]
Base.getindex( comma::SubComma{T,U,V,W}, i::Int, column::Union{Symbol,String} ) where {T,U,V,W} =
    comma.comma[comma.indices[i],column]

Base.getindex( comma::Union{Comma{T,U},SubComma{T,U,V,W}}, keep::BitVector, columns::AbstractVector{Symbol} ) where {T,U,V,W} =
    SubComma( comma[columns], findall(keep) )
    
Base.getindex( comma::Union{Comma{T,U},SubComma{T,U,V,W}}, keep::BitVector, column::Symbol ) where {T,U,V,W} = comma[keep,[column]]

Base.getindex( comma::Union{Comma{T,U},SubComma{T,U,V,W}}, keep::BitVector, ::Colon ) where {T,U,V,W} =
    comma[keep, collect(keys(comma))]

Base.getindex( comma::Union{Comma{T,U},SubComma{T,U,V,W}},
               indices::AbstractVector{Int}, columns::AbstractVector{Symbol} ) where {T,U,V,W} =
                   SubComma( comma[columns], indices )

Base.getindex( comma::Union{Comma{T,U},SubComma{T,U,V,W}}, indices::AbstractVector{Int}, ::Colon ) where {T,U,V,W} =
    comma[indices, collect(keys(comma))]

Base.lastindex( comma::Comma{T,U}, args... ) where {T,U} = length(comma[1])
Base.lastindex( comma::SubComma{T,U,V,W}, args... ) where {T,U,V,W} = length(comma.indices)

Base.getindex( subcomma::SubComma{T,U,V,W}, col ) where {T,U,V,W} = SubCommaColumn( subcomma.comma[col], subcomma.indices )

Base.getindex( col::SubCommaColumn{T,U}, i::Int ) where {T,U} = col.v[col.indices[i]]

Base.length( col::SubCommaColumn{T,U} ) where {T,U} = length(col.indices)

Base.size( col::SubCommaColumn{T,U} ) where {T,U} = (length(col),)

lexicographic( vs... ) = i -> getindex.( vs, i )

function Base.sort( comma::Union{Comma{T,U},SubComma{T,U,V,W}}, keys::Vararg{Symbol} ) where {T,U,V,W}
    indices = collect(1:size(comma,1))
    sort!( indices, by=lexicographic( getindex.( [comma], keys )... ) )
    return SubComma( comma, indices )
end

#=
struct GroupedComma{T,U,V,W}
    sorted::SubComma{T,U,V}
    result::SubComma{T,U,W}
    changes::Vector{Int}
    i::Int
end

different( vs... ) =
    reduce( .&, 
            map(vs) do v
                v[1:end-1] .!= v[2:end]
            end )

function groupby( comma::NamedTuple{T,U}, keys::Vararg{Symbol} ) where {T,U}
    sorted = sort( comma, keys... )
    changes = [0;different( getindex.( [sorted], keys )... )]
    return GroupedComma( sorted, SubComma( comma, sorted.indices[1:changes[2]] )
end
=#

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
    df::V;
    toprows::Int = div(displaysize(io)[1], 2) - 3,
    bottomrows::Int = toprows,
    termwidth::Int = displaysize(io)[2],
) where {T,U,V <: AbstractComma{T,U}}
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
