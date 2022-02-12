module Commas

using Dates
using Mmap
using Formatting
using DataFrames

export CharN, Comma, CommaColumn, groupby

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
Comma( S, nt::NamedTuple{T,U}, indices::V ) where {T,U,V} = 
    Comma{S,T,U,NamedTuple{T,U},V}( nt, indices )

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

CommaColumn( v::AbstractVector, indices::V = 1:length(v) ) where {V} = CommaColumn( v, indices )

# required transformations to move from 0.6 to 1.0
transformtypes = Dict(
    "Date" => "Commas.Dates.Date",
    "DateTime" => "Commas.Dates.DateTime",
    "Base.Dates.Time" => "Commas.Dates.Time",
)

Base.write( io::IO, v::Base.ReinterpretArray{T,U,V,W} ) where {T,U,V,W} =
    write( io, reinterpret( V, v ) )

function Base.write( filename::String, data::CommaColumn{T,U,V}; append::Bool = false ) where {T,U,V}
    io = open( joinpath( filename * "_$T" ), write=true, append=append )
    n = length(data)
    if data.indices == 1:n
        write( io, data.v )
    else
        for i = 1:length(data)
            write( io, data[i] )
        end
    end
    close( io )
end

function Base.read( filename::String, ::Type{CommaColumn{T}} ) where {T}
    filesize = stat( filename ).size
    n = sizeof(T) == 0 ? 0 : Int(filesize/sizeof(T))
        
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

function Base.read( dir::String, ::Type{Comma}; startcolindex=1, endcolindex=Inf )
    names = readdir( dir )
    matches = match.( r"^(.*)_([A-z0-9,{} ]*)$", names )
    if any( matches .== nothing )
        error( "Couldn't work out type(s) of:\n" * join( joinpath.( dir, names[matches.==nothing] ), "\n" ) * "\n" )
    end

    captures = getfield.( matches, :captures )
    cols = getindex.( captures, 1 )
    types = getindex.( captures, 2 )

    #nt = NamedTuple{}()
    range = startcolindex:Int(min(endcolindex,length(cols)))
    data = []
    for i in range
        transformedtype = get( transformtypes, types[i], types[i] )
        datatype = Base.eval(Main, Meta.parse(transformedtype))

        filename = joinpath( dir, names[i] )
        col = read( filename, CommaColumn{datatype} );
        #        nt = merge( nt, (;Symbol(cols[i]) => col) );
        push!( data, col )
    end
    return Comma( NamedTuple{(Symbol.(cols[range])...,)}( data ) )
end

Base.size( comma::Comma{S,T,U,NamedTuple{T,U}} ) where {S,T,U} = (length(comma.comma[1]), length(comma.comma))
Base.size( comma::AbstractComma{T,U} ) where {T,U} = (length(comma.indices), size(comma.comma,2))

Base.size( comma::AbstractComma{T,U}, i::Int ) where {T,U} = size(comma)[i]

Base.keys( subcomma::AbstractComma{T,U} ) where {T,U} = keys( subcomma.comma )
Base.values( comma::Comma{S,T,U,NamedTuple{T,U}} ) where {S,T,U} = values(comma.comma)
Base.values( subcomma::AbstractComma{T,U} ) where {T,U} = getindex.( values( subcomma.comma ), [subcomma.indices] )

DataFrames.DataFrame( comma::AbstractComma{T,U} ) where {T,U} =
    DataFrame( Any[values(comma)...], [keys(comma)...] )

function Base.vcat( comma::Comma{S,T,U,NamedTuple{T,U},UnitRange{Int}}, kwargs... ) where {S,T,U}
    n = size(comma,1)
    @assert( comma.indices.start == 1 )
    @assert( n == length(comma.comma[1]) )
    ks = (T...,getindex.(kwargs, 1)...)
    vs = (values(comma.comma)...,getindex.(kwargs, 2)...);
    nt = NamedTuple{ks}( vs );
    result = Comma( S, nt, 1:n );
    return result
end

Base.getindex( comma::AbstractComma{T,U}, ::Colon, column::Union{String,Symbol} ) where {T,U} = comma[column]

Base.getindex( comma::Comma{S,T,U,NamedTuple{T,U}}, columns::AbstractVector{Symbol} ) where {S,T,U} =
    Comma( NamedTuple{(columns...,)}(getfield.( [comma.comma], columns )) )
Base.getindex( comma::AbstractComma{T,U}, columns::AbstractVector{Symbol} ) where {T,U} =
    Comma( comma.comma[columns], comma.indices )

Base.getindex( comma::AbstractComma{T,U}, i::Int, column::Symbol ) where {T,U} = comma.comma[column][comma.indices[i]]
Base.getindex( comma::AbstractComma{T,U}, i::Int, column::String ) where {T,U} = comma[i,Symbol(column)]

Base.getindex( comma::AbstractComma{T,U}, keep::AbstractVector{Bool}, columns::AbstractVector{Symbol} ) where {T,U} =
    Comma( comma[columns], findall(keep) )
    
Base.getindex( comma::AbstractComma{T,U}, keep::AbstractVector{Bool}, column::Symbol ) where {T,U} = comma[keep,[column]]

Base.getindex( comma::AbstractComma{T,U}, keep::AbstractVector{Bool} ) where {T,U} =
    comma[keep, collect(keys(comma))]

Base.getindex( comma::AbstractComma{T,U}, keep::AbstractVector{Bool}, ::Colon ) where {T,U} =
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

function Base.sort( comma::Comma{S,T,U,V,W}, ks::Vararg{Symbol}; kwargs... ) where {S,T,U,V,W}
    indices = collect(1:size(comma,1))
    lt = lexicographic( getindex.( [comma], ks )... )
    if issorted( indices, by=lt; kwargs... )
        return Comma{ks,T,U,V,W}( comma.comma, comma.indices )
    else
        sort!( indices, by=lt; kwargs... )
        return Comma{ks,T,U,Comma{S,T,U,V,W},Vector{Int}}( comma, indices )
    end
end

sortcols( comma::Comma{S,T,U,V,W}, i::Int ) where {S,T,U,V,W} = getindex.( (comma,), i, S )

Base.getindex( comma::AbstractComma{T,U}, i::Int, S::NTuple{N,Symbol} ) where {T,U,N} = getindex.( (comma,), i, S )

struct Groups{S,T,U,V,W}
    changes::Vector{Int}
    comma::Comma{S,T,U,V,W}
end

Base.length( groups::Groups ) = length( groups.changes )-1

function findchanges( lt::F, n::Int ) where {F <: Function}
    changes = [1]

    i = 1
    prev = lt(i)
    i += 1
    next = lt(i)
    while i <= n
        next = lt(i)
        if prev != next
            push!( changes, i )
            prev = next
        end
        i += 1
    end
    push!( changes, i )
    return changes
end

function groupby( comma::Comma{S,T,U,V,W} ) where {S,T,U,V,W}
    changes = findchanges( lexicographic( getindex.( [comma], S )... ), size(comma,1) )
    return Groups( changes, comma )
end

function Base.iterate( groups::Groups, i::Int = 1 )
    if i < length(groups.changes)
        return (Comma( groups.comma, groups.changes[i]:groups.changes[i+1]-1 ), i+1)
    else
        return nothing
    end
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
        if sizeof(eltype(col)) != 0
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
    end
    print( io, join( .*( columns... ), '\n' ) )
end

const CharN{N} = NTuple{N,UInt8}

Base.convert( ::Type{CharN{N}}, x::AbstractString ) where {N} = convert( CharN{N}, (rpad(x,N)...,) )

Base.convert( ::Type{String}, x::CharN{N} ) where {N} = strip(String([x...]))

# This data structure doesn't support strings so this is the alternative for now
Base.show( io::IO, tuple::NTuple{N,UInt8} ) where {N} = print( io, String(UInt8[tuple...]) )

function CommaColumn( v::AbstractVector{T}, indices::V = 1:length(v) ) where {T <: AbstractString, V <: AbstractVector{Int}}
    N = length(v)
    l = mapreduce( length, max, v )
    if l > 0
        vu = fill( UInt8(' '), N*l )

        j = 1
        for i = 1:N
            copyto!( vu, j, v[i] )
            j += l
        end
        vc = reinterpret( CharN{l}, vu )
    else
        vc = Vector{CharN{0}}( undef, N )
    end

    return CommaColumn( vc, indices )
end

function Comma( dir::String, df::DataFrame )
    mkpath(dir)
    for name in names(df)
        write( joinpath( dir, name ), CommaColumn( df[!,name] ) )
    end
    return read( dir, Comma )
end

end # module
