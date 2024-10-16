using Dates
using Mmap
using Format
using DataFrames
using ZippedArrays
using MissingTypes
using SentinelArrays

import DataFrames: groupby

export CharN, Comma, CommaColumn, groupby, sortkeys, combine

abstract type AbstractComma{T,U}
end

mutable struct Comma{S,T,U,V <: Union{NamedTuple{T,U},AbstractComma{T,U}},
                     W <: AbstractVector{Int}} <: AbstractComma{T,U}
    comma::V
    indices::W
end

sortkeys( ::Comma{S,T,U,V,W} ) where {S,T,U,V,W} = S

Comma( comma::NamedTuple{T,U}, v::V = 1:length(comma[1]) ) where {T,U,V <: AbstractVector{Int}} =
    Comma{(),T,U,NamedTuple{T,U},V}( comma, v )
Comma( comma::V, v::W = 1:size(comma,1) ) where {T,U,V <: AbstractComma{T,U}, W<: AbstractVector{Int}} =
    Comma{(),T,U,V,W}( comma, v )
Comma( S, nt::NamedTuple{T,U}, indices::V ) where {T,U,V} = 
    Comma{S,T,U,NamedTuple{T,U},V}( nt, indices )

function Comma( df::DataFrame )
    ks = Symbol.(names(df))
    nt = NamedTuple{(ks...,)}( CommaColumn.(eachcol(df)) )
    return Comma( nt )
end

Base.getindex( comma::Comma{S,T,U,V,W}, column::Symbol ) where {S,T,U,V,W} = CommaColumn( comma.comma[column], comma.indices )

Base.names( comma::Comma{S,T,U,NamedTuple{T,U}} ) where {S,T,U} = string.(keys(comma.comma))
Base.names( comma::Comma ) = names(comma.comma)

eltypes( comma::Comma{S,T,U,NamedTuple{T,U}} ) where {S,T,U} = eltype.(values(comma.comma))
eltypes( comma::Comma ) = eltypes(comma.comma)

Base.getindex( comma::AbstractComma{T,U}, column::String ) where {T,U} =
    CommaColumn( comma.comma[Symbol(column)], comma.indices )

function convert_buffered( infile::AbstractString, intype::Type{T},
                             outfile::AbstractString, outtype::Type{U};
                             buffersize::Int = 2^20 ) where {T,U}
    n = Int(stat(infile).size/sizeof(T))
    m = buffersize
    inbuffer = Vector{T}( undef, m )
    outbuffer = Vector{U}( undef, m )
    
    fin = open( infile, "r" )
    out = open( outfile, "w" )
    i = 1
    while i + m - 1 <= n
        read!( fin, inbuffer )
        for j = 1:m
            outbuffer[j] = convert( U, inbuffer[j] )
        end
        write( out, outbuffer )
        i += m
    end
    if i < n
        stop = n-i+1
        remainingin = view( inbuffer, 1:stop )
        remainingout = view( outbuffer, 1:stop )
        read!( fin, remainingin )
        remainingout[1:stop] = convert.( U, remainingin[1:stop] )
        write( out, remainingout )
    end
        
    close( fin )
    close( out )
end

function append( dir::String, data::AbstractComma{T,U};
                 verbose::Bool = false, buffersize=2^20 ) where {T,U}
    (filenames, cols, types) = names_types( dir )
    typedict = Dict( cols .=> types )
    filenamedict = Dict( cols .=> filenames )

    newnames = names(data)
    @assert( Set(cols) == Set(newnames) )

    newtypes = eltypes(data)
    for i = 1:length(newnames)
        name = newnames[i]
        if verbose
            println( "Writing $name at $(now())" )
        end

        filename = filenamedict[name]
        type = typedict[name]
        coldata = data[name]
        newtype = newtypes[i]
        if newtype != type
            jointype = promote_type( newtype, type )
            if type != jointype
                newfilename = "$(name)_$jointype"
                filepath = joinpath( dir, filename )
                convert_buffered( filepath, type, joinpath( dir, newfilename ), jointype)
                rm( filepath )
                filename = newfilename
            end
            if newtype != jointype
                coldata = CommaColumn( convert.( jointype, coldata ) )
            end
        end
        
        write( joinpath( dir, filename ), coldata, append=true )
    end
end
    
function Base.write( dir::String, data::AbstractComma{T,U};
                     append::Bool = false, verbose::Bool = false ) where {T,U}
    if append && isdir( dir )
        return Commas.append( dir, data, verbose=verbose )
    end
    mkpath( dir )
    ns = names(data)
    ts = eltypes(data)
    for i in 1:length(ns)
        name = ns[i]
        col = data[name]
        write( joinpath( dir, name * "_$(ts[i])" ), col )
    end
end

const base_filename = r"^(.*)_([A-Za-z0-9,{}\. ]*)$"
const suffixes = Regex[]

function names_types( dir::String )
    names = readdir( dir )
    matches = match.( base_filename, names )
    bad = matches .== nothing
    if any( bad )
        error( "Couldn't work out type(s) of:\n" * join( joinpath.( dir, bad ), "\n" ) * "\n" )
    else
        has_suffix = [any(match.( suffixes, names[i] ) .!= nothing) for i in 1:length(names)]
        names = names[.!has_suffix]
        matches = matches[.!has_suffix]
    end

    captures = getfield.( matches, :captures )
    cols = getindex.( captures, 1 )
    types = getindex.( captures, 2 )
    datatypes = Base.eval.([Main], Meta.parse.(types))
    return (names, cols, datatypes)
end

function Base.read( dir::String, ::Type{Comma}; startcolindex=1, endcolindex=Inf )
    (names, cols, types) = names_types( dir )

    range = startcolindex:Int(min(endcolindex,length(cols)))
    data = []
    for i in range
        filename = joinpath( dir, names[i] )
        col = read( filename, CommaColumn{types[i]} );
        push!( data, col.v )
    end
    return Comma( NamedTuple{(Symbol.(cols[range])...,)}( data ) )
end

Base.size( comma::Comma{S,T,U,NamedTuple{T,U}} ) where {S,T,U} = (length(comma.indices), length(comma.comma))
Base.size( comma::AbstractComma{T,U} ) where {T,U} = (length(comma.indices), size(comma.comma,2))

Base.size( comma::AbstractComma{T,U}, i::Int ) where {T,U} = size(comma)[i]

Base.keys( subcomma::AbstractComma{T,U} ) where {T,U} = keys( subcomma.comma )
Base.values( comma::Comma{S,T,U,NamedTuple{T,U}} ) where {S,T,U} = values(comma.comma)
Base.values( subcomma::AbstractComma{T,U} ) where {T,U} = getindex.( values( subcomma.comma ), [subcomma.indices] )

DataFrames.DataFrame( comma::AbstractComma{T,U} ) where {T,U} =
    DataFrame( Any[values(comma)...], [keys(comma)...] )

function Base.vcat( comma::Comma{S,T,U,V,W}, kv::Pair{Symbol, X} ) where {S,T,U,V,W,X <: AbstractVector}
    comma = materialize( comma )
    ks = (T...,kv[1])
    vs = (values(comma.comma)..., CommaColumn( kv[2], 1:size(comma,1) ))
    return Comma( S, NamedTuple{ks}( vs ), comma.indices )
end

function materialize( comma::Comma{S,T,U,V,W} ) where {S,T,U,V,W}
    n = length(comma.indices)
    vs = []
    for i in 1:length(T)
        c = materialize( comma[T[i]] )
        push!( vs, c.v )
    end
    return Comma( S, NamedTuple{T}( vs ), 1:n )
end

Base.getindex( comma::Comma{S,T,U,NamedTuple{T,U}}, columns::AbstractVector{Symbol} ) where {S,T,U} =
    Comma( NamedTuple{(columns...,)}(getfield.( [comma.comma], columns )), comma.indices )
Base.getindex( comma::AbstractComma{T,U}, columns::AbstractVector{Symbol} ) where {T,U} =
    Comma( comma.comma[columns], comma.indices )

Base.getindex( comma::AbstractComma{T,U}, i::Int, column::V ) where {T,U,V} = comma.comma[column][comma.indices[i]]
Base.getindex( comma::AbstractComma{T,U}, i::Int, column::String ) where {T,U} = comma[i,Symbol(column)]

Base.setindex!( comma::AbstractComma{T,U}, v::V, i::Int, column::Symbol ) where {T,U,V} = comma.comma[column][comma.indices[i]] = v
Base.setindex!( comma::AbstractComma{T,U}, v::V, i::Int, column::String ) where {T,U,V} = comma[i,Symbol(column)] = v

Base.getindex( comma::AbstractComma{T,U}, keep::AbstractVector{Bool}, columns::AbstractVector{Symbol} ) where {T,U} =
    Comma( comma[columns], findall(keep) )
    
Base.getindex( comma::AbstractComma{T,U}, keep::AbstractVector{Bool}, column::Symbol ) where {T,U} = comma[column][keep]

Base.getindex( comma::AbstractComma{T,U}, keep::AbstractVector{Bool} ) where {T,U} =
    comma[keep, collect(keys(comma))]

Base.getindex( comma::AbstractComma{T,U}, indices::AbstractVector{Int}, columns::AbstractVector{Symbol} ) where {T,U} =
    Comma( comma[columns], indices )
Base.getindex( comma::AbstractComma{T,U}, indices::AbstractVector{Int}, column::Symbol ) where {T,U} =
    comma[column][indices]

Base.getindex( comma::AbstractComma{T,U}, indices::AbstractVector{Int} ) where {T,U} =
    Comma( comma, indices )

Base.lastindex( comma::AbstractComma{T,U}, args... ) where {T,U} = length(comma.indices)

function Base.sort( comma::Comma{S,T,U,V,W}, ks::Vararg{Symbol}; type::Type{X} = UInt16 ) where {S,T,U,V,W,X}
    vs = materialize.(getindex.( [comma], reverse(ks) ) )
    perm = 1:length(vs[1])
    for v in vs
        perm = countingsortperm( perm, v, type );
    end
    return Comma{ks,T,U,Comma{S,T,U,V,W},Vector{Int}}( comma, perm )
end

sortcols( comma::Comma{S,T,U,V,W}, i::Int ) where {S,T,U,V,W} = getindex.( (comma,), i, S )

Base.getindex( comma::AbstractComma{T,U}, i::Int, S::NTuple{N,Symbol} ) where {T,U,N} = getindex.( (comma,), i, S )

struct Groups{S,T,U,V,W}
    changes::Vector{Int}
    comma::Comma{S,T,U,V,W}
end

Base.length( groups::Groups ) = length( groups.changes )-1

function findchanges( a::AbstractVector )
    n = length(a)
    changes = [1]

    i = 1
    prev = a[i]
    i += 1
    while i <= n
        next = a[i]
        if prev != next
            push!( changes, i )
            prev = next
        end
        i += 1
    end
    push!( changes, i )
    return changes
end

function groupby( comma::Comma{S,T,U,V,W}, fields::Symbol... ) where {S,T,U,V,W}
    @assert( S[1:length(fields)] == fields )
    changes = findchanges( ZippedArray( getindex.( [comma], fields )... ) )
    return Groups( changes, comma )
end

groupby( comma::Comma{S,T,U,V,W} ) where {S,T,U,V,W} = groupby( comma, S... )

function Base.iterate( groups::Groups, i::Int = 1 )
    if i < length(groups.changes)
        return (Comma( groups.comma, groups.changes[i]:groups.changes[i+1]-1 ), i+1)
    else
        return nothing
    end
end
        
function DataFrames.combine( f::Function, groups::Groups{S,T,U,V,W} ) where {S,T,U,V,W}
    group = first( groups )
    ks = keys(group)
    cols = [eltype(group[k])[] for k in ks]
    for group in groups
        output = f( group )
        n = size(output, 1)
        for i = 1:length(ks)
            for j = 1:n
                push!( cols[i], output[ks[i]][j] )
            end
        end
    end
    return Comma( NamedTuple{ks}( cols ) )
end

formats = Dict(
    Dates.Date => DateFormat( "mm/dd/yyyy" ),
    Dates.Time => DateFormat( "HH:MM:SS.sss" ),
    Dates.DateTime => DateFormat( "mm/dd/yyyy HH:MM:SS.sss" ),
)

format( d::Dates.TimeType ) = Dates.format( d, formats[typeof(d)] )
format( x::Number ) = Format.format( x, precision=2 )
format( x ) = string(x)
format( x::Integer ) = string(x)

align(d::Dates.TimeType ) = rpad
align( x::Number ) = lpad
align( c::NTuple{N,UInt8} ) where N = rpad
align( x::MissingType{T} ) where T = align( x.x )

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
            
            collength = min( maximum(length.(colstrings)) + 1, termwidth - totallength )
            totallength += collength
            
            colstrings = getindex.( align( col[1] ).( colstrings, collength - 1 ), [1:collength-1] ) .* " "
            push!( columns, colstrings )
            totallength >= termwidth && break
        end
    end
    print( io, join( .*( columns... ), '\n' ) )
end

const CharN{N} = NTuple{N,UInt8}

Base.promote_type( ::Type{CharN{N}}, ::Type{CharN{M}} ) where {N,M} = CharN{max(M,N)}

Base.convert( ::Type{CharN{N}}, x::AbstractString ) where {N} = convert( CharN{N}, (x*' '^(N-length(x))...,) )

Base.convert( ::Type{String}, x::CharN{N} ) where {N} = strip(String([x...]))

Base.convert( ::Type{CharN{N}}, x::CharN{M} ) where {N,M} = (x..., fill(UInt8(' '),N-M)...)

Base.:(==)( s1::CharN{N}, s2::String ) where {N} = length(s2) > N ? false : convert( CharN{N}, s2 ) == s1
Base.:(==)( s1::String, s2::CharN{N} ) where {N} = length(s1) > N ? false : convert( CharN{N}, s1 ) == s2

# This data structure doesn't support strings so this is the alternative for now
Base.show( io::IO, tuple::NTuple{N,UInt8} ) where {N} = print( io, String(UInt8[tuple...]) )

Base.show( io::IO, df::Type{Comma{S,T,U,V,W}} ) where {S,T,U,V,W} = 
    print( io, "Comma{$S,...,$W}" )
