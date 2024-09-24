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

mutable struct Comma{S,T,U,V <: Union{NamedTuple{T,U},AbstractComma{T,U}},W <: AbstractVector{Int}} <: AbstractComma{T,U}
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
    nt = NamedTuple{(ks...,)}( eachcol(df) )
    return Comma( nt )
end

struct CommaColumn{T,U<:AbstractVector{T},V<:AbstractVector{Int}} <: AbstractVector{T}
    v::U
    indices::V
end

CommaColumn( v::AbstractVector, indices::V = 1:length(v) ) where {V} = CommaColumn( v, indices )

Base.length( col::CommaColumn ) = length(col.indices)
Base.size( col::CommaColumn ) = (length(col),)
Base.getindex( col::CommaColumn, i::Int ) = col.v[col.indices[i]]

function Base.setindex!( col::CommaColumn, v, i::Int )
    col.v[col.indices[i]] = v
end

Base.getindex( comma::Comma{S,T,U,V,W}, column::Symbol ) where {S,T,U,V,W} = CommaColumn( comma.comma[column], comma.indices )

# required transformations to move from 0.6 to 1.0
transformtypes = Dict(
    "Dates.Date" => "Commas.Dates.Date",
    "Date" => "Commas.Dates.Date",
    "DateTime" => "Commas.Dates.DateTime",
    "Base.Dates.Time" => "Commas.Dates.Time",
)

Base.write( io::IO, v::Base.ReinterpretArray{T,U,V,W} ) where {T,U,V,W} =
    write( io, reinterpret( V, v ) )

function write_buffered( filename::AbstractString, data::AbstractVector{T};
                         n = length(data), indices::AbstractVector{Int} = 1:n, append::Bool = false, buffersize=2^20 ) where {T}
    io = open(  filename, write=true, append=append )
    buffer = Array{T}( undef, buffersize )

    nb = 0
    i = 1
    while i <= n
        j = 1
        while j <= buffersize && i <= n
            buffer[j] = data[i]
            j += 1
            i += 1
        end
        if j <= buffersize
            resize!( buffer, j-1 )
        end
        nb += write( io, buffer )
    end
    close( io )
    return nb
end

Base.write( filename::AbstractString, data::CommaColumn{T,U,V}; append::Bool = false, buffersize=2^20 ) where {T,U,V} =
    write_buffered( filename, data.v, indices=data.indices, append=append, buffersize=buffersize )

function Base.write( filename::AbstractString,
                     v::CommaColumn{Union{Missing,T},U,V};
                     append::Bool=false
                     ) where {T,U <: AbstractVector{Union{Missing,T}},V}
    try
        write( filename, CommaColumn(convert( Vector{MissingType{T}}, v.v )), append=append )
    catch e
        error( "Error writing $filename" )
    end
end

# julia doesn't do well with MissingType{CharN{N}} for large N (seems to be LLVM-related) so let's just skip the MissingType for strings
demissing( v::AbstractVector{Union{Missing,T}} ) where {T <: AbstractString} = ifelse.( ismissing.(v), "", v )

function Base.write(
    filename::AbstractString,
    v::CommaColumn{Union{Missing,T},U,V};
    append::Bool=false,
) where {T <: AbstractString, U <: AbstractVector{Union{Missing,T}},V}
    w = demissing(v.v)
    N = reduce( max, length.(w), init=0 )
    write( filename, CommaColumn(convert.( CharN{N}, w )), append=append )
end

function Base.read( filename::String, ::Type{CommaColumn{T}} ) where {T}
    filesize = stat( filename ).size
    n = sizeof(T) == 0 ? 0 : Int(filesize/sizeof(T))
        
    return CommaColumn( Mmap.mmap( filename, Vector{T}, n ), 1:n )
end

Base.names( comma::Comma{S,T,U,NamedTuple{T,U}} ) where {S,T,U} = string.(keys(comma.comma))
Base.names( comma::Comma ) = names(comma.comma)

eltypes( comma::Comma{S,T,U,NamedTuple{T,U}} ) where {S,T,U} = eltype.(values(comma.comma))
eltypes( comma::Comma ) = eltypes(comma.comma)

Base.getindex( comma::AbstractComma{T,U}, column::String ) where {T,U} =
    CommaColumn( comma.comma[Symbol(column)], comma.indices )

type_name( dir, col, type ) = joipnath( dir, "$(name)_$type" )

function transform_buffered( infile::AbstractString, buffer::AbstractVector{T},
                             f::Function, outfile::AbstractString ) where {T}
    n = Int(stat(infile)/sizeof(T))
    m = length(inbuffer)
    
    in = open( infile, write=true )
    out = open( outfile, write=true )
    i = 1
    while i + m -1 <= n
        read!( in, inbuffer )
        outbuffer = f( inbuffer )
        write( out, outbuffer )
    end
    if i < n
        remaining = view( inbuffer, 1:n-i+1 )
        read!( in, remaining )
        outbuffer = f( remaining )
        write( out, outbuffer )
    end
        
    close( in )
    close( out )
end

function append( dir::String, data::AbstractComma{T,U};
                 verbose::Bool = false, buffersize=2^20 ) where {T,U}
    (filenames, cols, types) = names_types( dir )
    typedict = Dict( cols .=> types )
    filenamedict = Dict( filenames .=> types )

    newnames = names(data)
    @assert( Set(names) == Set(newnames) )

    newtypes = eltypes(data)
    for i = 1:length(newnames)
        name = newnames[i]
        newtype = newtypes[i]

        coldata = data[name]
        
        type = typedict[name]
        filename = filenamedict[name]
        
        if newtype != type
            jointype = promote_type( newtype, type )
            if type != jointype
                buffer = Vector{type}( undef, buffersize )
                f = b -> convert.( jointype, b )
                transform_buffered( filename, buffer, f, "$(col)_$jointype" )
            end
            if newtype != jointype
                coldata = convert.( jointype, coldata )
            end
        end
        write( filename, coldata, append=true )
    end
end
    
function Base.write( dir::String, data::AbstractComma{T,U};
                     append::Bool = false, verbose::Bool = false ) where {T,U}
    if append && isdir( dir )
        return append( dir, data, verbose=verbose )
    end
    mkpath( dir )
    ns = names(data)
    ts = eltypes(data)
    for i in 1:length(ns)
        name = ns[i]
        col = data[name]
        write( joinpath( dir, name * "_$(ts[i])" ), col, append=append )
    end
end

const base_filename = r"^([^_]*)_([A-Za-z0-9,{}\. ]*)"
const suffixes = Regex[]

function names_types( dir::String )
    names = readdir( dir )
    matches = match.( base_filename * r"$", names )
    if any( matches .== nothing )
        bad = matches .== nothing
        notbad = [any(match.( base_filename .* suffixes, names[i] ) .!= nothing) for i in 1:length(names)]
        if any(bad .& .!notbad)
            error( "Couldn't work out type(s) of:\n" * join( joinpath.( dir, names[bad[.!notbad]] ), "\n" ) * "\n" )
        else
            names = names[.!bad]
            matches = matches[.!bad]
        end
    end

    captures = getfield.( matches, :captures )
    cols = getindex.( captures, 1 )
    types = getindex.( captures, 2 )
    transformedtypes = get.( [transformtypes], types, types )
    datatypes = Base.eval.([Main], Meta.parse.(transformedtypes))
    return (names, cols, datatypes)
end

function Base.read( dir::String, ::Type{Comma}; startcolindex=1, endcolindex=Inf )
    (names, cols, types) = names_types( dir )

    range = startcolindex:Int(min(endcolindex,length(cols)))
    data = []
    for i in range
        filename = joinpath( dir, names[i] )
        col = read( filename, CommaColumn{types[i]} );
        #        nt = merge( nt, (;Symbol(cols[i]) => col) );
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

materialize( col::CommaColumn{T,Vector{T},UnitRange{Int}} ) where {T} = col

function materialize( col::CommaColumn{T,U,V} ) where {T,U <: CommaColumn,V}
    m = materialize( col.v )
    return CommaColumn( m.v[col.indices], 1:length(col.indices) )
end

materialize( col::AbstractVector ) = CommaColumn( col.v[col.indices], 1:length(col.indices) ) 

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

Base.getindex( col::CommaColumn{T,U,V}, i::Int ) where {T,U,V} = col.v[col.indices[i]]

Base.length( col::CommaColumn{T,U,V} ) where {T,U,V} = length(col.indices)

Base.size( col::CommaColumn{T,U,V} ) where {T,U,V} = (length(col),)

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

Base.convert( ::Type{CharN{N}}, x::AbstractString ) where {N} = convert( CharN{N}, (x*' '^(N-length(x))...,) )

Base.convert( ::Type{String}, x::CharN{N} ) where {N} = strip(String([x...]))

Base.:(==)( s1::CharN{N}, s2::String ) where {N} = length(s2) > N ? false : convert( CharN{N}, s2 ) == s1
Base.:(==)( s1::String, s2::CharN{N} ) where {N} = length(s1) > N ? false : convert( CharN{N}, s1 ) == s2

# This data structure doesn't support strings so this is the alternative for now
Base.show( io::IO, tuple::NTuple{N,UInt8} ) where {N} = print( io, String(UInt8[tuple...]) )

function CommaColumn(
    v::AbstractVector{T}, indices::V = 1:length(v) ) where {T <: AbstractString, V <: AbstractVector{Int}}
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

function Comma( dir::String, df::DataFrame; append=false )
    mkpath(dir)
    nt = NamedTuple([Symbol(n) => CommaColumn(c) for (n,c) in collect(pairs(eachcol(df)))]);
    return write( dir, Comma(nt), append=append )
end

Base.show( io::IO, df::Type{Comma{S,T,U,V,W}} ) where {S,T,U,V,W} = 
    print( io, "Comma{$S,...,$W}" )
