export VariableLengthStringVector

using StringViews

export StringViews

const StringType = StringViews.StringView{SubArray{UInt8, 1, Vector{UInt8}, Tuple{UnitRange{Int64}}, true}}

struct VariableLengthStringVector <: AbstractVector{StringType}
    data::Vector{UInt8}
    indices::Vector{Int}
end

function VariableLengthStringVector( vs::AbstractVector{S} ) where {S <: AbstractString}
    lens = length.(vs)
    cumlens = cumsum(lens)
    data = Vector{UInt8}( undef, cumlens[end] )
    i = 1
    j = 1
    while j <= length(vs)
        for k = 1:lens[j]
            data[i] = UInt8(vs[j][k])
            i += 1
        end
        j += 1
    end
    
    return VariableLengthStringVector( data, [0; cumlens] )
end

VariableLengthStringVector( vs::AbstractVector{Union{S,Missing}} ) where {S <: AbstractString} = 
    VariableLengthStringVector( demissing( vs ) )

VariableLengthStringVector( v::AbstractVector{CharN{N}} ) where {N} = VariableLengthStringVector( rstrip.( string.( v ) ) )

Base.promote_rule( ::Type{StringType}, ::Type{CharN{N}} ) where {N} = StringType

Base.Broadcast.broadcasted( convert, ::Type{StringType}, v::AbstractVector{CharN{N}} ) where {N} =
    VariableLengthStringVector( v )

CommaColumn( v::VariableLengthStringVector, indices::V = 1:length(v) ) where {V <: AbstractVector{Int}} =
    CommaColumn{StringType,VariableLengthStringVector,V}( v, indices )

Base.getindex( s::VariableLengthStringVector, i::Int ) = StringView( view( s.data, s.indices[i]+1:s.indices[i+1] ) )

Base.getindex( s::VariableLengthStringVector, i::AbstractVector{Int} ) =
    VariableLengthStringVector( getindex.( [s], i ) )

Base.size( s::VariableLengthStringVector ) = (length(s.indices)-1,)

Base.IndexStyle( ::Type{VariableLengthStringVector} ) = IndexLinear()

function Base.read( filename::String, ::Type{CommaColumn{StringType}} )
    data = Mmap.mmap( filename, Vector{UInt8}, stat(filename).size )

    indices_file = filename * "_indices"
    n = Int(stat(indices_file).size/8)
    indices = Mmap.mmap( indices_file, Vector{Int}, n )
        
    return CommaColumn( VariableLengthStringVector( data, indices ), 1:n-1 )
end

function Base.write( filename::AbstractString, data::VariableLengthStringVector;
    append::Bool = false, buffersize=2^20 )
    
    io = open(  filename, append ? "a" : "w" )
    write_buffered( io, data.data, append=append, buffersize=buffersize )
    close(io)

    colfilename = "$(filename)_indices"
    n = append ? Int(stat(colfilename).size/8) : 0
    start = append && n > 0 ? 2 : 1
    io = open(  colfilename, append ? "a" : "w" )
    write_buffered( io, n .+ data.indices[start:end], append=append, buffersize=buffersize )
    close(io)
end

function convert_buffered( infile::AbstractString, intype::Type{CharN{N}},
                           outfile::AbstractString, outtype::Type{StringType};
                           buffersize::Int = 2^20 ) where {N}
    n = Int(stat(infile).size/N)
    m = Int(buffersize)
    inbuffer = Vector{CharN{N}}( undef, m )
    
    fin = open( infile, "r" )
    i = 1
    while i + m - 1 <= n
        read!( fin, inbuffer )
        out = VariableLengthStringVector( inbuffer )
        write( outfile, out, append=true )
        i += m
    end
    if i < n
        stop = n-i+1
        remaining = view( inbuffer, 1:stop )
        read!( fin, remaining )
        out = VariableLengthStringVector( remaining )
        write( outfile, out, append=true )
    end
end

# this could be made more memory efficient
Base.write( filename::AbstractString, data::CommaColumn{StringType,VariableLengthStringVector}; append=false ) =
    write( filename, data.v[data.indices], append=append )

align( ::StringType ) = rpad

push!( suffixes, r"_indices$" )

