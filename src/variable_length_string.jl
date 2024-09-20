export VariableLengthStringVector

using StringViews

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

CommaColumn( v::VariableLengthStringVector, indices::V = 1:length(v) ) where {V <: AbstractVector{Int}} =
    CommaColumn{StringType,VariableLengthStringVector,V}( v, indices )

Base.getindex( s::VariableLengthStringVector, i::Int ) = StringView( view( s.data, s.indices[i]+1:s.indices[i+1] ) )

Base.size( s::VariableLengthStringVector ) = (length(s.indices)-1,)

Base.IndexStyle( ::Type{VariableLengthStringVector} ) = IndexLinear()

function Base.read( filename::String, ::Type{CommaColumn{StringType}} )
    data = Mmap.mmap( filename, Vector{UInt8}, stat(filename).size )

    indices_file = filename * "_indices"
    n = Int(stat(indices_file).size/8)
    indices = Mmap.mmap( indices_file, Vector{Int}, n )
        
    return CommaColumn( VariableLengthStringVector( data, indices ), 1:n-1 )
end

function Base.write( filename::AbstractString, data::CommaColumn{StringType};
                     append::Bool = false, buffersize=2^20 )
    base = filename

    write_buffered( base, data.v.data, append=append, buffersize=buffersize )
    write_buffered( base * "_indices", data.v.indices, append=append, buffersize=buffersize )
end

align( ::StringType ) = rpad

push!( suffixes, r"_indices$" )

