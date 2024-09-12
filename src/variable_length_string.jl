
using StringViews

struct VariableLengthStringVector <: AbstractVector{StringViews.StringView{SubArray{UInt8, 1, Vector{UInt8}, Tuple{UnitRange{Int64}}, true}}}
    data::Vector{UInt8}
    indices::Vector{Int}
end

function VariableLengthStringVector( vs::Vector{String} )
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

Base.getindex( s::VariableLengthStringVector, i::Int ) = StringView( view( s.data, s.indices[i]+1:s.indices[i+1] ) )

Base.size( s::VariableLengthStringVector ) = (length(s.indices)-1,)

Base.IndexStyle( ::Type{VariableLengthStringVector} ) = IndexLinear()

function Base.read( filename::String, ::Type{CommaColumn{SubString{String}}} )
    data = Mmap.mmap( filename, String, stat(filename) )

    indices_file = filename * "_indices"
    n = stat(indices_file)
    indices = Mmap( indices_file, Vector{Int}, n )
        
    return CommaColumn( VariableLengthStringVector( data, indices ), 1:n-1 )
end

function Base.write( filename::AbstractString, data::CommaColumn{SubString{String},U,V};
                     append::Bool = false, buffersize=2^20 ) where {U,V}
    base = filename * "_$T"
    data = open( base, write=true, append=append )
    indices = open( base * "_indices", write=true, append=append )

    close( data )
    close( indices )
end
