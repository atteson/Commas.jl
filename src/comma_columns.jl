
struct CommaColumn{T,U<:AbstractVector{T},V<:AbstractVector{Int}} <: AbstractVector{T}
    v::U
    indices::V
end

CommaColumn( v::AbstractVector{V} ) where {V} = CommaColumn( v, 1:length(v) )

CommaColumn( v::AbstractVector{Union{Missing,T}}, indices::AbstractVector{Int} = 1:length(v) ) where {T} =
    CommaColumn( convert( Vector{MissingType{T}}, v ), indices )

demissing( v::AbstractVector{Union{Missing,T}} ) where {T <: AbstractString} = ifelse.( ismissing.(v), "", v )

CommaColumn( v::AbstractVector{V},
             indices::AbstractVector{Int} = 1:length(v) ) where {T <: AbstractString, V <: Union{Missing,T}} =
                 CommaColumn( demissing( v ), indices )

function CommaColumn( v::AbstractVector{T}, indices::AbstractVector{Int} = 1:length(v);
                      factor::Float64 = 3 ) where {T <: AbstractString}
    N = length(v)
    lengths = length.(v)
    l = maximum(lengths)
    if l > 0
        if mean(lengths)*factor > l
            vu = fill( UInt8(' '), N*l )

            j = 1
            for i = 1:N
                copyto!( vu, j, v[i] )
                j += l
            end
            vc = reinterpret( CharN{l}, vu )
        else
            vc = VariableLengthStringVector( vu )
        end
    else
        vc = Vector{CharN{0}}( undef, N )
    end

    return CommaColumn( vc, indices )
end

Base.length( col::CommaColumn ) = length(col.indices)
Base.size( col::CommaColumn ) = (length(col),)
Base.getindex( col::CommaColumn, i::Int ) = col.v[col.indices[i]]

Base.getindex( col::CommaColumn{T,U,V}, i::Int ) where {T,U,V} = col.v[col.indices[i]]

Base.length( col::CommaColumn{T,U,V} ) where {T,U,V} = length(col.indices)

Base.size( col::CommaColumn{T,U,V} ) where {T,U,V} = (length(col),)

function Base.setindex!( col::CommaColumn, v, i::Int )
    col.v[col.indices[i]] = v
end

Base.write( io::IO, v::Base.ReinterpretArray{T,U,V,W} ) where {T,U,V,W} =
    write( io, reinterpret( V, v ) )

function write_buffered( io::IOStream, data::AbstractVector{T};
                         n = length(data), indices::AbstractVector{Int} = 1:n, append::Bool = false,
                         buffersize=2^20 ) where {T}
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
    return nb
end

function Base.write( filename::AbstractString, data::CommaColumn{T,U,V};
                     append::Bool = false, buffersize=2^20 ) where {T,U,V}
    io = open(  filename, append ? "a" : "w" )
    write_buffered( io, data.v, indices=data.indices, append=append, buffersize=buffersize )
    close( io )
end

function Base.read( filename::String, ::Type{CommaColumn{T}} ) where {T}
    filesize = stat( filename ).size
    n = sizeof(T) == 0 ? 0 : Int(filesize/sizeof(T))
        
    return CommaColumn( Mmap.mmap( filename, Vector{T}, n ), 1:n )
end

materialize( col::CommaColumn{T,Vector{T},UnitRange{Int}} ) where {T} = col

function materialize( col::CommaColumn{T,U,V} ) where {T,U <: CommaColumn,V}
    m = materialize( col.v )
    return CommaColumn( m.v[col.indices], 1:length(col.indices) )
end

materialize( col::AbstractVector ) = CommaColumn( col.v[col.indices], 1:length(col.indices) ) 
