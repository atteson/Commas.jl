
function reindex(
    v::AbstractVector{T},
    ::Type{U},
) where {T, U <: Unsigned}
    indices = Dict{T,U}()
    curr = 0
    vals = T[]
    counts = Int[]
    n = length(v)
    a = zeros( U, n )
    for i = 1:n
        vi = v[i]
        if !haskey( indices, vi )
            indices[vi] = curr += 1
            push!( vals, vi )
            push!( counts, 1 )
            a[i] = curr
        else
            index = indices[vi]
            a[i] = index
            counts[index] += 1
        end
    end
    return (a,vals,counts)
end

function countingsortperm(
    startperm::AbstractVector{Int},
    v::AbstractVector,
    ::Type{U} = UInt16,
) where {U <: Unsigned}
    (a,vals,counts) = reindex( v, U );
    p = sortperm( vals )
    m = length(counts)
    indices = zeros( Int, m )
    for i = 1:m-1
        indices[p[i+1]] = indices[p[i]] + counts[p[i]]
    end

    n = length(a);
    perm = Array{Int}( undef, n );
    for i = 1:n
        si = startperm[i]
        index = a[si]
        perm[indices[index] += 1] = si
    end
    return perm
end
