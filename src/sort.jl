
struct CountingSortAlg <: Base.Algorithm end

function boundedcountmap(
    v::AbstractVector{T},
    lo::Integer,
    hi::Integer;
    bound::Integer = 2 ^ 20,
) where {T}
    map = Dict{T,Int}()
    for i = lo:hi
        map[v[i]] = get( map, v[i], 0 ) + 1
        if length(map) > bound
            return (false,map)
        end
    end
    return (true,map)
end

function Base.sort!(
    startperm::AbstractVector{T},
    v::AbstractVector,
    lo::Integer,
    hi::Integer,
    a::CountingSortAlg,
    o::Base.Ordering,
) where {T <: Integer}
    (toobig,cm) = boundedcountmap( v );
    if toobig
        return sortperm( v, lo, hi, MergeSort, o )
    end
    sks = sort(collect(keys(cm)), order=o);
    indices = Dict( zip( sks, [1;1 .+ cumsum( getindex.( [cm], sks ) )[1:end-1]] ) );

    n = length(v);
    perm = zeros( T, n )
    for i = 1:n
        x = v[startperm[i]]
        perm[indices[x]] = startperm[i]
        indices[x] += 1
    end
    for i = 1:n
        startperm[i] = perm[i]
    end
    return startperm
end

    
