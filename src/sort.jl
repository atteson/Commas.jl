
struct CountingSortAlg <: Base.Algorithm end

function boundedcountmap(
    v::AbstractVector{T};
    bound::Integer = 2 ^ 20,
) where {T}
    map = Dict{T,Int}()
    for i = 1:length(v)
        map[v[i]] = get( map, v[i], 0 ) + 1
        if length(map) > bound
            return (false,map)
        end
    end
    return (true,map)
end

function Base.sort!(
    startperm::AbstractVector{T},
    ::CountingSortAlg,
    o::Base.Perm,
) where {T <: Integer}
    v = o.data
    (nottoobig,cm) = boundedcountmap( v );
    if !nottoobig
        return sortperm( startperm, MergeSort, o )
    end
    sks = sort(collect(keys(cm)))
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

    
