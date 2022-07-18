
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

export countingsortperm!

# this is really sortperm!
# Base sort is not designed in a way to easily override sortperm!
function countingsortperm!(
    startperm::AbstractVector{Int},
    v::AbstractVector;
    initialized::Bool = false,
)
    # note that there is a special sorting algorithm for floats which conflicts with the above
    (nottoobig,cm) = boundedcountmap( v );
    if !nottoobig
        return sortperm!( startperm, v, alg=MergeSort, initialized=initialized )
    end
    sks = sort(collect(keys(cm)))
    indices = Dict( zip( sks, [1;1 .+ cumsum( getindex.( [cm], sks ) )[1:end-1]] ) );

    n = length(v);
    if !initialized
        for i = 1:n
            startperm[i] = i
        end
    end
    perm = zeros( Int, n );
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
