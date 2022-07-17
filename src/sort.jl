
struct CountingSortAlg <: Base.Algorithm end

function boundedcountmap(
    v::AbstractVector{T},
    lo::Int = firstindex(v),
    hi::Int = lastindex(v);
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

# this is really sortperm!
function Base.sort!(
    startperm::AbstractVector{Int},
    lo::Int,
    hi::Int,
    ::CountingSortAlg,
    o::Base.Perm{T,U},
) where {T <: Base.Ordering, U <: AbstractVector}
    # note that there is a special sorting algorithm for floats which conflicts with the above
    v = o.data
    (nottoobig,cm) = boundedcountmap( v, lo, hi );
    if !nottoobig
        return sortperm!( startperm, lo, hi, MergeSort, o, initialized=true )
    end
    sks = sort(collect(keys(cm)))
    indices = Dict( zip( sks, [1;1 .+ cumsum( getindex.( [cm], sks ) )[1:end-1]] ) );

    n = length(v);
    perm = zeros( Int, n )
    for i = lo:hi
        x = v[startperm[i]]
        perm[indices[x]] = startperm[i]
        indices[x] += 1
    end
    for i = lo:hi
        startperm[i] = perm[i]
    end
    return startperm
end

Base.sort!(
    startperm::AbstractVector{Int},
    alg::CountingSortAlg,
    o::Base.Perm{T,U},
) where {T <: Base.Ordering, V <: Union{Int,Date}, U <: AbstractVector{V}} =
    sort!( startperm, firstindex(startperm), lastindex(startperm), alg, o )
    
