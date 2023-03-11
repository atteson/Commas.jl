using Commas
using Random
using GCTools

Random.seed!(1)

n1 = 10_000
c1 = Comma( (a=rand( 1:10, n1 ), b = rand( 2:20, n1 ), c = rand( 3:30, n1 )) )
c1 = sort( c1, :a, :b )

n2 = 100_000
c2 = Comma( (a=rand( 1:10, n2 ), b = rand( 2:20, n2 ), d = rand( 4:40, n2 )) )
c2 = sort( c2, :a, :b )

j1 = innerjoin( c1, c2 )
for c in keys(c1)
    @assert( j1[c] == c1[c] )
end

for a in unique(c1[:a])
    for b in unique(c1[:b])
        c1a = searchsorted( c1[:a], a )
        c1b = searchsorted( c1[c1a,:b], b )
        c2a = searchsorted( c2[:a], a )
        c2b = searchsorted( c2[c2a,:b], b )
        jb =  1:min(length(c1b), length(c2b))
        @assert( j1[c1a,:d][c1b] == c2[c2a,:d][c2b][jb] )
    end
end

function outerjoinindices!( v, lo, hi )
    n = length.(v)

    # allocate now so we don't need to allocate in loop
    (i, first, last) = ([1,1], [1,1], [1,1])
    while i[1] <= n[1] && i[2] <= n[2]
        equals = true
        for j = 1:2
            if hi[j][i[j]] < i[3-j] || (lo[j][i[j]] <= i[3-j] && v[j][i[j]] < v[3-j][i[3-j]])
                equals = false
                lo[j][i[j]] = i[3-j]
                hi[j][i[j]] = i[3-j] - 1
                i[j] += 1
                break
            end
        end
        if equals
            for j = 1:2
                first[j] = last[j] = i[j]
            end
            for j = 1:2
                i[j] += 1
                while i[j] <= hi[3-j][first[3-j]] && v[j][i[j]] == v[j][i[j]-1]
                    last[j] = i[j]
                    i[j] += 1
                end
            end
            for j = 1:2
                for k = first[j]:last[j]
                    lo[j][k] = first[3-j]
                    hi[j][k] = last[3-j]
                end
            end
        end
    end
    for j=1:2
        while i[j] <= n[j]
            lo[j][i[j]] = n[3-j] + 1
            hi[j][i[j]] = n[3-j]
            i[j] += 1
        end
    end
end

function testoji( v, lo, hi, lo1, hi1)
    for j = 1:2
        vs = getindex.( [v[3-j]], range.( lo1[j], hi1[j] ) )
        rs = searchsorted.( vs, v[j] )
        @assert( lo[j] == lo1[j] .- 1 .+ getfield.( rs, :start ) )
        @assert( hi[j] == lo1[j] .- 1 .+ getfield.( rs, :stop ) )
    end
end

function testoji( v, lo, hi )
    n = length.(v)
    testoji( v, lo, hi, ones.(Int, n), fill.(reverse(n), n) )
end

v = [[1, 1, 2, 3, 5], [1, 3, 7]]
n = length.(v)
lo = ones.(Int,n)
hi = fill.(reverse(n),n)

outerjoinindices!( v, lo, hi )
testoji( v, lo, hi )

n = [1_000_000, 100]
v = sort.(rand.( range.(1,n), n ))
lo = fill.(1, n)
hi = fill.(reverse(n), n)

@time outerjoinindices!( v, lo, hi );
@time testoji( v, lo, hi )

v = sort.(rand.( [range.(1,n[1])], n ))
lo = fill.(1, n)
hi = fill.(reverse(n), n)

@time outerjoinindices!( v, lo, hi );
@time testoji( v, lo, hi )

v = sort.(rand.( [range.(1,n[2])], n ))
lo = fill.(1, n)
hi = fill.(reverse(n), n)

@time outerjoinindices!( v, lo, hi );
@time testoji( v, lo, hi )

v1 = [[1,1,1,1,2,4,5,5,5], [1,1,1,3,4,4,4,5]]
v2 = [[1,1,2,3,1,2,1,2,3], [1,1,3,2,1,2,3,2]]
n = length.(v1)
@assert( length.(v2) == n )
lo = fill.(1, n)
hi = fill.(reverse(n), n)

outerjoinindices!( v1, lo, hi )
testoji( v1, lo, hi )

lo1 = deepcopy(lo)
hi1 = deepcopy(hi)

outerjoinindices!( v2, lo, hi )
testoji( v2, lo, hi, lo1, hi1 )
