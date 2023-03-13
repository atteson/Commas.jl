using Commas
using Random
using GCTools
using Commas

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

Commas.outerjoinindices!( v, lo, hi )
testoji( v, lo, hi )

n = [1_000_000, 100]
v = sort.(rand.( range.(1,n), n ))
lo = fill.(1, n)
hi = fill.(reverse(n), n)

@time Commas.outerjoinindices!( v, lo, hi );
@time testoji( v, lo, hi )

v = sort.(rand.( [range.(1,n[1])], n ))
lo = fill.(1, n)
hi = fill.(reverse(n), n)

@time Commas.outerjoinindices!( v, lo, hi );
@time testoji( v, lo, hi )

v = sort.(rand.( [range.(1,n[2])], n ))
lo = fill.(1, n)
hi = fill.(reverse(n), n)

@time Commas.outerjoinindices!( v, lo, hi );
@time testoji( v, lo, hi )

v1 = [[1,1,1,1,2,4,5,5,5], [1,1,1,3,4,4,4,5]]
v2 = [[1,1,2,3,1,2,1,2,3], [1,1,3,2,1,2,3,2]]
n = length.(v1)
@assert( length.(v2) == n )
lo = fill.(1, n)
hi = fill.(reverse(n), n)

Commas.outerjoinindices!( v1, lo, hi )
testoji( v1, lo, hi )

lo1 = deepcopy(lo)
hi1 = deepcopy(hi)

Commas.outerjoinindices!( v2, lo, hi )
testoji( v2, lo, hi, lo1, hi1 )

n = [1_000_000, 1_000]
v3 = rand.( [range.(1,n[1])], n );
perms = Commas.countingsortperm.( range.(1, n), v3, UInt32 )
v2 = rand.( [range.(1,n[1])], n );
perms = Commas.countingsortperm.( perms, v2, UInt32 )
v1 = rand.( [range.(1,n[1])], n );
perms = Commas.countingsortperm.( perms, v1, UInt32 )
lo = fill.(1, n)
hi = fill.(reverse(n), n)

lo1 = deepcopy(lo)
hi1 = deepcopy(hi)
v1 = getindex.( v1, perms )
@time Commas.outerjoinindices!( v1, lo1, hi1 )
@time testoji( v1, lo1, hi1 )

lo2 = deepcopy(lo1)
hi2 = deepcopy(hi1)
v2 = getindex.( v2, perms )
@time Commas.outerjoinindices!( v2, lo2, hi2 )
@time testoji( v2, lo2, hi2, lo1, hi1 )

lo3 = deepcopy(lo2)
hi3 = deepcopy(hi2)
v3 = getindex.( v3, perms )
@time Commas.outerjoinindices!( v3, lo3, hi3 )
@time testoji( v3, lo3, hi3, lo2, hi2 )

