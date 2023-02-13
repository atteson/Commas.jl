using Commas
using Random

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

j2 = outerjoin( c1, c2, defaults1 = Dict(:a=>0, :b=>0, :c=>0), defaults2 = Dict(:a=>0, :b=>0, :d=>0) )
size(j2,1)

c1r = sort(c1[rand( 1:size(c1,1), 2 )], :a, :b )
c2r = sort(c2[rand( 1:size(c2,1), 4 )], :a, :b )
j3 = outerjoin( c1r, c2r, defaults1 = Dict(:a=>0, :b=>0, :c=>0), defaults2 = Dict(:a=>0, :b=>0, :d=>0) )

c1r = sort(c1[rand( 1:size(c1,1), 2 )], :a, :b )
c2r = sort(c2[rand( 1:size(c2,1), 4 )], :a, :b )
j3 = outerjoin(
    c1r, Dict(:a => :a1, :b => :b1, :c => :c),
    c2r, Dict(:a => :a2, :b => :b2, :d => :d),
    defaults1 = Dict(:a=>0, :b=>0, :c=>0), defaults2 = Dict(:a=>0, :b=>0, :d=>0)
)


n = 1_000
m = 100
v1 = sort(rand( 1:100, n ))
v2 = sort(rand( 2:99, m ))

function outerjoinindices!( v1, ilo, ihi, v2; d = 1 )
    (n1, n2) = length.((v1, v2))
    f = (3 - d) >> 1
    r = (d + 3) >> 1
    (start1, stop1) = (1, n1)[[f,r]]
    (start2, stop2) = (1, n2)[[f,r]]
    for j = start1:d:stop1
        i = ilo[j]
        if d*j > d*start1
            i = d*max( d*i, d*ilo[j-d] )
        end
        while d*i <= d*ihi[j] && d*v1[j] > d*v2[i]
            i += d
        end
        ilo[j] = i
    end
    return ilo
end
ilo = ones(Int, n);
ihi = fill(m, n);
@time outerjoinindices!( v1, ilo, ihi, v2 );

function testoji( v1, i1, v2 )
    good = i1 .<= length(v2)
    @assert all(v1[good] .<= v2[i1[good]])
    good .&= i1 .> 1
    @assert all(v1[good] .> v2[i1[good].-1])
end

testoji( v1, ilo, v2 )

@time outerjoinindices!( v1, ihi, ilo, v2, d=-1 );
testoji( -reverse(v1), m .- reverse(ihi) .+ 1, -reverse(v2) )

n = 1_000_000
m = 100
v1a = sort(rand( 1:10, n ))
v2a = sort(rand( 0:9, m ))
v1b = rand( 1:10, n )
v2b = rand( 2:11, m )

s1 = 1:n
@time s1b = Commas.countingsortperm( s1, v1b );
@time s1a = Commas.countingsortperm( s1b, v1a );

[v1a[s1a] v1b[s1a]]
unique(diff(v1a[s1a]))
unique(diff(v1b[s1a]))

s2 = 1:m
s2b = Commas.countingsortperm( s2, v2b )
s2a = Commas.countingsortperm( s2b, v2a )

i1 = ones(Int, n)
@time outerjoinindices!( v1a[s1a], i1, v2a[s2a] );
testoji( v1a[s1a], i1, v2a[s2a] )
@time outerjoinindices!( v1b[s1a], i1, v2b[s2a] );
testoji( v1a[s1a], i1, v2a[s2a] )

v1 = v1a[s1a]
v2 = v2a[s2a]

good = i1 .<= length(v2a)
@assert all(v1[good] .<= v2[i1[good]])

