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

j2 = forwardjoin( c1, c2 )
size(j2,1)
j2 = forwardjoin( sort( c1[1:52], :a, :b ), sort( c2[1:100], :a, :b ) )
size(j2,1)

comma1 = c1
comma2 = c2
cs1 = Dict([k => k for k in keys(c1)])
cs2 = Dict(:d => :d)
sortcols(::Comma{S}) where {S} = S
S1 = sortcols(c1)
S2 = sortcols(c2)
