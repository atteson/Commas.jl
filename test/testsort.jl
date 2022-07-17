using Commas
using Random

test( v, p, op ) = (u -> op( u[1:end-1], u[2:end] ))( v[p] )

n = 1_000_000
Random.seed!(1)


v2 = rand( 1:10, n );
perm = collect(1:n);
@time sortperm!( perm, v2, alg=Commas.CountingSortAlg() );
@assert( issorted( v2[perm] ) )

v1 = rand( 11:100, n );
@time sortperm!( perm, v1, alg=Commas.CountingSortAlg(), initialized=true );
@assert( all(test( v1, perm, .< ) .| (test( v1, perm, .== ) .& test( v2, perm, .<= ))) )


v2 = rand( n );
perm = collect(1:n);
@time sortperm!( perm, v2, alg=Commas.CountingSortAlg() );
@assert( issorted( v2[perm] ) )

v1 = rand( 1:100, n );
@time sortperm!( perm, v1, alg=Commas.CountingSortAlg(), initialized=true );
@assert( all(test( v1, perm, .< ) .| (test( v1, perm, .== ) .& test( v2, perm, .<= ))) )


v2 = rand( 1:100, n );
perm = collect(1:n);
@time sortperm!( perm, v2, alg=Commas.CountingSortAlg() );
@assert( issorted( v2[perm] ) )

v1 = rand( n );
@time sortperm!( perm, v1, alg=Commas.CountingSortAlg(), initialized=true );
@assert( all(test( v1, perm, .< ) .| (test( v1, perm, .== ) .& test( v2, perm, .<= ))) )


n = 10_000_000
v2 = rand( 1:n, n );
perm = collect(1:n);
@time sortperm!( perm, v2, alg=Commas.CountingSortAlg() );
@assert( issorted( v2[perm] ) )

v1 = rand( 1:100, n );
@time sortperm!( perm, v1, alg=Commas.CountingSortAlg(), initialized=true );
@assert( all(test( v1, perm, .< ) .| (test( v1, perm, .== ) .& test( v2, perm, .<= ))) )


v2 = rand( 1:100, n );
perm = collect(1:n);
@time sortperm!( perm, v2, alg=Commas.CountingSortAlg() );
@assert( issorted( v2[perm] ) )

v1 = rand( 1:n, n );
@time sortperm!( perm, v1, alg=Commas.CountingSortAlg(), initialized=true );
@assert( all(test( v1, perm, .< ) .| (test( v1, perm, .== ) .& test( v2, perm, .<= ))) )

