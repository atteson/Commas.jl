using Commas
using Random

n = 1_000_000
Random.seed!(1)
v = rand( 1:10, n );
perm = collect(1:n);
@time sortperm!( perm, v, alg=Commas.CountingSortAlg() );

issorted( v[perm] )
