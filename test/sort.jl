using Commas
using Random

test( v, p, op ) = (u -> op( u[1:end-1], u[2:end] ))( v[p] )

n = 1_000_000
Random.seed!(1)


v2 = rand( 1:10, n );
perm = collect(1:n);
@time perm = Commas.countingsortperm( perm, v2 );
@assert( issorted( v2[perm] ) )

v1 = rand( 11:100, n );
@time perm = Commas.countingsortperm( perm, v1 );
@assert( all(test( v1, perm, .< ) .| (test( v1, perm, .== ) .& test( v2, perm, .<= ))) )


v2 = rand( n );
perm = collect(1:n);
@time perm = Commas.countingsortperm( perm, v2 );
@time perm = Commas.countingsortperm( perm, v2, UInt32 );

v1 = rand( 1:100, n );
@time perm = Commas.countingsortperm( perm, v1 );
@assert( all(test( v1, perm, .< ) .| (test( v1, perm, .== ) .& test( v2, perm, .<= ))) )


v2 = rand( 1:100, n );
perm = collect(1:n);
@time perm = Commas.countingsortperm( perm, v2 );
@assert( issorted( v2[perm] ) )

v1 = rand( n );
try
    @time perm = Commas.countingsortperm( perm, v1 )
    @assert( false, "Should have run out of spots!" )
catch e
    @assert( true )
end
@time perm = Commas.countingsortperm( perm, v1, UInt32 );
@assert( all(test( v1, perm, .< ) .| (test( v1, perm, .== ) .& test( v2, perm, .<= ))) )


n = 10_000_000
v2 = rand( 1:n, n );
perm = collect(1:n);
try
    @time perm = Commas.countingsortperm( perm, v2 );
    @assert( false, "Should have run out of spots!" )
catch e
    @assert( true )
end
@time perm = Commas.countingsortperm( perm, v2, UInt32 );
@assert( issorted( v2[perm] ) )

v1 = rand( 1:100, n );
@time perm = Commas.countingsortperm( perm, v1 );
@assert( all(test( v1, perm, .< ) .| (test( v1, perm, .== ) .& test( v2, perm, .<= ))) )


v2 = rand( 1:100, n );
perm = collect(1:n);
@time perm = Commas.countingsortperm( perm, v2 );
@assert( issorted( v2[perm] ) )

v1 = rand( 1:n, n );
try
    @time perm = Commas.countingsortperm( perm, v1 );
    @assert( false, "Should have run out of spots!" )
catch e
    @assert( true )
end
@time perm = Commas.countingsortperm( perm, v1, UInt32 );
@assert( all(test( v1, perm, .< ) .| (test( v1, perm, .== ) .& test( v2, perm, .<= ))) )


c = Comma( (x = rand(1:10, n), y = rand(11:100, n)) );
@time s = sort( c, :x, :y );

@assert( all(test( s[:x], 1:n, .< ) .| (test( s[:x], 1:n, .== ) .& test( s[:y], 1:n, .<= ))) )
