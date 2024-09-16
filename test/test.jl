using Commas
using Random
using DataFrames

Random.seed!(1)
n = 1_000_000
n = 3
m = 20
ks = Symbol.(Char.(UInt8('a') .+ (0:m-1)))
types = rand([Int, UInt8, Float64], m)
vs = rand.( types, n );
nt = NamedTuple{(ks...,)}(vs);

comma = Comma( nt )
n1 = 1_000
subcomma = Comma( comma, rand( 1:n, n1 ) )

filename = tempname()
write( filename, comma[:a] )
type = eltype(comma[:a])
col = read( filename, CommaColumn{type} )
@assert( col == comma[:a] )

dirname = tempname()
write( dirname, comma )
comma2 = read( dirname, Comma )

@assert( names(comma) == names(comma2) )
for name in names(comma)
    @assert( comma[name] == comma2[name] )
end
@assert( comma.indices == comma2.indices )

@assert( size(comma) == (n,m) )
@assert( size(comma,1) == n )
@assert( size(subcomma) == (n1,m) )
@assert( size(subcomma,1) == n1 )

@assert( keys(comma) == (ks...,) )
@assert( keys(subcomma) == (ks...,) )
@assert( all(values( comma ) .== vs) )
@assert( all(values( subcomma ) .== getindex.(vs, [subcomma.indices])) )

df = DataFrame( comma )
comma2 = Comma( df )
@assert( names(comma) == names(comma2) )
@assert( all([comma[name] == comma2[name] for name in names(comma)]) )
@assert( comma.indices == comma2.indices )

@assert( size(comma[[:a,:b]]) == (n,2) )
@assert( size(subcomma[[:a,:b]]) == (n1,2) )

@assert( comma[2,:a] == vs[1][2] )
@assert( subcomma[3,:b] == vs[2][subcomma.indices[3]] )
@assert( comma[3,"c"] == vs[3][3] )

exit()
