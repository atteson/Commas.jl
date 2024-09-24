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

comma2 = nothing
rm(dirname, recursive=true)

r = 0x41:0x5a
x0 = [tuple(rand( r, 5 )...) for i in 1:100]
x1 = [tuple(rand( r, 3 )...) for i in 1:10]
y0 = [tuple(rand( r, 3 )...) for i in 1:100]
y1 = [tuple(rand( r, 5 )...) for i in 1:10]
comma = Comma( (x=x0, y=y0) )

dirname = tempname()
write( dirname, comma )
comma2 = read( dirname, Comma )

@assert( comma[:x] == comma2[:x] )
@assert( comma[:y] == comma2[:y] )
comma2 = nothing

comma3 = Comma( (x=x1, y=y1) )
write( dirname, comma3, append=true )

comma4 = read( dirname, Comma )
@assert( comma4[:x][1:100]  == x0 )
@assert( getindex.( comma4[:x][101:end], [1:3] ) == x1 )
@assert( getindex.( comma4[:y][1:100], [1:3] ) == y0 )
@assert( comma4[:y][101:end] == y1 )

rm(dirname, recursive=true)

exit()
