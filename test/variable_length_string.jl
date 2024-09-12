using Commas
using Random
using Distributions

lendist = Geometric(0.01)

dir = mktemp()
v = [randstring( rand( lendist ) ) for i = 1:1_000_000]
c = Commas.VariableLengthStringVector( v )

size( c )
for i = 1:length(c)
    @assert( c[i] == v[i], "The $(i)th elements don't match" )
end

