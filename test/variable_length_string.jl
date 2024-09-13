using Commas
using Random
using Distributions
using Dates
using StringViews

lendist = Geometric(0.01)

dir = mktemp()
v = [randstring( rand( lendist ) ) for i = 1:1_000_000]
cc = Commas.VariableLengthStringVector( v )

size( cc )
for i = 1:length(cc)
    @assert( cc[i] == v[i], "The $(i)th elements don't match" )
end

c1 = Comma( (s = cc,) )
tmpdir = mktempdir()
write( tmpdir, c1 )


c2 = read( tmpdir, Comma );
@assert( size(c1) == size(c2) )

cc1 = c1[:s];
cc2 = c2[:s];
t0 = now()
for i = 1:size(c1,1)
    t1 = now()
    if t1 > t0 + Second(5)
        println( "Processing $i at $t1" )
        t0 = t1
    end
    @assert( cc1[i] == cc2[i] )
end

rm(tmpdir, recursive=true)
