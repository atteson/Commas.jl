using Commas
using Random

Random.seed!(1)
n = 1_000_000
m = 20
names = Symbol.(Char.(UInt8('a') .+ (0:m-1)))
types = rand([Int, UInt8, Float64], m)
vals = rand.( types, n )
nt = NamedTuple{(names...,)}(vals)

comma = Commas.Comma( nt )

comma[:a]

comma[[:a,:b]]

subcomma = comma[rand(1:n,Int(round(sqrt(n)))),:]

subcomma[:a]
@assert( findall(subcomma[:a][1] .== comma[:a]) == [subcomma.indices[1]] )

smallnames = names[findall(types .== UInt8)]
result = sort( comma, smallnames... )

@assert(issorted( result[smallnames[1]] ))

exit()

