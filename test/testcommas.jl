using Commas

n = 1_000_000
data = NamedTuple{(:x,:y)}( [rand(1:10,n), randn(n)] )

data[data[:x].>0,[:x]]

