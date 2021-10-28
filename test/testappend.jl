using Commas
using DataFrames

dir = joinpath( dirname(dirname(pathof(Commas))), "data", "test" )
mkpath( dir )
rm( dir, recursive=true )

df1 = DataFrame(
    i = 1:10,
    j = (1:10).^2,
)

Commas.writecomma( dir, df1, append=true )

df2 = DataFrame(
    i = 11:20,
    j = (11:20).^2,
)

Commas.writecomma( dir, df2, append=true )

df = Commas.readcomma( dir )
