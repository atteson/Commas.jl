
comma1 = data
comma2 = prices
S1 = typeof(comma1).parameters[1]
S2 = typeof(comma2).parameters[1]

data3 = Commas.outerjoin( comma1, Dict( (k => k for k in keys(comma1)) ), comma2, Dict( (k => k for k in setdiff(keys(comma2), S2)) ) )
cs1 = Dict( (k => k for k in keys(comma1)) )
cs2 = Dict( (k => k for k in setdiff(keys(comma2), S2)) )
cs2 = Dict( (k => k for k in keys(comma2)) )

defaults1 = Dict{Symbol,Any}()
defaults2 = Dict{Symbol,Any}()
printevery = Inf
stopat = Inf
fillforward = false

import Commas: materialize, align!, find_indices, invert_indices

typedefault( x ) = error( "No default for type $x" )
typedefault( ::Type{Float64} ) = NaN
typedefault( ::Type{Int64} ) = typemin(Int64)
typedefault( ::Type{Date} ) = Date( 0 )
typedefault( ::Type{Bool} ) = false
typedefault( ::Type{CharN{N}} ) where N = convert( CharN{N}, "" )
typedefault( ::Type{MissingType{T}} ) where T = convert( MissingType{T}, missing )
