export innerjoin, outerjoin

function innerjoinindices( vs1, vs2; printevery=Inf )
    indices = [Int[], Int[]]
    currindex = [1,1]
    currvalues = [getindex.(vs1, currindex[1]), getindex.(vs2, currindex[2])]
    
    n1 = length(vs1[1])
    n2 = length(vs2[1])
    while currindex[1] <= n1 && currindex[2] <= n2
        c = cmp( currvalues[1], currvalues[2] )
        if c == 0
            push!( indices[1], currindex[1] )
            push!( indices[2], currindex[2] )
        end
        if c <= 0
            currindex[1] += 1
            if currindex[1] <= n1
                currvalues[1] = getindex.( vs1, currindex[1] )
            end
            if currindex[1] % printevery == 0
                println( "Done $(currindex[1]) of first vectors at $(now())" )
            end
        end
        if c >= 0
            currindex[2] += 1
            if currindex[2] <= n2
                currvalues[2] = getindex.( vs2, currindex[2] )
            end
            if currindex[2] % printevery == 0
                println( "Done $(currindex[2]) of second vectors at $(now())" )
            end
        end
    end
    
    return indices
end

function innerjoin(
    comma1::Comma{S1,T1,U1,V1,W1},
    cs1::Dict{Symbol,Symbol},
    comma2::Comma{S2,T2,U2,V2,W2},
    cs2::Dict{Symbol,Symbol};
    printevery = Inf,
) where {S1, T1, U1, V1, W1, S2, T2, U2, V2, W2}
    vs1 = getindex.( (comma1,), S1 )
    vs2 = getindex.( (comma2,), S2 )
    indices = innerjoinindices( vs1, vs2, printevery=printevery )

    nt1 = NamedTuple{(values(cs1)...,)}( getindex.( getindex.( (comma1,), keys(cs1) ), (indices[1],) ) )
    nt2 = NamedTuple{(values(cs2)...,)}( getindex.( getindex.( (comma2,), keys(cs2) ), (indices[2],) ) )
    return Comma( merge( nt1, nt2 ) )
end

innerjoin( comma1::Comma{S1,T1,U1,V1,W1}, comma2::Comma{S2,T2,U2,V2,W2}; kwargs... ) where {S1, T1, U1, V1, W1, S2, T2, U2, V2, W2} =
    innerjoin( comma1, Dict( (k => k for k in keys(comma1)) ), comma2, Dict( (k => k for k in setdiff(keys(comma2), S2)) ); kwargs... )

typedefault( _ ) = nothing
typedefault( ::Type{Float64} ) = NaN

function outerjoin(
    comma1::Comma{S1,T1,U1,V1,W1},
    cs1::Dict{Symbol,Symbol},
    comma2::Comma{S2,T2,U2,V2,W2},
    cs2::Dict{Symbol,Symbol};
    defaults1 = Dict{Symbol,Any}(),
    defaults2 = Dict{Symbol,Any}(),
    printevery = Inf,
) where {S1, T1, U1, V1, W1, S2, T2, U2, V2, W2}
    currindex = [1,1]
    currvalues = [comma1[currindex[1], S1], comma2[currindex[2], S2]]
    
    ns = [length(comma1[S1[1]]), length(comma2[S2[1]])]

    commas = [comma1, comma2]
    Ss = [S1,S2]

    ks = [ keys( cs1 ), keys( cs2 ) ]
    types = [
        eltype.( getindex.( (comma1,), ks[1] ) ),
        eltype.( getindex.( (comma2,), ks[2] ) ),
    ]
    results = [ getindex.( types[1] ), getindex.( types[2] ) ]
    defaults = [
        get.( (defaults1,), ks[1], typedefault.( types[1] ) ),
        get.( (defaults2,), ks[2], typedefault.( types[2] ) ),
    ]

    while currindex[1] <= ns[1] && currindex[2] <= ns[2]
        c = cmp( currvalues[1], currvalues[2] )
        cs = [c <= 0, c >= 0]
        for i = 1:2
            if cs[i]
                push!.( results[i], getindex.( (commas[i],), currindex[i], ks[i] ) )
                currindex[i] += 1
                if currindex[i] <= ns[i]
                    currvalues[i] = commas[i][currindex[i],Ss[i]]
                end
                if currindex[i] % printevery == 0
                    println( "Done $(currindex[1]) of $(i)th vectors at $(now())" )
                end
            else
                push!.( results[i], defaults[i] )
            end
        end
    end
    vs = [ values( cs1 ), values( cs2 ) ]
    nts = []
    for i = 1:2
        while currindex[i] <= ns[i]
            push!.( results[i], getindex.( (commas[i],), currindex[i], ks[i] ) )
            push!.( results[3-i], defaults[3-i] )
            currindex[i] += 1
        end
    end
    for i = 1:2
        push!( nts, NamedTuple{(vs[i]...,)}( results[i] ) )
    end
    
    return Comma( merge( nts... ) )
end    

outerjoin( comma1::Comma{S1,T1,U1,V1,W1}, comma2::Comma{S2,T2,U2,V2,W2}; kwargs... ) where {S1, T1, U1, V1, W1, S2, T2, U2, V2, W2} =
    outerjoin( comma1, Dict( (k => k for k in keys(comma1)) ), comma2, Dict( (k => k for k in setdiff(keys(comma2), S2)) ); kwargs... )
