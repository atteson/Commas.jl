export innerjoin

function innerjoinindices( vs1, vs2; printevery=Inf )
    indices = [Int[], Int[]]
    currindex = [1,1]
    currvalues = [getindex.(vs1, currindex[1]), getindex.(vs2, currindex[2])]
    updates = [false,false]
    
    n1 = length(vs1[1])
    n2 = length(vs2[1])
    while currindex[1] <= n1 && currindex[2] <= n2
        if currvalues[1] == currvalues[2]
            push!( indices[1], currindex[1] )
            push!( indices[2], currindex[2] )
        end
        if currvalues[1] <= currvalues[2]
            currindex[1] += 1
            updates[1] = true
        end
        if currindex[1] % printevery == 0
            println( "Done $(currindex[1]) of first vectors at $(now())" )
        end
        if currvalues[2] <= currvalues[1]
            currindex[2] += 1
            updates[2] = true
        end
        if currindex[2] % printevery == 0
            println( "Done $(currindex[2]) of second vectors at $(now())" )
        end
        if updates[1] && currindex[1] <= n1
            currvalues[1] = getindex.( vs1, currindex[1] )
            updates[1] = false
        end
        if updates[2] && currindex[2] <= n2
            currvalues[2] = getindex.( vs2, currindex[2] )
            updates[2] = false
        end
    end
    
    return indices
end

function innerjoin(
    comma1::Comma{S1,T1,U1,V1,W1},
    cs1::Dict{Symbol,Symbol},
    comma2::Comma{S2,T2,U2,V2,W2},
    cs2::Dict{Symbol,Symbol},
) where {S1, T1, U1, V1, W1, S2, T2, U2, V2, W2}
    vs1 = getindex.( (comma1,), S1 )
    vs2 = getindex.( (comma2,), S2 )
    indices = innerjoinindices( vs1, vs2 )

    nt1 = NamedTuple{(values(cs1)...,)}( getindex.( getindex.( (comma1,), keys(cs1) ), (indices[1],) ) )
    nt2 = NamedTuple{(values(cs2)...,)}( getindex.( getindex.( (comma2,), keys(cs2) ), (indices[2],) ) )
    return Comma( merge( nt1, nt2 ) )
end
