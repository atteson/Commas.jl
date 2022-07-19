
function innerjoin(
    comma1::Comma{S1,T1,U1,V1,W1},
    cs1::Dict{Symbol,Symbol},
    comma2::Comma{S2,T2,U2,V2,W2},
    cs2::Dict{Symbol,Symbol},
) where {S1, T1, U1, V1, W1, N, S2, T2, U2, V2, W2}
    vs1 = getindex.( [comma1], S1 )
    vs2 = getindex.( [comma2], S2 )
    
    indices = [Int[], Int[]]
    currindex = [1,1]
    currvalues = [getindex.(vs1, currindex[1]), getindex.(vs2, currindex[2])]
    updates = [false,false]
    
    n1 = size(comma1,1)
    n2 = size(comma2,1)
    while currindex[1] <= n1 && currindex[2] <= n2
        if currvalues[1] == currvalues[2]
            push!( indices[1], currindex[1] )
            push!( indices[2], currindex[2] )
        end
        if currvalues[1] <= currvalues[2]
            currindex[1] += 1
            updates[1] = true
        end
        if currvalues[2] <= currvalues[1]
            currindex[2] += 1
            updates[2] = true
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

