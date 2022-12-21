export innerjoin, forwardjoin

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

function forwardjoin(
    comma1::Comma{S1,T1,U1,V1,W1},
    cs1::Dict{Symbol,Symbol},
    comma2::Comma{S2,T2,U2,V2,W2},
    cs2::Dict{Symbol,Symbol};
    printevery = Inf,
    has1 = Nothing,
    has2 = Nothing,
) where {S1, T1, U1, V1, W1, S2, T2, U2, V2, W2}
    currindex = [1,1]
    currvalues = [comma1[currindex[1], S1], comma2[currindex[2], S2]]
    record = false
    prevc = cmp( currvalues[1], currvalues[2] )
    
    ns = [length(comma1[S1[1]]), length(comma2[S2[1]])]

    vs1 = get.( (cs1,), S1, : )
    vs2 = get.( (cs2,), S2, : )
    @assert( all( (ks1 .== :) .| (ks2 .== :) ) )
    # priority to S1 unless explicitly provided
    vs = [vs1[i] != : ? vs1[i] : ( vs2[i] != : ? vs2[i] : S1[i] ) for i in 1:length(vs1)]

    ks = [setdiff(keys(cs1), S1), setdiff(keys(cs2), S2)]

    commas = [comma1, comma2]
    Ss = [S1,S2]

    result0 = getindex.( eltype.( getindex.( (comma1,), S1 ) ) )
    result1 = getindex.( eltype.( getindex.( (comma1,), ks1 ) ) )
    result2 = getindex.( eltype.( getindex.( (comma2,), ks2 ) ) )
    has = [Bool[], Bool[]]
    while currindex[1] <= ns[1] && currindex[2] <= ns[2]
        c = cmp( currvalues[1], currvalues[2] )
        if c == 0 || c != prevc
            record = true
        end
        if record
            i = (c > 0) + 1
            push!.( result0, getindex.( (commas[i],), currindex[i], ks[i] ) )
            push!.( result1, getindex.( (commas[1],), (currindex[1] - (c > 0),), ks[1] ) )
            push!.( result2, getindex.( (commas[2],), (currindex[2] - (c < 0),), ks[2] ) )
            push!( has[1], c <= 0 )
            push!( has[2], c >= 0] )
        end
        if c <= 0 && currindex[1] <= ns[1]
            currindex[1] += 1
            if currindex[1] <= ns[1]
                currvalues[1] = comma1[currindex[1],S1]
            end
            if currindex[1] % printevery == 0
                println( "Done $(currindex[1]) of first vectors at $(now())" )
            end
        end
        if c >= 0 && currindex[2] <= ns[2]
            currindex[2] += 1
            if currindex[2] <= ns[2]
                currvalues[2] = comma2[currindex[2],S2]
            end
            if currindex[2] % printevery == 0
                println( "Done $(currindex[2]) of second vectors at $(now())" )
            end
        end
        prevc = c
    end
    while currindex[1] <= ns[1]
        push!.( result0, getindex.( (commas[1],), currindex[1], ks[1] ) )
        push!.( result1, getindex.( (comma1,), currindex[1], ks[1] ) )
        push!.( result2, getindex.( (comma2,), currindex[2] - 1, ks[2] ) )
        push!.( has[1], true )
        push!.( has[2], false )
        currindex[1] += 1
    end
    while currindex[2] <= ns[2]
        push!.( result0, getindex.( (commas[2],), currindex[2], ks[2] ) )
        push!.( result1, getindex.( (comma1,), (currindex[1] - 1,), keys(cs1) ) )
        push!.( result2, getindex.( (comma2,), (currindex[2],), keys(cs2) ) )
        push!.( has[1], false )
        push!.( has[2], true )
        currindex[2] += 1
    end
    
    nt1 = NamedTuple{(values(cs1)...,)}( result1 )
    nt2 = NamedTuple{(values(cs2)...,)}( result2 )
    return [Comma( merge( nt1, nt2 ) ); has1 => has[1], has2 => has[2]]
end    

forwardjoin( comma1::Comma{S1,T1,U1,V1,W1}, comma2::Comma{S2,T2,U2,V2,W2}; kwargs... ) where {S1, T1, U1, V1, W1, S2, T2, U2, V2, W2} =
    forwardjoin( comma1, Dict( (k => k for k in keys(comma1)) ), comma2, Dict( (k => k for k in setdiff(keys(comma2), S2)) ), kwargs... )
