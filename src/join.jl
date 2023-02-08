export innerjoin, outerjoin, fillforward!

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

function outerjoinindices( vs1, vs2; printevery=Inf )
    n = [length(vs1[1]), length(vs2[1])]
    indices = fill.( 0, n )

    currindex = [1,1]
    currvalues = [getindex.(vs1, currindex[1]), getindex.(vs2, currindex[2])]
    
    index = 1
    while currindex[1] <= n[1] && currindex[2] <= n[2]
        c = cmp( currvalues... )
        if c <= 0
            indices[1][currindex[1]] = index
            currindex[1] += 1
            if currindex[1] <= n[1]
                currvalues[1] = getindex.( vs1, currindex[1] )
            end
            if currindex[1] % printevery == 0
                println( "Done $(currindex[1]) of first vectors at $(now())" )
            end
        end
        if c >= 0
            indices[2][currindex[2]] = index
            currindex[2] += 1
            if currindex[2] <= n[2]
                currvalues[2] = getindex.( vs2, currindex[2] )
            end
            if currindex[2] % printevery == 0
                println( "Done $(currindex[2]) of second vectors at $(now())" )
            end
        end
        index += 1
    end
    for i = 1:2
        while currindex[i] <= n[i]
            indices[i][currindex[i]] = index
            currindex[i] += 1
            index += 1
        end
    end
    
    return indices
end

typedefault( _ ) = nothing
typedefault( ::Type{Float64} ) = NaN
typedefault( ::Type{Int64} ) = typemin(Int64)
typedefault( ::Type{Date} ) = Date( 0 )
typedefault( ::Type{Bool} ) = false

function outerjoin(
    comma1::Comma{S1,T1,U1,V1,W1},
    cs1::Dict{Symbol,Symbol},
    comma2::Comma{S2,T2,U2,V2,W2},
    cs2::Dict{Symbol,Symbol};
    defaults1 = Dict{Symbol,Any}(),
    defaults2 = Dict{Symbol,Any}(),
    print = false,
    printevery = Inf,
    stopat = Inf,
) where {S1, T1, U1, V1, W1, S2, T2, U2, V2, W2}
    vs1 = getindex.( (comma1,), S1 )
    vs2 = getindex.( (comma2,), S2 )
    
    print && println( "Calculating indices at $(now())" )

    indices = outerjoinindices( vs1, vs2, printevery=printevery )

    ks = collect.([ keys( cs1 ), keys( cs2 ) ])
    types = [
        eltype.( getindex.( (comma1,), ks[1] ) ),
        eltype.( getindex.( (comma2,), ks[2] ) ),
    ]
    defaults = [
        get.( (defaults1,), ks[1], typedefault.( types[1] ) ),
        get.( (defaults2,), ks[2], typedefault.( types[2] ) ),
    ]
    
    print && println( "Allocating results at $(now())" )

    results = [fill.( defaults[i],  max(indices[1][end], indices[2][end])) for i in 1:2]

    commas = [comma1, comma2];
    css = [cs1, cs2]
    for i = 1:2
        ii = indices[i]
        ki = ks[i]
        ri = results[i]
        ci = commas[i]
        for k = 1:length(ki)
            print && println( "Processing column $k or dataset $i at $(now())..." )
            ri[k][ii] = ci[1:length(ii),ki[k]]
        end
    end
    print && println( "Merging columns at $(now())" )
    
    vs = [ values( cs1 ), values( cs2 ) ]
    nts = []
    for i = 1:2
        push!( nts, NamedTuple{(vs[i]...,)}( results[i] ) )
    end
    
    return Comma( merge( nts... ) )
end    

outerjoin( comma1::Comma{S1,T1,U1,V1,W1}, comma2::Comma{S2,T2,U2,V2,W2}; kwargs... ) where {S1, T1, U1, V1, W1, S2, T2, U2, V2, W2} =
    outerjoin( comma1, Dict( (k => k for k in keys(comma1)) ), comma2, Dict( (k => k for k in setdiff(keys(comma2), S2)) ); kwargs... )

function fillforward!(
    comma::Comma{S,T,U,V,W},
    s1::Union{AbstractVector{Symbol},NTuple{N,Symbol}},
    m1::Union{AbstractVector{Symbol},NTuple{N1,Symbol}},
    s2::Union{AbstractVector{Symbol},NTuple{N,Symbol}},
    m2::Union{AbstractVector{Symbol},NTuple{N2,Symbol}},
) where {S,T,U,V,W,N,N1,N2}
    s = [s1, s2]
    m = [[s1;m1], [s2;m2]]
    startc = cmp( getindex.( (comma,), 1, s[1] ), getindex.( (comma,), 1, s[2] ) )
    record = false
    for i = 1:size(comma,1)
        c = cmp( getindex.( (comma,), i, s[1] ), getindex.( (comma,), i, s[2] ) )
        if c == 0 || c != startc
            record = true
        end
        cs = [c <= 0, c >= 0]

        for j = 1:2
            if record && cs[j] && !cs[3-j]
                setindex!.( (comma,), getindex.( (comma,), i-1, m[j] ), i, m[j] )
            end
        end
    end
    return comma
end