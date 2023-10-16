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

function align!( v, lo, hi )
    n = length.(v)

    # allocate now so we don't need to allocate in loop
    (i, first, last) = ([1,1], [1,1], [1,1])
    while i[1] <= n[1] && i[2] <= n[2]
        equals = true
        for j = 1:2
            if hi[j][i[j]] < i[3-j] || (lo[j][i[j]] <= i[3-j] && v[j][i[j]] < v[3-j][i[3-j]])
                equals = false
                lo[j][i[j]] = i[3-j]
                hi[j][i[j]] = i[3-j] - 1
                i[j] += 1
                break
            end
        end
        if equals
            for j = 1:2
                first[j] = last[j] = i[j]
            end
            for j = 1:2
                i[j] += 1
                while i[j] <= hi[3-j][first[3-j]] && v[j][i[j]] == v[j][i[j]-1]
                    last[j] = i[j]
                    i[j] += 1
                end
            end
            for j = 1:2
                for k = first[j]:last[j]
                    lo[j][k] = first[3-j]
                    hi[j][k] = last[3-j]
                end
            end
        end
    end
    for j=1:2
        while i[j] <= n[j]
            lo[j][i[j]] = n[3-j] + 1
            hi[j][i[j]] = n[3-j]
            i[j] += 1
        end
    end
end

function find_indices( lo, hi )
    n = length.(lo)
    indices = zeros.(Int,n)
    in = [1,1]
    out = 1
    while in[1] <= n[1] || in[2] <= n[2]
        for i = 1:2
            if in[i] <= n[i]
                if hi[i][in[i]] < in[3-i]
                    indices[i][in[i]] = out
                    in[i] += 1
                    out += 1
                elseif lo[i][in[i]] <= in[3-i]
                    indices[i][in[i]] = out
                    indices[3-i][in[3-i]] = out
                    in[i] += 1
                    in[3-i] += 1
                    out += 1
                end
            end
        end
    end
    return indices
end

function invert_indices( inindices )
    n = max( inindices[1][end], inindices[2][end] )
    outindices = zeros.( Int, (n,n) )
    for i = 1:2
        for j = 1:length(inindices[i])-1
            for k = inindices[i][j]:inindices[i][j+1]-1
                outindices[i][k] = j
            end
        end
        m = length(inindices[i])
        for k = inindices[i][end]:n
            outindices[i][k] = m
        end
    end
    return outindices
end

typedefault( _ ) = nothing
typedefault( ::Type{Float64} ) = NaN
typedefault( ::Type{Int64} ) = typemin(Int64)
typedefault( ::Type{Date} ) = Date( 0 )
typedefault( ::Type{Bool} ) = false
typedefault( ::Type{CharN{N}} ) where N = convert( CharN{N}, "" )

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
    fillforward = false,
) where {S1, T1, U1, V1, W1, S2, T2, U2, V2, W2}
    print && println( "Materializing at $(now())" )
    commas = materialize.([comma1, comma2]);
    vs = collect(zip([getindex.( (commas[i],), [S1,S2][i] )  for i in 1:2]...))
    n = length.(vs[1])
    
    print && println( "Calculating indices at $(now())" )

    lo = fill.( 1, n )
    hi = fill.( reverse(n), n )
    
    for v in vs
        align!( v, lo, hi )
    end

    ks = collect.([ keys( cs1 ), keys( cs2 ) ])
    types = [
        eltype.( getindex.( (commas[1],), ks[1] ) ),
        eltype.( getindex.( (commas[2],), ks[2] ) ),
    ]
    defaults = [
        get.( (defaults1,), ks[1], typedefault.( types[1] ) ),
        get.( (defaults2,), ks[2], typedefault.( types[2] ) ),
    ]
    
    print && println( "Allocating results at $(now())" )

    indices = find_indices( lo, hi )

    results = [fill.( defaults[i],  max(indices[1][end], indices[2][end])) for i in 1:2]

    css = [cs1, cs2]
    if fillforward
        outindices = invert_indices( indices )
    end
    for i = 1:2
        if fillforward
            ii = outindices[i]
            r = findfirst(ii.!=0):length(ii)
            ii = ii[r]
        else
            ii = indices[i]
        end
        ki = ks[i]
        ri = results[i]
        ci = commas[i]
        for k = 1:length(ki)
            print && println( "Processing column $k of dataset $i at $(now())..." )
            if fillforward
                ri[k][r] = ci[ii,ki[k]]
            else
                ri[k][ii] = ci[1:length(ii),ki[k]]
            end
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
