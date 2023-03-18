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
    vs = collect(zip(getindex.( (comma1,), S1 ), getindex.( (comma2,), S2 )))
    n = length.(vs[1])
    
    print && println( "Calculating indices at $(now())" )

    lo = fill.( 1, n )
    hi = fill.( reverse(n), n )
    for v in vs
        align!( v, lo, hi )
    end

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

    indices = find_indices( lo, hi )

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
    m2::Union{AbstractVector{Symbol},NTuple{N2,Symbol}};
    printevery = Inf,
) where {S,T,U,V,W,N,N1,N2}
    s1 = (s1...,)
    s2 = (s2...,)
    s = [s1, s2]
    m = [(s1...,m1...), (s2...,m2...)]
    startc = cmp( getindex.( (comma,), 1, s[1] ), getindex.( (comma,), 1, s[2] ) )
    record = false
    printevery < Inf && println( "Starting at $(now())..." )

    tc = (comma,)
    for i = 1:size(comma,1)
        if i % printevery == 0
            println( "Done $i at $(now())..." )
        end
        c = cmp( getindex.( tc, i, s[1] ), getindex.( tc, i, s[2] ) )
        if c == 0 || c != startc
            record = true
        end
        cs = (c <= 0, c >= 0)

        for j = 1:2
            if record && cs[j] && !cs[3-j]
                data = getindex.( tc, i-1, m[j] )
                setindex!.( tc, data, i, m[j] )
            end
        end
    end
    return comma
end

function fillforwardindices(
    comma::Comma{S,T,U,V,W},
    defaults = Dict{Symbol,Any}(),
) where {S,T,U,V,W,N}
    ks = 
    types = eltype.( getindex.( (comma,), symbols ) )
    defaults = get.( (defaults,), symbols, typedefault.( types ) )

    n = size(comma,1)
    currfillfrom = 1
    fillfrom = zeros(Int,n)
    for i = 2:size(comma,1)
        fill = false
        for j in 1:length(symbols)
            if comma[i,symbols[j]] == defaults[j]
                fill = true
                break
            end
        end
        if fill
            fillfrom[i] = currfillfrom
        else
            currfillfrom = i
        end
    end
    return fillfrom
end
