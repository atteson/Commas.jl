using Commas

nt1 = (t = cumsum(rand(10^6)),);
Commas.makegetters( nt1 )

sorted = fill( NaN, 2*10^6 );
sortedindex = [0]
update( row::Commas.DataRow{NT}, sorted::Vector{Float64}, sortedindex::Vector{Int} ) where {NT <: NamedTuple} =
    sorted[sortedindex[1] += 1] = Commas.gett(row)
dc1 = DataCaller( nt1, [row -> update( row, sorted, sortedindex )] );

@time runcallbacks( dc1 )
sortedindex = [0]
@time runcallbacks( dc1 )

sorted = fill( NaN, 2*10^6 );
sortedindex = [0]
mc = MergeCaller( [dc1], Commas.gett );
@time runcallbacks( mc )
sortedindex = [0]
@time runcallbacks( mc )

exit()

nt2 = (t = cumsum(rand(10^6)),);
dc2 = DataCaller( nt1, [row -> update( row, sorted, sortedindex )] );

mc = MergeCaller( [dc1,dc2], Commas.gett );
sortedindex = [0]
@time runcallbacks( mc )
sortedindex = [0]
@time runcallbacks( mc )

@assert( maximum(abs.(sorted - sort(vcat(nt1.t, nt2.t)))) == 0.0 )
