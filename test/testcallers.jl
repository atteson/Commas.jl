using Commas
using Random

Random.seed!(1)
nt1 = (t = cumsum(rand(10^6)),);
Commas.makegetters( nt1 )

sorted = fill( NaN, 2*10^6 );
sortedindex = [0]
update( row::Commas.DataRow{NT}, sorted::Vector{Float64}, sortedindex::Vector{Int} ) where {NT <: NamedTuple} =
    sorted[sortedindex[1] += 1] = Commas.gett(row)
dc1 = DataCaller( nt1, [row -> update( row, sorted, sortedindex )] );

function f1( caller::DataCaller{NT}, sorted, sortedindex ) where {NT}
    row = Commas.start( caller )
    while Commas.next!( caller, row )
        update( row, sorted, sortedindex )
    end
end
sortedindex[1] = 0
@time f1( dc1, sorted, sortedindex )
sortedindex[1] = 0
@time f1( dc1, sorted, sortedindex )

sortedindex[1] = 0
@time runcallbacks( dc1 )
sortedindex[1] = 0
@time runcallbacks( dc1 )

update2( row, sorted::Vector{Float64}, sortedindex::Vector{Int} ) =
    sorted[sortedindex[1] += 1] = Commas.gett(row)
dc2 = DataCaller( nt1, [row -> update2( row, sorted, sortedindex )] );
sortedindex[1] = 0
@time runcallbacks( dc2 )
sortedindex[1] = 0
@time runcallbacks( dc2 )

fc1 = FilterCaller( dc1, row -> Commas.gett( row ) > 10^5 )
function f2( caller::FilterCaller{NT}, sorted, sortedindex ) where {NT}
    row = Commas.start( caller )
    while Commas.next!( caller, row )
        update( row, sorted, sortedindex )
    end
end
sortedindex[1] = 0
@time f2( fc1, sorted, sortedindex )
sortedindex[1] = 0
@time f2( fc1, sorted, sortedindex )
@assert( sortedindex[1] == sum(nt1.t.>10^5) )

sortedindex[1] = 0
@time runcallbacks( fc1 )
sortedindex[1] = 0
@time runcallbacks( fc1 )
@assert( sortedindex[1] == sum(nt1.t.>10^5) )



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

