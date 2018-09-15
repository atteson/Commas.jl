abstract type AbstractDataRow end

mutable struct DataRow{NT <: NamedTuple} <: AbstractDataRow
    nt::NT
    row::Int
end

eltypes( nt::NamedTuple{U,T} ) where {U,T} = T

function makegetters( nt )
    names = keys(nt)
    getternames = Symbol.("get" .* string.(names))
    types = eltypes( nt )
    for i = 1:length(names)
         eval( quote
             $(getternames[i])( row::DataRow{$(typeof(nt))} ) = row.nt.$(names[i])[row.row]
         end )
    end
end

abstract type AbstractCaller end

getcallbacks( caller::AbstractCaller ) = caller.callbacks

changecallbacks( caller::DataCaller, callbacks::Vector{F} ) where {F <: Function} = DataCaller( caller.df, callbacks )

mutable struct DataCaller{NT <: NamedTuple, F <: Function} <: AbstractCaller
    df::NT
    callbacks::Vector{F}
end

function runcallbacks( data::DataCaller{NT} ) where {NT <: NamedTuple}
    df = data.df
    row = DataRow( df, 0 )
    # must pre-decrement because we use Channel for merge
    while row.row < length(df[1])
        row.row += 1
        for callback in data.callbacks
            callback( row )
        end
    end
end

nt1 = (t = randn(10^6),);
makegetters( nt1 )

update( row::DataRow{NT}, sorted::Vector{Float64}, sortedindex::Vector{Int} ) where {NT <: NamedTuple} =
    sorted[sortedindex[1] += 1] = gett(row)

sorted = fill(NaN, 10^6);
sortedindex = [0]
dc1 = DataCaller( nt1, [row -> update(row,sorted,sortedindex)] );

@time runcallbacks( dc1 )
sortedindex = [0]
@time runcallbacks( dc1 )

mutable struct MergeCaller{C <: AbstractCaller, F <: Function} <: AbstractCaller
    callers::Vector{C}
    getter::F
end


function runcallbacks( mc::MergeCaller{C,F} ) where {C,F}
    n = length(mc.callers)
    data = Vector{AbstractDataRow}( undef, n )
    running = trues( n )

    maintask = current_task()
    
    function callertask( index::Int )
        caller = mc.callers[index]
        
        callbacks = getcallbacks( caller )
        
        function callback( row )
            data[index] = row
            yieldto( maintask )
        end

        newcaller = changecallbacks( caller, [callback] )

        runcallbacks( newcaller )

        running[index] = false
        yieldto( maintask )
    end

    callbackses = getcallbacks.( mc.callers )
        
    tasks = [Task(() -> callertask( i )) for i in 1:n]
    yieldto.( tasks )
    numrunning = sum(running)
    
    gotten = mc.getter.( data[running] )
    while numrunning > 0
        i = 1
        while !running[i]
            i += 1
        end
        minvalue = gotten[i]
        minindex = i

        while i < length(running)
            i += 1
            if running[i] && gotten[i] < minvalue
                minvalue = gotten[i]
                minindex = i
            end
        end
                
        for callback in callbackses[minindex]
            callback( data[minindex] )
        end
        yieldto( tasks[minindex] )
        if running[minindex]
            gotten[minindex] = mc.getter( data[minindex] )
        else
            numrunning -= 1
        end
    end
end

mc = MergeCaller( [dc1], gett );

sortedindex = [0]
@time runcallbacks( mc )
sortedindex = [0]
@time runcallbacks( mc )



