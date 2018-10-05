export DataCaller, runcallbacks, FilterCaller, HcatCaller, MergeCaller

abstract type AbstractCaller end

getcallbacks( caller::AbstractCaller ) = caller.callbacks

function setcallbacks!( caller::AbstractCaller, callbacks::Vector{T} ) where {T <: Function}
    caller.callbacks = callbacks
end



abstract type NamedTupleDataCaller{NT <: NamedTuple} <: AbstractCaller end

function runcallbacks( data::AbstractCaller )
#function runcallbacks( data::NamedTupleDataCaller{NT} ) where {NT <: NamedTuple}
    (row, state) = start( data )
    while next!( data, row, state )
        for callback in data.callbacks
            callback( row )
        end
    end
end



mutable struct DataCaller{NT <: NamedTuple, F <: Function} <: NamedTupleDataCaller{NT}
    df::NT
    callbacks::Vector{F}
end

# standard iterator doesn't quite fit our use case
start( data::DataCaller{NT, F} ) where {NT, F} = (DataRow( data.df, 0 ), nothing)

function next!( caller::DataCaller{NT,F}, row::DataRow{NT}, state ) where {NT, F}
    row.row += 1
    return row.row <= length(row.nt[1])
end



mutable struct FilterCaller{NT <: NamedTuple, F <: Function} <: NamedTupleDataCaller{NT}
    caller::NamedTupleDataCaller{NT}
    filter::Function
    callbacks::Vector{F}
end

FilterCaller( caller::NamedTupleDataCaller{NT}, filter::Function, callbacks::Vector{F} = caller.callbacks ) where {NT, F} =
    FilterCaller( caller, filter, callbacks )

start( caller::FilterCaller{NT, F} ) where {NT, F} = start( caller.caller )

function next!( caller::FilterCaller{NT, F}, row::DataRow{NT}, state ) where {NT, F}
    done = false
    while !done && next!( caller.caller, row, state )
        done = caller.filter( row )
    end
    return done
end



mutable struct HcatCaller <: AbstractCaller
    iterator
    callbacks::Vector{Function}
end

function start( data::HcatCaller )
    result = iterate( data.iterator )
    if result != nothing
        (dir, state) = result
        df = readcomma( dir )
        row = DataRow( df, 0 )
        return (row, [state])
    end
    return (nothing, nothing)
end

function next!( data::HcatCaller, row::DataRow, state )
    state == nothing && return false

    row.row += 1
    if row.row > length(row.nt[1])
        result = iterate( data.iterator, state[1] )
        result == nothing && return false
        
        (dir, newstate) = result
        row.nt = readcomma( dir )
        row.row = 1
        state[1] = newstate
    end
    return true
end



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

        setcallbacks!( caller, [callback] )

        runcallbacks( caller )

        setcallbacks!( caller, callbacks )

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
