export DataCaller, runcallbacks, FilterCaller, HcatCaller, MergeCaller

abstract type AbstractCaller end

getcallbacks( caller::AbstractCaller ) = caller.callbacks

function setcallbacks!( caller::AbstractCaller, callbacks::Vector{T} ) where {T <: Function}
    caller.callbacks = callbacks
end

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

mutable struct FilterCaller <: AbstractCaller
    data::AbstractCaller
end

function FilterCaller( data::AbstractCaller, filter::Function )
    callbacks = getcallbacks( data )
    
    function filtercallback( row )
        if filter( row )
            for callback in callbacks
                callback( row )
            end
        end
    end
    
    setcallbacks!( data, Function[filtercallback] )
    return FilterCaller( data )
end

runcallbacks( filter::FilterCaller ) = runcallbacks( filter.data )

mutable struct HcatCaller <: AbstractCaller
    iterator
    callbacks::Vector{Function}
end

HcatCaller( iterator, callbacks::Vector{F} ) where {NT <: NamedTuple, F <: Function} =
    HcatCaller( iterator, Vector{Function}(callbacks) )

function runcallbacks( hc::HcatCaller )
    callbacks = getcallbacks( hc )
    for name in hc.iterator
        df = readcomma( name )
        runcallbacks( DataCaller( df, callbacks ) )
    end
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
