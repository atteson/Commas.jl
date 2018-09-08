export DataCaller, runcallbacks, FilterCaller, HcatDataCaller, MergeDataCaller

abstract type AbstractCaller end

getcallbacks( caller::AbstractCaller ) = caller.callbacks

setcallbacks!( caller::AbstractCaller, callbacks::Vector{T} ) where {T <: Function} =
    caller.callbacks = callbacks

mutable struct DataCaller{NT <: NamedTuple} <: AbstractCaller
    df::NT
    callbacks::Vector{Function}
end

DataCaller( df::NT, callbacks::Vector{F} ) where {NT <: NamedTuple, F <: Function} =
    DataCaller( df, Vector{Function}(callbacks) )

function runcallbacks( data::DataCaller{NT} ) where {NT <: NamedTuple}
    df = data.df
    row = DataRow( df, 0 )
    # must pre-decrement because we use Channel for merge
    while row.row < length(df[1])
        row.row += 1
        println( "Calling callbacks with row $(row.row)" )
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

mutable struct HcatDataCaller <: AbstractCaller
    iterator
    callbacks::Vector{Function}
end

HcatDataCaller( iterator, callbacks::Vector{F} ) where {NT <: NamedTuple, F <: Function} =
    HcatDataCaller( iterator, Vector{Function}(callbacks) )

function runcallbacks( hc::HcatDataCaller )
    callbacks = getcallbacks( hc )
    for name in hc.iterator
        df = readcomma( name )
        runcallbacks( DataCaller( df, callbacks ) )
    end
end

mutable struct MergeDataCaller{C <: AbstractCaller} <: AbstractCaller
    callers::Vector{C}
    getter::Function
end

function gencoroutine( caller::AbstractCaller )
    function coroutine( c::Channel )
        mergecallback( row::DataRow ) = put!( c, row )
            
        callbacks = getcallbacks( caller )
        setcallbacks!( caller, Function[mergecallback] )

        runcallbacks( caller )

        setcallbacks!( caller, callbacks )
    end
end

function runcallbacks( mc::MergeDataCaller )
    callbacks = getcallbacks.( mc.callers )

    channels = Channel.( gencoroutine.( mc.callers ) )
    
    return take!.( channels )
end
