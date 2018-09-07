export DataCaller, runcallbacks, FilterCaller

abstract type AbstractCaller end

getcallbacks( caller::AbstractCaller ) = caller.callbacks

setcallbacks!( caller::AbstractCaller, callbacks::Vector{T} ) where {T <: Function} =
    caller.callbacks = callbacks

mutable struct DataCaller{NT <: NamedTuple, F <: Function} <: AbstractCaller
    df::NT
    callbacks::Vector{F}
end

function runcallbacks( data::DataCaller{NT} ) where {NT <: NamedTuple}
    df = data.df
    row = DataRow( df, 1 )
    while row.row <= length(df[1])
        for callback in data.callbacks
            callback( row )
        end
        row.row += 1
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
    
    setcallbacks!( data, [filtercallback] )
    return FilterCaller( data )
end

runcallbacks( filter::FilterCaller ) = runcallbacks( filter.data )

struct HcatCaller{F <: Function} <: AbstractCaller
    iterator
    callbacks::Vector{F}
end

function runcallbacks( hc::HcatCaller )
    callbacks = getcallbacks( hc )
    for name in hc.iterator
        df = readcomma( name )
        runcallbacks( DataCaller( df, callbacks ) )
    end
end
