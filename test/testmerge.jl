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

mutable struct DataCaller{NT <: NamedTuple, F <: Function} <: AbstractCaller
    df::NT
    callbacks::Vector{F}
end

changecallbacks( caller::DataCaller, callbacks::Vector{F} ) where {F <: Function} = DataCaller( caller.df, callbacks )

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
    # this needs to change
    data = Vector{DataRow{NamedTuple{(:t,),Tuple{Vector{Float64}}}}}( undef, n )
    running = trues( n )

    maintask = current_task()
    
#    function callertask( index::Int, mc::MergeCaller{C,F}, data::Vector{DataRow{NamedTuple{(:t,),Tuple{Vector{Float64}}}}}, maintask::Task, running::BitArray ) where {C,F}
    function callertask( index::Int, mc::MergeCaller{C,F}, maintask::Task, running::BitArray ) where {C,F}
        caller = mc.callers[index]
        
        callbacks = getcallbacks( caller )
        
#        function callback( row::DataRow{NamedTuple{(:t,),Tuple{Vector{Float64}}}}, index::Int, data::Vector{DataRow{NamedTuple{(:t,),Tuple{Vector{Float64}}}}}, maintask::Task )
        function callback( row::DataRow{NamedTuple{(:t,),Tuple{Vector{Float64}}}}, maintask::Task )
#            data[index] = row
            yieldto( maintask )
        end

#        newcaller = changecallbacks( caller, [row -> callback( row, index, data, maintask )] )
        newcaller = changecallbacks( caller, [row -> callback( row, maintask )] )

        runcallbacks( newcaller )

        running[index] = false
        yieldto( maintask )
    end

    callbackses = getcallbacks.( mc.callers )
        
#    tasks = [Task(() -> callertask( i, mc, data, maintask, running )) for i in 1:n]
    tasks = [Task(() -> callertask( i, mc, maintask, running )) for i in 1:n]
    yieldto.( tasks )
    numrunning = sum(running)
    
#    gotten = mc.getter.( data[running] )
    sorted = fill( NaN, 10^6 )
    sortedindex = [0]
    while numrunning > 0
        i = 1
#        while !running[i]
#            i += 1
#        end
#        minvalue = gotten[i]
        minindex = i

#        while i < length(running)
#            i += 1
#            if running[i] && gotten[i] < minvalue
#                minvalue = gotten[i]
#                minindex = i
#            end
#        end
                
#        for callback in callbackses[minindex]
#            callback( data[minindex] )
        #        end
#        update( data[minindex], sorted, sortedindex )
        yieldto( tasks[minindex] )
        if running[minindex]
#            gotten[minindex] = gett( data[minindex] )
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

function f()
    data = [0]
    maintask = current_task()
    function taskf( maintask::Task, data::Vector{Int} )
        for i = 1:10^6
            yieldto( maintask, data )
        end
    end
    subtask = Task(()->taskf( maintask, data ))
    for i = 1:10^6
        yieldto( subtask, data )
    end
end
@time f()
@time f()

function f()
    maintask = current_task()
    function taskf(maintask::Task)
        for i = 1:10^6
            yield( maintask )
        end
    end
    subtask = Task(() -> taskf(maintask))
    for i = 1:10^6
        yield( subtask )
    end
end
@time f()
@time f()

function g()
    c = Channel(1)
    x = cumsum( rand(10^6) )
    function taskf()
        for i = 1:10^6
            put!( c, x[i] )
        end
    end
    task = Task(taskf)
    schedule(task)
    for i = 1:10^6
        y = take!(c)
    end
end
@time g()        
@time g()        

function h()
    x = cumsum( rand(10^6) )
    function taskf(c::Channel)
        for i = 1:10^6
            put!( c, x[i] )
        end
    end
    c = Channel(taskf)
    for i = 1:10^6
        y = take!(c)
    end
end
@time h()
@time h()

function f2()
    fastyield( task::Task ) = 
        ccall( :jl_switchto, Cvoid, (Any,), Ref(task) )
    
    function taskf(maintask::Task)
        for i = 1:10^6
            fastyield( maintask )
        end
    end
    
    maintask = current_task()
    task = Task(()->taskf(maintask))
    for i = 1:10^6
        fastyield( task )
    end
end
@time f2()
@time f2()

caller = DataCaller( nt1, Function[] )

Base.iterate( caller::DataCaller{U,T}, state::Int = 1 ) where {U,T} =
    if state > length(caller.df.t)
        return nothing
    else
        return (caller.df.t[state], state+1)
    end

function f()
    sum = 0
    result = Iterators.iterate( nt1.t )
    while result != nothing
        sum += 1
        result = Iterators.iterate( nt1.t, result[2] )
    end
end
@time f()
@time f()

function g()
    sum = 0
    for t in nt1.t
        sum += 1
    end
    return sum
end
@time g()
@time g()

x = rand(10^6)
function f1( x::Vector{Float64} )
    sum = 0.0
    i = 1
    n = 10^6
    while i < n
        sum += x[i]
        i += 1
    end
    return sum
end
@time f1(x)
@time f1(x)

function f2( x::Vector{Float64} )
    sum = 0.0
    for xi in x
        sum += xi
    end
    return sum
end
@time f2(x)
@time f2(x)

