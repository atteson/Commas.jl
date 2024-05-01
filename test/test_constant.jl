abstract type A{T,U}
end

struct T1{T,U,V <: Union{NamedTuple{T,U},A{T,U}}} <: A{T,U}
    t::V
end

f( t::NamedTuple{T,U}, s::Symbol ) where {T,U} = t[s]
f( t::T1{T,U}, s::Symbol ) where {T,U} = f( t.t, s )

function g( t::T1{T,U} ) where {T,U}
    s = 0.0
    for i = 1:3
        s += f( t, :a )[i]
    end
    return s
end

t = T1(T1((a=[1,2,3],b=[1.0,2.0])))

@code_warntype g(t)

