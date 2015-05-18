function fadd(a,b)
  return a+b
end

function fib(n)
  if n < 2 then return n; end
  local n1 = async('fib',n-1)
  local n2 = fib(n-2)
  --calling get() is inefficient,
  --especially in lua. See fib2.lua
  return n2 + n1:Get()
end
------------------------------
--Need to register functions to
--run them through hpx
HPX_PLAIN_ACTION('fadd','fib')

--Like async(), but tries to
--not create a new thread. Needed
--if the argument is a futrue.
f1 = fib(20)
dataflow('print',f1)
