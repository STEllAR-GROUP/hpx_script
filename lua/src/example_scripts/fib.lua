function fadd(a,b)
  return a+b
end

function fib(n)
  if n < 2 then return n; end
  local n1 = async('fib',n-1)
  local n2 = fib(n-2)
  --calling get() is inefficient,
  --especially in lua. See fib2.lua
  return n2 + n1:get()
end
------------------------------
--Need to register functions to
--run them through hpx
hpx_reg('fadd','fib')

--Like async(), but tries to
--not create a new thread. Needed
--if the argument is a futrue.
f1 = unwrap('fib',20)
unwrap('print',f1)
