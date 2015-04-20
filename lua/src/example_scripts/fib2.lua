function fadd(a,b)
  return a+b
end

function fibs(n)
  if n < 2 then return n; end
  return fibs(n-1)+fibs(n-2)
end

function fib(n)
  if n < 2 then return n; end
  local n1
  local n2
  if (n > 21) then
    n1 = dataflow('fib',n-1)
    n2 = fib(n-2)
    --use unwrap with fadd instead of :get()
    --if possible
    return dataflow('fadd',n1,n2)
  else
    -- avoid any hpx calls if possible
    n1 = fibs(n-1)
    n2 = fibs(n-2)
    return n1+n2
  end
end

------------------------------
hpx_reg('fadd','fib','fibs')

f1 = fib(30)
print(f1:get())
