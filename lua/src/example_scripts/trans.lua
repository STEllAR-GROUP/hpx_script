function transpose(inp,outp)
  local i,j,n
  n = #inp
  for i=1,n do
    for j=1,n do
      outp[i][j] = inp[j][i]
    end
  end
end

function fill(i,j)
  return 100*i+j
end

function zero(i,j)
  return 0
end

function create_matrix(order)
  local i,j,m
  m = table_t.new()
  for i=1,order do
    m[i] = vector_t.new()
  end
  return m
end

function fill_matrix(m,f)
  local i,j,order
  order = #m
  for i=1,order do
    for j=1,order do
      m[i][j] = f(i,j)
    end
  end
end

function print_matrix(m,n)
  local i,j
  if n == nil then
    n = #m
  end
  for i=1,n do
    for j=1,n do
      io.write(m[i][j])
      io.write(' ')
    end
    io.write('\n')
  end
end

block_count = 3
block_size = 80
order = block_count*block_size
a = create_matrix(order)
fill_matrix(a,fill)
b = create_matrix(order)
fill_matrix(b,zero)
transpose(a,b)
print("=====")
print_matrix(a,6)
print("=====")
print_matrix(b,6)
