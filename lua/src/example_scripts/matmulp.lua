function inner(a,b,c,i,n)
  local k,j
  for j=1,n do
    for k=1,n do
      c[i][j] = c[i][j]+a[i][k]*b[k][j]
    end
  end
end

function matmul(a,b,c,n)
  local i
  local f={} --table_t.new()
  for i=1,n do
    f[i]=async('inner',a,b,c,i,n)
  end
  wait_all(f)
end

n = 50
local a=table_t.new()
local b=table_t.new()
local c=table_t.new()
for i=1,n do
  a[i] = vector_t.new()
  b[i] = vector_t.new()
  c[i] = vector_t.new()
  for j=1,n do
    a[i][j] = i+j
    b[i][j] = i-j
    c[i][j] = 0
  end
end

HPX_PLAIN_ACTION('inner')

matmul(a,b,c,n)

for i=1,10 do
  for j=1,10 do
    io.write(c[i][j])
    io.write('\t')
  end
  io.write('\n')
end
