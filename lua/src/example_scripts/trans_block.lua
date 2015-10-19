function transpose(inp,outp,block_count,block_size)
  local i,j,ib,jb
  for ib=1,block_count do
    for jb=1,block_count do
      for i=1,block_size do
        for j=1,block_size do
          outp[ib][jb][i][j] = inp[jb][ib][j][i]
        end
      end
    end
  end
end

function fill(i,j)
  return 100*i+j
end

function zero(i,j)
  return 0
end

function create_matrix(block_count,block_size)
  local i,ib,jb,m,mb
  m = table_t.new()
  for ib=1,block_count do
    m[ib] = table_t.new()
    for jb=1,block_count do
      mb = table_t.new()
      m[ib][jb] = mb
      for i=1,block_size do
        mb[i] = vector_t.new()
        mb[i][block_size] = 0
      end
    end
  end
  return m
end

function fill_matrix(m,f,block_count,block_size)
  local i,j,ib,jb,ioff,joff
  for ib=1,block_count do
    ioff = (ib-1)*block_size
    for jb=1,block_count do
      joff = (jb-1)*block_size
      mb = m[ib][jb]
      for i=1,block_size do
        for j=1,block_size do
          mb[i][j] = f(ioff+i,joff+j)
        end
      end
    end
  end
end

function print_matrix(m,n,block_count,block_size)
  local i,j,ib,jb
  for ib=1,block_count do
    for i=1,block_size do
      if (ib-1)*block_size+i <= n then
        for jb=1,block_count do
          for j=1,block_size do
            if (jb-1)*block_size+j <= n then
              io.write(m[ib][jb][i][j])
              io.write(' ')
            end
          end
        end
        io.write('\n')
      end
    end
  end
end

block_count = 3
block_size = 80
order = block_count*block_size
a = create_matrix(block_count,block_size)
fill_matrix(a,fill,block_count,block_size)
b = create_matrix(block_count,block_size)
fill_matrix(b,zero,block_count,block_size)
transpose(a,b,block_count,block_size)
print("=====")
print_matrix(a,6,block_count,block_size)
print("=====")
print_matrix(b,6,block_count,block_size)
