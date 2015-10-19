function transpose_s(self,block_size)
  local i,j,m,inp
  inp = self.mb;
  m = table_t.new()
  for i=1,block_size do
    m[i] = vector_t.new()
    for j=1,block_size do
      m[i][j] = inp[j][i]
    end
  end
  return m
end

function transpose(inp,outp,block_count,block_size)
  local i,j,ib,jb,v,f
  v={}
  for ib=1,block_count do
    for jb=1,block_count do
      f = outp[ib][jb]:Set("mb",inp[jb][ib]:Call(transpose_s,block_size))
      v[#v+1] = f
    end
  end
  wait_all(f)
end

function fill(i,j)
  return 100*i+j
end

function zero(i,j)
  return 0
end

function create_matrix(block_count,block_size)
  local i,ib,jb,m,mb,all,loc
  all = find_all_localities()
  m = table_t.new()
  for ib=1,block_count do
    m[ib] = table_t.new()
    for jb=1,block_count do
      loc = all[(ib+jb) % #all + 1]
      sub = component.new(loc)
      mb = table_t.new()
      for i=1,block_size do
        mb[i] = vector_t.new()
        mb[i][block_size] = 0
      end
      sub:Set("mb",mb)
      m[ib][jb] = sub
    end
  end
  return m
end

function fill_matrix_s(self,f,ioff,joff,block_size)
  local i,j,mb
  mb = self.mb
  for i=1,block_size do
    for j=1,block_size do
      mb[i][j] = f(ioff+i,joff+j)
    end
  end
end

function fill_matrix(m,f,block_count,block_size)
  local i,j,ib,jb,ioff,joff,v,fut,sub
  v={}
  for ib=1,block_count do
    ioff = (ib-1)*block_size
    for jb=1,block_count do
      joff = (jb-1)*block_size
      sub = m[ib][jb]
      fut = sub:Call(fill_matrix_s,f,ioff,joff,block_size)
      v[#v+1] = fut
    end
  end
  wait_all(v)
end

function print_matrix(m,n,block_count,block_size)
  local i,j,ib,jb,sub,mb
  for ib=1,block_count do
    for i=1,block_size do
      if (ib-1)*block_size+i <= n then
        for jb=1,block_count do
          for j=1,block_size do
            if (jb-1)*block_size+j <= n then
              sub = m[ib][jb]
              mb = sub:Get("mb"):Get()
              io.write(mb[i][j])
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
print("=====")
print_matrix(a,6,block_count,block_size)
b = create_matrix(block_count,block_size)
fill_matrix(b,zero,block_count,block_size)
transpose(a,b,block_count,block_size)
print("=====")
print_matrix(b,6,block_count,block_size)
