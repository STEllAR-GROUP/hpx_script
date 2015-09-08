function mypr(a)
  local t
  t = type(a)
  if(t == "nil")then
    io.write('nil')
  elseif(t == "string")then
    io.write('"',a,'"')
  elseif(t == "number")then
    io.write(a)
  elseif(t == "table")then
    if(a["pr"] ~= nil)then
      a:pr()
      return
    end
    local k,v,sep
    io.write('{')
    sep = ''
    for k,v in pairs(a)do
      io.write(sep)
      mypr(k)
      io.write('=')
      mypr(v)
      sep = ','
    end
    io.write('}')
  elseif(t == "userdata")then
    local v
    v = a:Get()
    if(type(v)=="userdata")then
      io.write("recursion")
    else
      io.write('${')
      mypr(v)
      io.write('}')
    end
  elseif(t == "xuserdata")then
    io.write("userdata={")
    mypr(getmetatable(a))
    io.write("}")
  elseif(t == "function")then
    io.write("func()")
  else
    io.write('[',t,"?]")
  end
end
function myprn(a)
  mypr(a)
  io.write("\n")
end

function ret(x)
  return x
end

function idx(i,dir,nx)
  if(dir > 0 and i == nx)then
    return 1
  end
  if(dir < 0 and i == 1)then
    return nx
  end
  return i+dir
end

function heat(left,middle,right)
  k = 0.5
  dx = 1
  dt = 1
  return middle + (k*dt/(dx*dx)) * (left - 2*middle + right);
end

function table_insert(t,v)
  t[1+#t] = v
end

function Partition_new(size,initial_value)
  if(size == nil)then
    return nil
  end
  local pdata = vector_t.new() 
  for i=1,size do
    table_insert(pdata,initial_value+i)
  end
  return pdata
end

function heat_part(left,middle,right)
  local nh
  local nextp = Partition_new(#middle,0)
  local lf = left[#left] 
  nh = heat(lf,middle[1],middle[2])
  nextp[1]=nh
  for i=2,#middle-1 do
    nh = heat(middle[i-1],middle[i],middle[i+1])
    nextp[i]=nh
  end
  nh = heat(middle[#middle-1],middle[#middle],right[1])
  nextp[#middle]=nh
  return nextp
end

function do_work(nx,nt,sz)
  local u
  u = table_t.new()

  local t,x,np
  np = nx / sz
  for t=1,2 do
    table_insert(u,table_t.new())
    for x=1,nx,sz do
      table_insert(u[t],Partition_new(sz,x-1))
    end
  end

  local currv, nextv
  for t=1,nt do
    currv = ((t-1) % 2)+1
    nextv = (t % 2)+1
    for x=1,np do
      local result = heat_part(
        u[currv][idx(x,-1,np)],
        u[currv][x],
        u[currv][idx(x,1,np)])
      u[nextv][x] = result
    end
  end

  return u[nextv]
end

HPX_PLAIN_ACTION('heat','do_work','ret','idx','heat_part','Partition_new','mypr','myprn','table_insert')

u = do_work(80000,60,1000)
n = 1
for i,v in ipairs(u) do
  uv = v
  for j,k in ipairs(uv) do
    io.write('U[',n,'] = ',k,'\n')
    n = n+1
    if n == 11 then
      break
    end
  end
  if n == 11 then
    break
  end
end