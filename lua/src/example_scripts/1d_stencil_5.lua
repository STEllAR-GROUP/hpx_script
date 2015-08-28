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

function Partition_new(size,initial_value)
  if(size == nil)then
    return nil
  end
  local pdata = {}
  for i=1,size do
    table.insert(pdata,initial_value+i)
  end
  --Create and return the object
  return {
    data = pdata,
    size = size
  }
end

function heat_part(tf)
  local t = tf:Get()
  local left   = t[1]:Get()
  local middle = t[2]:Get()
  local right  = t[3]:Get()
  local nh
  local nextp = Partition_new(middle.size,0)
  local lf = left.data[left.size] --get(left.size-1)
  nh = heat(lf,middle.data[1],middle.data[2])
  nextp.data[1]=nh
  for i=2,middle.size-1 do
    nh = heat(middle.data[i-1],middle.data[i],middle.data[i+1])
    nextp.data[i]=nh
  end
  nh = heat(middle.data[middle.size-1],middle.data[middle.size],right.data[1])
  nextp.data[middle.size]=nh
  return nextp
end

function do_work(nx,nt,sz)
  local u
  u = {}

  local t,x,np
  np = nx / sz
  for t=1,2 do
    table.insert(u,{})
    for x=1,nx,sz do
      table.insert(u[t],make_ready_future(Partition_new(sz,x-1)))
    end
  end

  local currv, nextv
  for t=1,nt do
    currv = ((t-1) % 2)+1
    nextv = (t % 2)+1
    for x=1,np do
      local fut = when_all(
        u[currv][idx(x,-1,np)],
        u[currv][x],
        u[currv][idx(x,1,np)])
      local result = fut:Then('heat_part')
      u[nextv][x] = result
    end
  end

  return u[nextv]
end

HPX_PLAIN_ACTION('heat','do_work','idx','heat_part','Partition_new','mypr','myprn')

u = do_work(1000,600,100)
n = 1
for i,v in ipairs(u) do
  uv = v:Get()
  for j,k in ipairs(uv.data) do
    io.write('U[',n,'] = ',k,'\n')
    n = n+1
  end
end
