function ret(x)
  return x
end

function make_ready_future(x)
  return async('ret',x)
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
  return middle:Get() + (k*dt/(dx*dx)) * (left:Get() - 2*middle:Get() + right:Get());
end

function do_work(nx,nt)
  local u
  u = {}

  local t,x
  for t=1,2 do
    table.insert(u,{})
    for x=1,nx do
      table.insert(u[t],async('ret',x))
    end
  end

  for t=1,nt do
    local currv = ((t-1) % 2)+1
    local nextv = (t % 2)+1
    for x=1,nx do
      u[nextv][x] = async('heat',u[currv][idx(x,-1,nx)],u[currv][x],u[currv][idx(x,1,nx)])
    end
  end

  return u[(nt % 2)+1]
end

HPX_PLAIN_ACTION('heat','do_work','make_ready_future','ret','idx')

u = do_work(100,60)
nn = 0
for x,v in ipairs(u) do
  print("U["..x.."] = "..v:Get())
  nn = nn + 1
  if nn == 10 then
    break
  end
end
