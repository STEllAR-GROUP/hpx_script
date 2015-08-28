k = 0.5
dx = 1
dt = 1

function heat(left,middle,right)
  return middle + (k*dt/(dx*dx)) * (left - 2*middle + right);
end

function do_work(nx,nt)
  local u
  u = {}

  local t,x
  for t=1,2 do
    table.insert(u,{})
    for x=1,nx do
      table.insert(u[t],x)
    end
  end

  for t=1,nt do
    local currv = ((t-1) % 2)+1
    local nextv = (t % 2)+1
    u[nextv][1] = heat(u[currv][nx],u[currv][1],u[currv][2])
    for x=2,nx-1 do
      u[nextv][x] = heat(u[currv][x-1],u[currv][x],u[currv][x+1])
    end
    u[nextv][nx] = heat(u[currv][nx-1],u[currv][nx],u[currv][1])
  end

  return u[(nt % 2)+1]
end

u = do_work(80000,60)
nn = 0
for x,v in ipairs(u) do
  print("U["..x.."] = "..v)
  nn = nn + 1
  if nn == 10 then
    break
  end
end
