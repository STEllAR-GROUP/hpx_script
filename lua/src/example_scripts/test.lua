--basic async
a = async(function() end)
if a:Get() ~= nil then
  print('a not nil')
end

a = async(function() return 5 end)
if a:Get() ~= 5 then
  print('a not five')
end

function after(f)
  if f:Get() ~= 5 then
    print('f not five')
  end
  return 5
end

function afteru(f)
  if f ~= 5 then
    print('f not five')
  end
  return 5
end

--then
HPX_PLAIN_ACTION('after','afteru')
a:Then('after'):Get()
a:Then(after):Get()

--remote call
loc = find_here()
a = async(loc,after,make_ready_future(5))
after(a)

--Unravel nested futures
after(make_ready_future(make_ready_future(5)))

--when_any
futsi = { make_ready_future(1), make_ready_future(2), make_ready_future(3) }
wait_all(futsi)
futso = {}
while #futsi > 0 do
  w = when_any(futsi):Get()
  futso[#futso+1] = w.futures[w.index]:Get()
  table.remove(futsi,w.index)
end
sum = 0
for i,v in ipairs(futso) do
  sum = sum + v
end
if sum ~= 6 then
  print('sum not 6')
end

--test tables
t = {i=5,s='str',a=function() return 6 end,t={i2=6}}
tf = make_ready_future(t)
t2 = tf:Get()
if #t ~= #t2 then
  print("table lengths are incorrect")
end
for k,v in pairs(t) do
  if type(v) == "table" then
    -- test if tables are set correctly
    for k2,v2 in pairs(v) do
      if v2 ~= t2[k][k2] then
        print('field ',k,',',k2,' is not set right')
      end
    end
  elseif type(v) == 'function' then
    -- test if functions are the same by
    -- testing the return value
    if v() ~= t2[k]() then
      print('field ',k,' is not set right')
    end
  elseif v ~= t2[k] then
    print('field ',k,' is not set right')
  end
end

--test unwrapped
a = async(unwrapped(afteru,make_ready_future(5)))
a:Get()
a = async(unwrapped('afteru',make_ready_future(5)))
a:Get()

--test counters
d = discover_counter_types()

counter = nil

--Search for the specific counter
--we are interested in
for i,c in ipairs(d) do
  if string.find(c["fullname_"],"uptime") then
    counter = hpx.get_counter(c)
  end
end
if counter == nil then
  print('could not find counter')
end

--Get the counter value
value = hpx.get_value(counter)
t1 = value["time_"]
if type(t1) ~= "number" then
  print("wrong type returned from value")
end

c = component.new(find_here())
c:Set("x",1)
if c:Get("x"):Get() ~= 1 then
  print("Components don't work")
end
c:Set("loc",find_here())
if ""..c:Get("loc"):Get() ~= ""..find_here() then
  print("Component locality check failed")
end

print("No other output than this message means test succeeded")
