--create a script

function size(a)
  local sz,k,v
  sz = 0
  for k,v in pairs(a) do
    sz = sz + 1
  end
  return sz
end

function pr(a)
	print('pr='..a)
end

f = {}

--register it so it's available everywhere
HPX_PLAIN_ACTION('pr')

--find the current locality
here = locality.find_here()

--run the script here
table.insert(f,async(here,"pr","hello"))

--get all localities
all = locality.find_all_localities()

--run the script on all of them
print('all localities')
for k,v in ipairs(all) do
	print('k=',k,'v=',""..v)
	table.insert(f,async(v,"pr",k))
end

remote = locality.find_remote_localities()
print('remote localities')
for k,v in ipairs(remote) do
	print('k=',k,'v=',""..v)
	table.insert(f,async(v,"pr",k))
end

print('root locality='..locality.find_root_locality())
sz = size(f)
print("wait for futures")
for i=1,sz do
  w = when_any(f)
  wa = w:Get()
  print('index='..wa.index..'/'..size(f))
  table.remove(f,wa.index)
end
