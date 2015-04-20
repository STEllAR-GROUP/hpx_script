--create a script
function pr(a)
	print('pr='..a)
end

--register it so it's available everywhere
HPX_PLAIN_ACTION('pr')

--find the current locality
here = locality.find_here()

--run the script here
async(here,"pr","hello")

--get all localities
all = locality.find_all_localities()

--run the script on all of them
print('all localities')
for k,v in ipairs(all) do
	print('k=',k,'v=',""..v)
	async(v,"pr",k)
end

remote = locality.find_remote_localities()
print('remote localities')
for k,v in ipairs(remote) do
	print('k=',k,'v=',""..v)
	async(v,"pr",k)
end

print('root locality='..locality.find_root_locality())
