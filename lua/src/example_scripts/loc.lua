--create a script
function pr(a)
	print('pr='..a)
end

--register it so it's available everywhere
hpx_reg('pr')

--find the current locality
here = locality.find_here()

--run the script here
async(here,"pr","hello")

--get all localities
all = locality.all_localities()

--run the script on all of them
for k,v in ipairs(all) do
	print('k=',k,'v=',""..v)
	async(v,"pr",k)
end
