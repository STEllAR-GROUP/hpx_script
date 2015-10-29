--still need to combine the data back
function quicks(data)
    local pivot = partition(data)
    local i=-1
    local left=vector_t.new()
    local right=vector_t.new()
    local f

    if #data > 75 then
      if pivot>2 then
         for i=1, pivot-1 do
           left[i]=data[i]
         end
         f = async('quicks',left)
      else
         f = make_ready_future(nil)
      end
      if pivot < #data-2 then
         for i=pivot+1,#data do
             right[i-pivot]=data[i]
         end
         quicks(right)
      end
      f:Get()
    else
      if pivot>2 then
         for i=1, pivot-1 do
           left[i]=data[i]
         end
         quicks(left)
      end
      if pivot < #data-2 then
         for i=pivot+1,#data do
             right[i-pivot]=data[i]
         end
         quicks(right)
      end
    end

    if pivot > 2 then
      for i=1, pivot-1 do
        data[i]=left[i]
      end
    end
    if pivot < #data-2 then
       for i = pivot+1, #data do
         data[i]=right[i-pivot]
       end
    end

    if pivot == 3 and data[1]>data[2] then
      data[1], data[2] = data[2], data[1]
   end
    if pivot == #data-2 and data[#data-1]>data[#data] then
       data[#data-1], data[#data] = data[#data], data[#data-1]
    end
end

function partition(data)
   local pivot=math.random(1,#data)
   local pivotVal=data[pivot]
   data[pivot],data[#data] = data[#data],data[pivot]
   local indexsmall=1
   local i=1
   for i=1, #data do
     if data[i]< pivotVal then
       data[i], data[indexsmall] = data[indexsmall], data[i]
       indexsmall = indexsmall+1
     end
   end
   --print('index small '..indexsmall)
   data[indexsmall],data[#data] = data[#data],data[indexsmall]
   return indexsmall
end

HPX_PLAIN_ACTION('quicks','partition')

mydata=vector_t.new()
local j=0
for j=1, 100000 do
   mydata[j]=math.random(10000)
end

quicks(mydata)
for i,v in ipairs(mydata) do io.write(v) io.write(' ') if i > 20 then break end end
