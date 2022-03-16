local key = KEYS[1]
local min_score = ARGV[1]
local max_score = ARGV[2]
local num = ARGV[3]

-- get zset data
local datas = nil
if num ~= '' then
    datas = redis.call('zrangebyscore', key, min_score, max_score, 'withscores', 'limit', 0, num)
else
    datas = redis.call('zrangebyscore', key, min_score, max_score, 'withscores')
end

-- del data
local item_list = {}
for i=1, #datas, 2 do
    table.insert(item_list, datas[i])
    redis.call('zrem', key, datas[i])
end
return item_list