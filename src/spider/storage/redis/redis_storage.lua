#!lua name=spider_storage

local function spider_hset_heartbeat(keys, args)
    local timestamp = redis.call('TIME')[1]
    for i = 1, #keys do
        redis.call('HSET', keys[i], 'heartbeat', timestamp, unpack(args))
    end
end

redis.register_function('spider_hset_heartbeat', spider_hset_heartbeat)