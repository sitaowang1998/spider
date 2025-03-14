
local function hset_heartbeat(keys, args)
    local timestamp = redis.call('TIME')[1]
    for i = 1, #keys do
        redis.call('HSET', keys[i], 'heartbeat', timestamp, unpack(args))
    end
end

redis.register_function('hset_heartbeat', hset_heartbeat)