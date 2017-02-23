-- Creator: Marshall Fate
-- Date: 2016/11/26
-- Time: 16:56

--this lua script is used for redis enque action with two conditions:
--1. fields is diffrent with former fields which refers to old_fields in key threshold of redis
--2. the difference between timestamp and old timestamp(in the key threshold of redis)
--   is longer than the time range (user specified), as a heart beat.
--The data can be enque at least one of above condition met, or just drop it.^_^

--  keys[1]  eqpt_no
--  args[1]  timestamp
--  args[2]  tags
--  args[3]  fields
--  args[4]  measurement

local timestamp = ARGV[1]
local tags =ARGV[2]
local fields = ARGV[3]
local measurement = ARGV[4]
--how much longer you want your time range is ???
local time_range = 30

local function threshold (fields, timestamp, time_range)
--    parameters:
--      fields      table influx json fields.
--      timestamp   string timestamp
--      return f_flag,t_flag  boolean
--    f_flag get True when fields is diffrent with old fields.
--    t_flag get True when time is longer than threshold time range.

    local old_fields = redis.call("HGET", "threshold", "fields")
    local old_timestamp = redis.call("HGET", "threshold", "timestamp")

    if old_fields == false or old_timestamp == false then
        redis.call("HSET", 'threshold', "fields", fields)
        redis.call("HSET", 'threshold', "timestamp", timestamp)
        return true, true
    end

    local f_flag = false
    local t_flag = false
    if fields ~= old_fields then
            f_flag = true
        redis.call("HSET", 'threshold', "fields", fields)
        redis.call("HSET", 'threshold', "timestamp", timestamp)
    end
    if tonumber(timestamp) - tonumber(old_timestamp) > time_range then
            t_flag = true
        redis.call("HSET", 'threshold', "fields", fields)
        redis.call("HSET", 'threshold', "timestamp", timestamp)
    end
    return f_flag, t_flag
end

--for using two user variables f_flag and t_flag.
local f_flag = nil;local t_flag = nil
f_flag, t_flag = threshold(fields, timestamp, time_range)

if f_flag == true then
    local influx_json = {
        data = {

            measurement = cmsgpack.unpack(ARGV[4]),
            time = ARGV[1],
            fields = cmsgpack.unpack(ARGV[3]),
            tags = cmsgpack.unpack(ARGV[2]),
        }
    }
    local msg = cmsgpack.pack(influx_json)
    redis.call("RPUSH", "data_queue",msg) -- msg queue
    return 'field enque worked~'

elseif t_flag == true then
    tags = cmsgpack.unpack(tags)
    tags['heartbeat'] = "yes"
    local influx_json = {
        data = {

            measurement = cmsgpack.unpack(measurement),
            time = timestamp,
            fields = cmsgpack.unpack(fields),
            tags = tags
        }
    }
    local msg = cmsgpack.pack(influx_json)
    redis.call("RPUSH", "data_queue",msg) -- msg queue
    return 'heart beat enque worked~'
else
    return 'ignoring schema worked!'
end
