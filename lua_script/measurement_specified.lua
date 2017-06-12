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
--  args[5]  data_type

--set parameters:
--how much seconds you want your time range is ???
--unit (s)
local time_range = 10


-- for threshold value , don't unpack this because we need put it to redis queue.



-- up pack here, for processing data.
local tags = KEYS[1]
local timestamp = ARGV[1]
local fields = ARGV[2]
local measurement = ARGV[3]
local unit = ARGV[4]


-- FUNCTION PART----------------------------------------------------------------------
local function threshold(fields, timestamp, time_range, measurement)
    --    parameters:
    --      fields      msgpacked binary format for fields data.
    --      timestamp   string format timestamp.
    --      data_type   for weiss machine, two data type I and F.
    --      time_range  threshold value.
    --    return :
    --      f_flag      boolean value.
    --      t_flag      boolean value.
    --    f_flag get True when fields are diffrent with old fields.
    --    t_flag get True when time is longer than threshold time_range.

    local measurement = cmsgpack.unpack(measurement)
    local threshold_name = string.format('threshold_%s', measurement)

    --  get old threshold value
    local old_fields = redis.call("HGET", threshold_name, "fields")
    local old_timestamp = redis.call("HGET", threshold_name, "timestamp")

    --  threshold value doesn't exist, set the value.
    if old_fields == false or old_timestamp == false then
        redis.call("HSET", threshold_name, "fields", fields)
        redis.call("HSET", threshold_name, "timestamp", timestamp)
        return true, true
    end

    --  threshold value exists, enque or not ?
    local f_flag = false
    local t_flag = false

    --  the fields are changed, refresh the threshold value.
    if fields ~= old_fields then
        f_flag = true
        redis.call("HSET", threshold_name, "fields", fields)
        redis.call("HSET", threshold_name, "timestamp", timestamp)
    end

    --  the time difference is longer than time_range, refresh the threshold value.
    if tonumber(timestamp) - tonumber(old_timestamp) > time_range then
        t_flag = true
        redis.call("HSET", threshold_name, "fields", fields)
        redis.call("HSET", threshold_name, "timestamp", timestamp)
    end
    return f_flag, t_flag
end

local function to_float(fields)

    for key, value in pairs(fields) do
        if type(fields[key]) == type(1) and key ~= 'status' then
            fields[key] = value + 0.001
        end
    end

    return fields
end

-- FUNCTION PART----------------------------------------------------------------------



--for using two user variables f_flag and t_flag.
local f_flag = nil; local t_flag = nil

-- use unit to determine time_range.
if cmsgpack.unpack(unit) == 'u' then
    time_range = time_range * 1000000
end

--Save the amount of data for 2 days based on a data of 5 seconds
if redis.call("llen", "data_queue") > 2 * 24 * 60 * 60 / 5 then
    redis.call("lpop", "data_queue")
end

f_flag, t_flag = threshold(fields, cmsgpack.unpack(timestamp), time_range, measurement)

--fields = to_float(fields) to float

-- push this data to the data_queue of redis.



if f_flag == true then

    local data = {
        measurement = measurement,
        time = timestamp,
        fields = fields,
        tags = tags,
        unit = unit
    }

    local msg = cmsgpack.pack(data)
    redis.call("RPUSH", "data_queue", msg) -- msg queue
    return 'field enque worked~'

elseif t_flag == true then



    local data = {
        heartbeat = true,
        measurement = measurement,
        time = timestamp,
        fields = fields,
        tags = tags,
        unit = unit
    }
    local msg = cmsgpack.pack(data)
    redis.call("RPUSH", "data_queue", msg) -- msg queue
    return 'heart beat enque worked~'
else
    return 'ignoring schema worked!'
end
