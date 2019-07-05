local cjson = require "cjson"

_M = { _VERSION = "1.0" }

local reset = 0

local function expire_key(redis_connection, key, interval, log_level)
    local expire, error = redis_connection:expire(key, interval)
    if not expire then
        ngx.log(log_level, "failed to get ttl: ", error)
        return
    end
end

local function bump_request(redis_connection, redis_pool_size, ip_key, rate, interval, current_time, log_level, long_ttl)
    local key = "RL:" .. ip_key

    -- Fetch key
    local key_data, error = redis_connection:get(key)
    if not key_data then
        ngx.log(log_level, "failed to get key: ", error)
        return
    end

    -- If data exists, use it otherwise set default start data
    local key_table = {}
    if key_data == ngx.null then
      key_table.count = 0
      key_table.reset = current_time + interval
      key_table.interval = interval
      key_table.date_blocked = 0
      key_table.long_blocked = 0
    else
      key_table = cjson.decode(key_data)
    end

    -- If key is long blocked, set interval to display that.
    if key_table.long_blocked == 1 then
      key_table.interval = long_ttl
    end

    -- Increment request count
    key_table.count = key_table.count + 1;

    if tonumber(key_table.count) == 1 then -- If first request for key
        key_table.reset = (current_time + key_table.interval)
    else
        if current_time >= key_table.reset then -- If reset has been passed, reset count
          if key_table.long_blocked == 1 then
            -- If currently longblocked and reset has been met
            key_table.long_blocked = 0;
            key_table.date_blocked = 0
            key_table.interval = interval
          end

          key_table.count = 1
          key_table.reset = current_time + key_table.interval
        end
    end

    local already_blocked = false
    if key_table.long_blocked == 1 then
      already_blocked = true
    end

    -- If this request will be blocked.
    if key_table.long_blocked ~= 1 and key_table.count >= rate then
      if key_table.date_blocked == 0 then -- If first time blocked, save timestamp
        key_table.date_blocked = current_time
      elseif current_time > key_table.date_blocked + key_table.interval then -- If second time, set long_blocked to 1 and update blocked timestamp
        key_table.interval = long_ttl
        key_table.long_blocked = 1
        key_table.date_blocked = current_time
        key_table.reset = current_time + key_table.interval
      end
    end

    -- Only set key if not already long blocked.
    if already_blocked == false then

      local key_ttl = interval + 300
      if key_table.long_blocked == 1 then
        key_ttl = long_ttl + 300
      end

      -- Set key with table data
      local set, error = redis_connection:setex(key, key_ttl, cjson.encode(key_table))
      if not set then
        ngx.log(log_level, "failed to set key: ", error)
        return
      end
    end

    local ok, error = redis_connection:set_keepalive(60000, redis_pool_size)
    if not ok then
        ngx.log(log_level, "failed to set keepalive: ", error)
    end

    -- Remaining request before block.
    local remaining = rate - key_table.count

    return { count = key_table.count, remaining = remaining, reset = key_table.reset }

end

function _M.limit(config)
    local uri_parameters = ngx.req.get_uri_args()
    local enforce_limit = true
    local whitelisted_api_keys = config.whitelisted_api_keys or {}

    for i, api_key in ipairs(whitelisted_api_keys) do
        if api_key == uri_parameters.api_key then
            enforce_limit = false
        end
    end

    if enforce_limit then
        local log_level = config.log_level or ngx.ERR

        if not config.connection then
            local ok, redis = pcall(require, "resty.redis")
            if not ok then
                ngx.log(log_level, "failed to require redis")
                return
            end

            local redis_config = config.redis_config or {}
            redis_config.timeout = redis_config.timeout or 1
            redis_config.host = redis_config.host or "127.0.0.1"
            redis_config.port = redis_config.port or 6379
            redis_config.pool_size = redis_config.pool_size or 100

            local redis_connection = redis:new()
            redis_connection:set_timeout(redis_config.timeout * 1000)

            local ok, error = redis_connection:connect(redis_config.host, redis_config.port)
            if not ok then
                ngx.log(log_level, "failed to connect to redis: ", error)
                return
            end

            config.redis_config = redis_config
            config.connection = redis_connection
        end

        local current_time = ngx.now()
        local connection = config.connection
        local redis_pool_size = config.redis_config.pool_size
        local key = config.key or ngx.var.remote_addr
        local rate = config.rate or 10
        local interval = config.interval or 1
        local long_ttl = config.long_interval or 300

        local response, error = bump_request(connection, redis_pool_size, key, rate, interval, current_time, log_level, long_ttl)
        if not response then
            return
        end

        if response.count > rate then
            local retry_after = math.floor(response.reset - current_time)
            if retry_after < 0 then
                retry_after = 0
            end

            ngx.header["Access-Control-Allow-Origin"] = "*"
            ngx.header["Content-Type"] = "application/json; charset=utf-8"
            ngx.header["Retry-After"] = retry_after
            ngx.status = 429
            ngx.say('{"status_code":25,"status_message":"Your request count (' .. response.count .. ') is over the allowed limit of ' .. rate .. '."}')
            ngx.exit(ngx.HTTP_OK)
        else
            ngx.header["X-RateLimit-Limit"] = rate
            ngx.header["X-RateLimit-Remaining"] = math.floor(response.remaining)
            ngx.header["X-RateLimit-Reset"] = math.floor(response.reset)
        end
    else
        return
    end
end

return _M
