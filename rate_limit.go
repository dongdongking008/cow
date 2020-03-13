package main

import (
	"errors"
	"fmt"
	"github.com/go-redis/redis/v7"
	"github.com/go-redis/redis_rate/v8"
	"strconv"
	"time"
)

var rdb *redis.Client

func getRDB() *redis.Client {
	if rdb == nil {
		if len(config.RateLimitRedisSentinelAddrs) > 0 {
			rdb = redis.NewFailoverClient(&redis.FailoverOptions{
				MasterName:    config.RateLimitRedisSentinelMasterName,
				SentinelAddrs: config.RateLimitRedisSentinelAddrs,
				Password:      config.RateLimitRedisPassword,
			})
		} else {
			if len(config.RateLimitRedisAddr) <=0 {
				Fatal("client rate limit need to config RateLimitRedisAddr")
			}
			rdb = redis.NewClient(&redis.Options{
				Addr: config.RateLimitRedisAddr,
				Password:      config.RateLimitRedisPassword,
			})
		}
	}
	return rdb
	//_ = rdb.FlushDB().Err()
}

func NewLimiter() *redis_rate.Limiter {
	return redis_rate.NewLimiter(getRDB())
}

type rediser interface {
	Eval(script string, keys []string, args ...interface{}) *redis.Cmd
	EvalSha(sha1 string, keys []string, args ...interface{}) *redis.Cmd
	ScriptExists(hashes ...string) *redis.BoolSliceCmd
	ScriptLoad(script string) *redis.StringCmd
}

type RateLimit struct {
	Rate   int	// 0 means unlimited
	Period time.Duration
	Burst  int
}

func (rateLimit *RateLimit) String() string {
	return fmt.Sprint(rateLimit.Rate, ":", rateLimit.Period.Seconds(), ":", rateLimit.Burst)
}

//------------------------------------------------------------------------------

// Limiter controls how frequently events are allowed to happen.
type RemainCountLimiter struct {
	rdb rediser
	limiterKey string
	rcLastStatSortedSetKey string
	rcConfigHashKey string
	rcConfig map[string]*RateLimit
}

const redisPrefix = "rc_rate:"

// NewRemainCountLimiter returns a new Remain Count based Limiter.
func NewRemainCountLimiter(limiterKey string, rcConfig map[string]*RateLimit) *RemainCountLimiter {

	rdb := getRDB()

	limiter := &RemainCountLimiter{
		rdb: rdb,
		limiterKey: limiterKey,
		rcLastStatSortedSetKey: redisPrefix + limiterKey,
		rcConfigHashKey: redisPrefix + limiterKey + ":config",
		rcConfig: rcConfig,
	}

	mapConfig := make(map[string]interface{})
	values := make([]*redis.Z, 0)

	for key, val := range rcConfig {
		mapConfig[key] = val.String()
		values = append(values, &redis.Z{Score: 0, Member: key})
	}

	// Set config hash
	resConfig := rdb.HMSet(limiter.rcConfigHashKey, mapConfig)
	info.Println("init remain count rate limiter config result", resConfig)
	if resConfig.Err() != nil {
		panic(errors.New(fmt.Sprint("init remain count rate limiter config error:", resConfig.Err().Error())))
	}

	// Set last stat sorted set.
	// Don't update already existing elements. Always add new elements.
	result := rdb.ZAddNX(limiter.rcLastStatSortedSetKey, values...)
	info.Println("init remain count rate limiter result", result)
	if result.Err() != nil {
		panic(errors.New(fmt.Sprint("init remain count rate limiter error:", result.Err().Error())))
	}

	return limiter
}

// Allow reports whether 1 events may happen at time now.
func (l *RemainCountLimiter) Allow() (*Result, error) {
	//if debug {
	//	rdb := getRDB()
	//	zResult := rdb.ZRangeWithScores(l.rcLastStatSortedSetKey, 0, 10)
	//	debugLog.Printf("%s = %v", l.rcLastStatSortedSetKey, zResult.Val())
	//}

	v, err := remainCountLua.Run(l.rdb, []string{l.rcLastStatSortedSetKey,
	l.rcConfigHashKey}).Result()

	//debugLog.Printf("remain count limiter allow result: %v", v)

	if err != nil {
		return nil, err
	}

	values := v.([]interface{})

	retryAfter, err := strconv.ParseFloat(values[2].(string), 64)
	if err != nil {
		return nil, err
	}

	resetAfter, err := strconv.ParseFloat(values[3].(string), 64)
	if err != nil {
		return nil, err
	}

	res := &Result{
		Allowed:    values[0].(int64) == 0,
		Remaining:  int(values[1].(int64)),
		RetryAfter: dur(retryAfter),
		ResetAfter: dur(resetAfter),
		RateLimitKey: values[4].(string),
	}
	return res, nil
}

func dur(f float64) time.Duration {
	if f == -1 {
		return -1
	}
	return time.Duration(f * float64(time.Second))
}

type Result struct {
	// Allowed reports whether event may happen at time now.
	Allowed bool

	// Remaining is the maximum number of requests that could be
	// permitted instantaneously for this key given the current
	// state. For example, if a rate limiter allows 10 requests per
	// second and has already received 6 requests for this key this
	// second, Remaining would be 4.
	Remaining int

	// RetryAfter is the time until the next request will be permitted.
	// It should be -1 unless the rate limit has been exceeded.
	RetryAfter time.Duration

	// ResetAfter is the time until the RateLimiter returns to its
	// initial state for a given key. For example, if a rate limiter
	// manages requests per second and received one request 200ms ago,
	// Reset would return 800ms. You can also think of this as the time
	// until RateLimit and Remaining will be equal.
	ResetAfter time.Duration

	// RateLimitKey is the key selected to perform
	RateLimitKey string
}


var remainCountLua = redis.NewScript(`
-- this script has side-effects, so it requires replicate commands mode
redis.replicate_commands()

local limited = 1
local remaining = 0
local retry_after = 60
local reset_after = 60
local rate_limit_key_in_set

local rate_limit_sorted_set_key = KEYS[1]
local rate_limit_config_hash_key = KEYS[2]
local rate_limit_key_replies = redis.call("ZRANGE", rate_limit_sorted_set_key, 0, 2)

if #rate_limit_key_replies > 0 then
	local config
	for limit_key in ipairs(rate_limit_key_replies) do
		rate_limit_key_in_set = rate_limit_key_replies[1]
		config = redis.call("HGET", rate_limit_config_hash_key, rate_limit_key_in_set)
		if #config > 0 then
	    	break
		else
			redis.call("ZREM", rate_limit_config_hash_key, rate_limit_key_in_set)
		end
	end
	if #config > 0 then
		local t = {}
		for str in string.gmatch(config, "([^\:]+)") do
			table.insert(t, str)
    	end
		if #t == 3 then
			local rate_limit_key = rate_limit_sorted_set_key .. ":" ..rate_limit_key_in_set 
			local rate = t[1]
			local period = t[2]
			local burst = t[3]

			local emission_interval = period / rate
			local increment = emission_interval
			local burst_offset = emission_interval * burst
			local now = redis.call("TIME")
	
			-- redis returns time as an array containing two integers: seconds of the epoch
			-- time (10 digits) and microseconds (6 digits). for convenience we need to
			-- convert them to a floating point number. the resulting number is 16 digits,
			-- bordering on the limits of a 64-bit double-precision floating point number.
			-- adjust the epoch to be relative to Jan 1, 2017 00:00:00 GMT to avoid floating
			-- point problems. this approach is good until "now" is 2,483,228,799 (Wed, 09
			-- Sep 2048 01:46:39 GMT), when the adjusted value is 16 digits.
			local jan_1_2017 = 1483228800
			now = (now[1] - jan_1_2017) + (now[2] / 1000000)

			local tat = redis.call("GET", rate_limit_key)

			if not tat then
  			  tat = now
			else
  			  tat = tonumber(tat)
			end

			local new_tat = math.max(tat, now) + increment

			local allow_at = new_tat - burst_offset
			local diff = now - allow_at

			remaining = math.floor(diff / emission_interval + 0.5) -- poor man's round

			if remaining < 0 then
			  limited = 1
			  remaining = 0
			  reset_after = tat - now
			  retry_after = diff * -1
			else
			  limited = 0
			  reset_after = new_tat - now
			  redis.call("SET", rate_limit_key, new_tat, "EX", math.ceil(reset_after))
			  redis.call("ZADD", rate_limit_sorted_set_key, new_tat, rate_limit_key_in_set)
			  retry_after = -1
			end
		end
	end
end

return {limited, remaining, tostring(retry_after), tostring(reset_after), rate_limit_key_in_set}
`)
