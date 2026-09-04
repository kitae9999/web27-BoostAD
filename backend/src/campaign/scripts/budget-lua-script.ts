// Budget Redis stores campaign state in plain Redis hashes. Search Redis/RedisJSON
// commands must never be introduced into these scripts.

// KEYS[1] = budget:campaign:{id}; ARGV[1] = JSON projection
export const REDIS_APPLY_BUDGET_PROJECTION_SCRIPT = `
  local incoming = cjson.decode(ARGV[1])
  local incomingVersion = tonumber(incoming.servingVersion)
  local currentVersion = tonumber(redis.call('HGET', KEYS[1], 'servingVersion')) or -1
  if currentVersion > incomingVersion then return -1 end
  if currentVersion == incomingVersion then return 0 end

  local dailySpent = tonumber(redis.call('HGET', KEYS[1], 'dailySpent'))
  local totalSpent = tonumber(redis.call('HGET', KEYS[1], 'totalSpent'))
  local dailyReserved = tonumber(redis.call('HGET', KEYS[1], 'dailyReserved')) or 0
  local totalReserved = tonumber(redis.call('HGET', KEYS[1], 'totalReserved')) or 0
  local dailyReservedDate = redis.call('HGET', KEYS[1], 'dailyReservedDate')

  if dailySpent == nil then dailySpent = tonumber(incoming.dailySpent) or 0 end
  if totalSpent == nil then totalSpent = tonumber(incoming.totalSpent) or 0 end
  if not dailyReservedDate or dailyReservedDate == '' then
    dailyReservedDate = incoming.budgetDate
  end

  local totalBudget = ''
  if incoming.totalBudget ~= nil and incoming.totalBudget ~= cjson.null then
    totalBudget = tostring(incoming.totalBudget)
  end

  redis.call('HSET', KEYS[1],
    'servingVersion', tostring(incomingVersion),
    'tombstone', '0',
    'status', incoming.status,
    'maxCpc', tostring(incoming.maxCpc),
    'dailyBudget', tostring(incoming.dailyBudget),
    'totalBudget', totalBudget,
    'dailySpent', tostring(dailySpent),
    'totalSpent', tostring(totalSpent),
    'dailyReserved', tostring(dailyReserved),
    'totalReserved', tostring(totalReserved),
    'dailyReservedDate', dailyReservedDate,
    'lastResetDate', incoming.lastResetDate
  )
  redis.call('PERSIST', KEYS[1])
  return 1
`;

// KEYS[1] = budget:campaign:{id}; ARGV[1] = servingVersion,
// ARGV[2] = optional JSON fallback used only when a historical/shadow key is absent
export const REDIS_APPLY_BUDGET_TOMBSTONE_SCRIPT = `
  local incomingVersion = tonumber(ARGV[1])
  local currentVersion = tonumber(redis.call('HGET', KEYS[1], 'servingVersion')) or -1
  if currentVersion > incomingVersion then return -1 end
  if currentVersion == incomingVersion and redis.call('HGET', KEYS[1], 'tombstone') == '1' then
    return 0
  end
  if currentVersion == -1 and ARGV[2] and ARGV[2] ~= '' then
    local incoming = cjson.decode(ARGV[2])
    local totalBudget = ''
    if incoming.totalBudget ~= nil and incoming.totalBudget ~= cjson.null then
      totalBudget = tostring(incoming.totalBudget)
    end
    redis.call('HSET', KEYS[1],
      'maxCpc', tostring(incoming.maxCpc),
      'dailyBudget', tostring(incoming.dailyBudget),
      'totalBudget', totalBudget,
      'dailySpent', tostring(tonumber(incoming.dailySpent) or 0),
      'totalSpent', tostring(tonumber(incoming.totalSpent) or 0),
      'dailyReserved', '0',
      'totalReserved', '0',
      'dailyReservedDate', incoming.budgetDate,
      'lastResetDate', incoming.lastResetDate
    )
  end
  redis.call('HSET', KEYS[1],
    'servingVersion', tostring(incomingVersion),
    'tombstone', '1',
    'status', 'PAUSED'
  )
  redis.call('PERSIST', KEYS[1])
  return 1
`;

export const REDIS_HASH_INCREMENT_SPENT_SCRIPT = `
  local cpc = tonumber(ARGV[1])
  if redis.call('EXISTS', KEYS[1]) == 0 then return -99 end
  if redis.call('HGET', KEYS[1], 'tombstone') == '1' then return -2 end
  if redis.call('HGET', KEYS[1], 'status') ~= 'ACTIVE' then return -2 end
  local dailyBudget = tonumber(redis.call('HGET', KEYS[1], 'dailyBudget'))
  local totalBudget = tonumber(redis.call('HGET', KEYS[1], 'totalBudget'))
  local dailySpent = tonumber(redis.call('HGET', KEYS[1], 'dailySpent')) or 0
  local totalSpent = tonumber(redis.call('HGET', KEYS[1], 'totalSpent')) or 0
  if not dailyBudget or dailySpent + cpc > dailyBudget then return 0 end
  if totalBudget and totalSpent + cpc > totalBudget then return -1 end
  redis.call('HINCRBY', KEYS[1], 'dailySpent', cpc)
  redis.call('HINCRBY', KEYS[1], 'totalSpent', cpc)
  return 1
`;

export const REDIS_HASH_REPLACE_SPENT_SCRIPT = `
  if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
  redis.call('HSET', KEYS[1], 'dailySpent', ARGV[1], 'totalSpent', ARGV[2])
  return 1
`;

export const REDIS_HASH_RESET_DAILY_BUDGET_SCRIPT = `
  if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
  redis.call('HSET', KEYS[1],
    'dailySpent', '0',
    'dailyReserved', '0',
    'dailyReservedDate', ARGV[1],
    'lastResetDate', ARGV[2]
  )
  return 1
`;

export const REDIS_HASH_RESET_LOADTEST_SCRIPT = `
  if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
  redis.call('HSET', KEYS[1],
    'dailySpent', '0',
    'totalSpent', '0',
    'dailyReserved', '0',
    'totalReserved', '0',
    'dailyReservedDate', ARGV[1],
    'lastResetDate', ARGV[2]
  )
  return 1
`;

// KEYS[1] expiration ZSET, KEYS[2] auction key, KEYS[3..] budget campaign hashes
// ARGV[1..4] auctionId, blogId, budgetDate, expiresAt
// ARGV[5..] expected serving versions, followed by campaign IDs
export const REDIS_HASH_RESERVE_AUCTION_SCRIPT = `
  local existingRaw = redis.call('GET', KEYS[2])
  if existingRaw then
    local ok, existing = pcall(cjson.decode, existingRaw)
    if ok and tonumber(existing.version) == 2 then
      return {2, existing.campaignId or '', 0, existingRaw, 0}
    end
    return {-2, '', 0, '', 0}
  end

  local candidateCount = #KEYS - 2
  local redisTime = redis.call('TIME')
  local nowMs = tonumber(redisTime[1]) * 1000 + math.floor(tonumber(redisTime[2]) / 1000)
  for i = 1, candidateCount do
    local campaignKey = KEYS[i + 2]
    local expectedVersion = tonumber(ARGV[4 + i])
    local campaignId = ARGV[4 + candidateCount + i]
    local values = redis.call('HMGET', campaignKey,
      'servingVersion', 'tombstone', 'status', 'maxCpc', 'dailyBudget',
      'totalBudget', 'dailySpent', 'totalSpent', 'dailyReserved',
      'totalReserved', 'dailyReservedDate')
    local currentVersion = tonumber(values[1])

    if not currentVersion or currentVersion ~= expectedVersion then
      -- A higher-ranked Search candidate whose Budget projection is absent or
      -- on another version must trigger a fresh match. Skipping it and
      -- reserving a lower-ranked candidate would mix a stale ranking decision
      -- with a newer Budget view.
      return {-3, '', i, '', 1}
    elseif values[2] ~= '1' then
      local cpc = tonumber(values[4])
      local dailyBudget = tonumber(values[5])
      local totalBudget = tonumber(values[6])
      local dailySpent = tonumber(values[7]) or 0
      local totalSpent = tonumber(values[8]) or 0
      local dailyReserved = 0
      if values[11] == ARGV[3] then dailyReserved = tonumber(values[9]) or 0 end
      local totalReserved = tonumber(values[10]) or 0
      local dailyEligible = cpc and dailyBudget and dailySpent + dailyReserved + cpc <= dailyBudget
      local totalEligible = not totalBudget or totalSpent + totalReserved + cpc <= totalBudget

      if values[3] == 'ACTIVE' and dailyEligible and totalEligible then
        redis.call('HSET', campaignKey,
          'dailyReserved', tostring(dailyReserved + cpc),
          'totalReserved', tostring(totalReserved + cpc),
          'dailyReservedDate', ARGV[3])
        local reservation = {
          version = 2,
          auctionId = ARGV[1],
          campaignId = campaignId,
          campaignServingVersion = currentVersion,
          blogId = tonumber(ARGV[2]),
          cost = cpc,
          status = 'RESERVED',
          budgetDate = ARGV[3],
          createdAt = nowMs,
          updatedAt = nowMs,
          expiresAt = tonumber(ARGV[4])
        }
        local reservationJson = cjson.encode(reservation)
        redis.call('SET', KEYS[2], reservationJson)
        redis.call('ZADD', KEYS[1], tonumber(ARGV[4]), ARGV[1])
        return {1, campaignId, i, reservationJson, 0}
      end
    end
  end

  return {0, '', candidateCount, '', 0}
`;

export const REDIS_HASH_COMMIT_AUCTION_SCRIPT = `
  local raw = redis.call('GET', KEYS[1])
  if not raw then redis.call('ZREM', KEYS[2], ARGV[1]); return {-99, ''} end
  local ok, reservation = pcall(cjson.decode, raw)
  if not ok or tonumber(reservation.version) ~= 2 then return {-98, ''} end
  if reservation.status == 'COMMITTED' then return {2, raw} end
  if reservation.status == 'RELEASED' then return {-1, raw} end
  local amount = tonumber(reservation.cost)
  local nowMs = tonumber(ARGV[4])

  local function writeMarker(status)
    local marker = { version = 2, auctionId = ARGV[1], status = status, updatedAt = nowMs }
    local markerRaw = cjson.encode(marker)
    redis.call('SETEX', KEYS[1], tonumber(ARGV[3]), markerRaw)
    redis.call('ZREM', KEYS[2], ARGV[1])
    return markerRaw
  end
  local function releaseReservation()
    if redis.call('EXISTS', KEYS[3]) == 1 then
      local totalReserved = tonumber(redis.call('HGET', KEYS[3], 'totalReserved')) or 0
      redis.call('HSET', KEYS[3], 'totalReserved', tostring(math.max(0, totalReserved - amount)))
      if redis.call('HGET', KEYS[3], 'dailyReservedDate') == reservation.budgetDate then
        local dailyReserved = tonumber(redis.call('HGET', KEYS[3], 'dailyReserved')) or 0
        redis.call('HSET', KEYS[3], 'dailyReserved', tostring(math.max(0, dailyReserved - amount)))
      end
    end
    return writeMarker('RELEASED')
  end

  if tonumber(reservation.expiresAt) <= nowMs or reservation.budgetDate ~= ARGV[2] then
    return {-2, releaseReservation()}
  end
  if redis.call('EXISTS', KEYS[3]) == 0 then return {-3, releaseReservation()} end
  local totalReserved = tonumber(redis.call('HGET', KEYS[3], 'totalReserved')) or 0
  local dailyReserved = tonumber(redis.call('HGET', KEYS[3], 'dailyReserved')) or 0
  redis.call('HSET', KEYS[3],
    'totalReserved', tostring(math.max(0, totalReserved - amount)),
    'dailyReserved', tostring(math.max(0, dailyReserved - amount)))
  redis.call('HINCRBY', KEYS[3], 'dailySpent', amount)
  redis.call('HINCRBY', KEYS[3], 'totalSpent', amount)
  return {1, writeMarker('COMMITTED')}
`;

export const REDIS_HASH_RELEASE_AUCTION_SCRIPT = `
  local raw = redis.call('GET', KEYS[1])
  if not raw then redis.call('ZREM', KEYS[2], ARGV[1]); return {-99, ''} end
  local ok, reservation = pcall(cjson.decode, raw)
  if not ok or tonumber(reservation.version) ~= 2 then return {-98, ''} end
  if reservation.status == 'COMMITTED' then return {-1, raw} end
  if reservation.status == 'RELEASED' then return {2, raw} end
  local amount = tonumber(reservation.cost)
  if redis.call('EXISTS', KEYS[3]) == 1 then
    local totalReserved = tonumber(redis.call('HGET', KEYS[3], 'totalReserved')) or 0
    redis.call('HSET', KEYS[3], 'totalReserved', tostring(math.max(0, totalReserved - amount)))
    if redis.call('HGET', KEYS[3], 'dailyReservedDate') == reservation.budgetDate then
      local dailyReserved = tonumber(redis.call('HGET', KEYS[3], 'dailyReserved')) or 0
      redis.call('HSET', KEYS[3], 'dailyReserved', tostring(math.max(0, dailyReserved - amount)))
    end
  end
  local marker = { version = 2, auctionId = ARGV[1], status = 'RELEASED', updatedAt = tonumber(ARGV[3]) }
  local markerRaw = cjson.encode(marker)
  redis.call('SETEX', KEYS[1], tonumber(ARGV[2]), markerRaw)
  redis.call('ZREM', KEYS[2], ARGV[1])
  return {1, markerRaw}
`;

export const REDIS_HASH_DECREMENT_SPENT_SCRIPT = `
  local cpc = tonumber(ARGV[1])
  if redis.call('EXISTS', KEYS[1]) == 0 then return -99 end
  local dailySpent = tonumber(redis.call('HGET', KEYS[1], 'dailySpent')) or 0
  local totalSpent = tonumber(redis.call('HGET', KEYS[1], 'totalSpent')) or 0
  if dailySpent - cpc < 0 then return 0 end
  if totalSpent - cpc < 0 then return -1 end
  redis.call('HINCRBY', KEYS[1], 'dailySpent', -cpc)
  redis.call('HINCRBY', KEYS[1], 'totalSpent', -cpc)
  return 1
`;
