// Lua Script: 원자적 예산 검증 + Spent 증가 (by Claude)
// KEYS[1] = campaign:{id}
// KEYS[2] = daily exhausted set
// KEYS[3] = total exhausted set
// ARGV[1] = cpc
// ARGV[2] = dailyBudget
// ARGV[3] = totalBudget (null이면 "null" 문자열)
// ARGV[4] = campaignId
//
// 반환값:
// 1 = 성공 (Spent 증가됨)
// 0 = 일일 예산 초과
// -1 = 총 예산 초과
// -99 = 캠페인 없음
export const REDIS_INCREMENT_SPENT_SCRIPT = `
    local campaignKey = KEYS[1]
    local dailyExhaustedKey = KEYS[2]
    local totalExhaustedKey = KEYS[3]
    local cpc = tonumber(ARGV[1])
    local dailyBudget = tonumber(ARGV[2])
    local totalBudgetStr = ARGV[3]
    local campaignId = ARGV[4]
    
    -- 현재 spent 값 조회 (JSON 경로에서)
    local dailySpentRaw = redis.call('JSON.GET', campaignKey, '$.dailySpent')
    local totalSpentRaw = redis.call('JSON.GET', campaignKey, '$.totalSpent')
    local currentMaxCpcRaw = redis.call('JSON.GET', campaignKey, '$.maxCpc')
    local currentDailyBudgetRaw = redis.call('JSON.GET', campaignKey, '$.dailyBudget')
    local currentTotalBudgetRaw = redis.call('JSON.GET', campaignKey, '$.totalBudget')
    
    if not dailySpentRaw or not totalSpentRaw then
      return -99  -- 캠페인 없음
    end
    
    -- JSON 배열 형태로 반환되므로 파싱 필요 (예: "[50000]")
    local dailySpent = tonumber(string.match(dailySpentRaw, '%[(%d+)%]')) or 0
    local totalSpent = tonumber(string.match(totalSpentRaw, '%[(%d+)%]')) or 0
    local hintCpc = currentMaxCpcRaw and tonumber(string.match(currentMaxCpcRaw, '%[([%d%.]+)%]')) or cpc
    local hintDailyBudget = currentDailyBudgetRaw and tonumber(string.match(currentDailyBudgetRaw, '%[([%d%.]+)%]')) or dailyBudget
    local hintTotalBudget = currentTotalBudgetRaw and tonumber(string.match(currentTotalBudgetRaw, '%[([%d%.]+)%]'))

    local function syncBudgetHints(nextDailySpent, nextTotalSpent)
      if nextDailySpent + hintCpc > hintDailyBudget then
        redis.call('SADD', dailyExhaustedKey, campaignId)
      else
        redis.call('SREM', dailyExhaustedKey, campaignId)
      end

      if hintTotalBudget and nextTotalSpent + hintCpc > hintTotalBudget then
        redis.call('SADD', totalExhaustedKey, campaignId)
      else
        redis.call('SREM', totalExhaustedKey, campaignId)
      end
    end
    
    -- 일일 예산 검증
    if dailySpent + cpc > dailyBudget then
      syncBudgetHints(dailySpent, totalSpent)
      return 0  -- 일일 예산 초과
    end
    
    -- 총 예산 검증 (totalBudget이 "null"이 아닌 경우만)
    if totalBudgetStr ~= "null" then
      local totalBudget = tonumber(totalBudgetStr)
      if totalSpent + cpc > totalBudget then
        syncBudgetHints(dailySpent, totalSpent)
        return -1  -- 총 예산 초과
      end
    end
    
    -- 원자적으로 Spent 증가
    redis.call('JSON.NUMINCRBY', campaignKey, '$.dailySpent', cpc)
    redis.call('JSON.NUMINCRBY', campaignKey, '$.totalSpent', cpc)

    -- 다음 예약 가능 여부는 요청 snapshot의 cpc가 아니라 Redis의 최신
    -- maxCpc로 판단한다. campaign update와 경합해 stale hint가 생기는 것을 막는다.
    local nextDailySpent = dailySpent + cpc
    local nextTotalSpent = totalSpent + cpc
    syncBudgetHints(nextDailySpent, nextTotalSpent)
    
    return 1  -- 성공
  `;

// 순위가 확정된 후보 window에서 첫 예산 가능 후보 1개만 원자적으로 선점
// KEYS[1] = daily exhausted set
// KEYS[2] = total exhausted set
// KEYS[3..] = campaign:{id}
// ARGV[1..N] = 후보 cpc
// ARGV[N+1..2N] = 후보 campaignId
// 반환값 = {성공한 1-based index(없으면 0), 실제 검사한 후보 수}
export const REDIS_RESERVE_FIRST_AVAILABLE_SCRIPT = `
  local dailyExhaustedKey = KEYS[1]
  local totalExhaustedKey = KEYS[2]
  local candidateCount = #KEYS - 2

  for i = 1, candidateCount do
    local campaignKey = KEYS[i + 2]
    local cpc = tonumber(ARGV[i])
    local campaignId = ARGV[candidateCount + i]
    local statusRaw = redis.call('JSON.GET', campaignKey, '$.status')
    local dailyBudgetRaw = redis.call('JSON.GET', campaignKey, '$.dailyBudget')
    local totalBudgetRaw = redis.call('JSON.GET', campaignKey, '$.totalBudget')
    local dailySpentRaw = redis.call('JSON.GET', campaignKey, '$.dailySpent')
    local totalSpentRaw = redis.call('JSON.GET', campaignKey, '$.totalSpent')
    local maxCpcRaw = redis.call('JSON.GET', campaignKey, '$.maxCpc')

    if statusRaw and string.find(statusRaw, 'ACTIVE', 1, true)
      and dailyBudgetRaw and totalBudgetRaw and dailySpentRaw and totalSpentRaw and maxCpcRaw then
      local dailyBudget = tonumber(string.match(dailyBudgetRaw, '%[([%d%.]+)%]'))
      local dailySpent = tonumber(string.match(dailySpentRaw, '%[([%d%.]+)%]')) or 0
      local totalSpent = tonumber(string.match(totalSpentRaw, '%[([%d%.]+)%]')) or 0
      local totalBudget = tonumber(string.match(totalBudgetRaw, '%[([%d%.]+)%]'))
      local maxCpc = tonumber(string.match(maxCpcRaw, '%[([%d%.]+)%]')) or cpc
      local dailyEligible = dailyBudget and dailySpent + cpc <= dailyBudget
      local totalEligible = not totalBudget or totalSpent + cpc <= totalBudget

      if dailyEligible and totalEligible then
        redis.call('JSON.NUMINCRBY', campaignKey, '$.dailySpent', cpc)
        redis.call('JSON.NUMINCRBY', campaignKey, '$.totalSpent', cpc)

        local nextDailySpent = dailySpent + cpc
        local nextTotalSpent = totalSpent + cpc
        if nextDailySpent + maxCpc > dailyBudget then
          redis.call('SADD', dailyExhaustedKey, campaignId)
        else
          redis.call('SREM', dailyExhaustedKey, campaignId)
        end
        if totalBudget and nextTotalSpent + maxCpc > totalBudget then
          redis.call('SADD', totalExhaustedKey, campaignId)
        else
          redis.call('SREM', totalExhaustedKey, campaignId)
        end
        return {i, i}
      end

      if dailySpent + maxCpc > dailyBudget then
        redis.call('SADD', dailyExhaustedKey, campaignId)
      else
        redis.call('SREM', dailyExhaustedKey, campaignId)
      end
      if totalBudget and totalSpent + maxCpc > totalBudget then
        redis.call('SADD', totalExhaustedKey, campaignId)
      else
        redis.call('SREM', totalExhaustedKey, campaignId)
      end
    end
  end

  return {0, candidateCount}
`;

// Lua Script: 원자적 Spent 감소 (롤백용)
// KEYS[1] = campaign:{id}
// KEYS[2] = daily exhausted set
// KEYS[3] = total exhausted set
// ARGV[1] = cpc (감소할 금액, 양수로 전달)
// ARGV[2] = campaignId
//
// 반환값:
// 1 = 성공 (Spent 감소됨)
// 0 = 음수 방지 (dailySpent가 음수가 될 뻔함)
// -1 = 음수 방지 (totalSpent가 음수가 될 뻔함)
// -99 = 캠페인 없음
export const REDIS_DECREMENT_SPENT_SCRIPT = `
  local campaignKey = KEYS[1]
  local dailyExhaustedKey = KEYS[2]
  local totalExhaustedKey = KEYS[3]
  local cpc = tonumber(ARGV[1])
  local campaignId = ARGV[2]
  
  -- 현재 spent 값 조회
  local dailySpentRaw = redis.call('JSON.GET', campaignKey, '$.dailySpent')
  local totalSpentRaw = redis.call('JSON.GET', campaignKey, '$.totalSpent')
  
  if not dailySpentRaw or not totalSpentRaw then
    return -99  -- 캠페인 없음
  end
  
  -- JSON 배열 파싱
  local dailySpent = tonumber(string.match(dailySpentRaw, '%[(%d+)%]')) or 0
  local totalSpent = tonumber(string.match(totalSpentRaw, '%[(%d+)%]')) or 0
  
  -- 음수 방지 검증 (일일)
  if dailySpent - cpc < 0 then
    return 0  -- 일일 Spent가 음수가 될 수 없음
  end
  
  -- 음수 방지 검증 (총)
  if totalSpent - cpc < 0 then
    return -1  -- 총 Spent가 음수가 될 수 없음
  end
  
  -- 원자적으로 Spent 감소
  redis.call('JSON.NUMINCRBY', campaignKey, '$.dailySpent', -cpc)
  redis.call('JSON.NUMINCRBY', campaignKey, '$.totalSpent', -cpc)
  -- rollback으로 다시 집행 가능해질 수 있다. false-negative hint는 다음
  -- reserve가 재등록하며, stale false-positive로 캠페인을 막지 않는다.
  redis.call('SREM', dailyExhaustedKey, campaignId)
  redis.call('SREM', totalExhaustedKey, campaignId)
  
  return 1  -- 성공
`;

// 일일 spent와 일일 소진 hint를 하나의 Redis transaction으로 초기화한다.
// KEYS[1] = campaign:{id}
// KEYS[2] = daily exhausted set
// ARGV[1] = campaignId
// ARGV[2] = reset timestamp ISO string
export const REDIS_RESET_DAILY_SPENT_SCRIPT = `
  local campaignKey = KEYS[1]
  if redis.call('EXISTS', campaignKey) == 0 then
    return 0
  end

  redis.call('JSON.SET', campaignKey, '$.dailySpent', '0')
  redis.call('JSON.SET', campaignKey, '$.lastResetDate', ARGV[2])
  redis.call('SREM', KEYS[2], ARGV[1])
  return 1
`;

// Phase 1C: auction 멱등 예약.
// KEYS[1] reservation key, [2] expiration zset, [3] daily reserved hash,
// [4] total reserved hash, [5]/[6] exhausted sets, [7..] campaign JSON keys.
// ARGV[1] fingerprint, [2] auctionId, [3] blogId, [4] budgetDate,
// [5] expiresAt(ms), [6] result TTL(sec), [7..N] cpc, [7+N..] campaignId.
// return: {code, campaignId, attemptedCount, reservationJson}
// code 1=new, 2=replay, -2=fingerprint conflict, 0=exhausted
export const REDIS_RESERVE_AUCTION_SCRIPT = `
  local existingRaw = redis.call('GET', KEYS[1])
  if existingRaw then
    local existing = cjson.decode(existingRaw)
    if existing.requestFingerprint ~= ARGV[1] then
      return {-2, existing.campaignId or '', 0, existingRaw}
    end
    return {2, existing.campaignId or '', 0, existingRaw}
  end

  local candidateCount = #KEYS - 6
  local redisTime = redis.call('TIME')
  local nowMs = tonumber(redisTime[1]) * 1000 + math.floor(tonumber(redisTime[2]) / 1000)
  local function firstValue(document, path)
    local values = document[path]
    if not values or #values == 0 or values[1] == cjson.null then
      return nil
    end
    return values[1]
  end

  for i = 1, candidateCount do
    local campaignKey = KEYS[i + 6]
    local cpc = tonumber(ARGV[6 + i])
    local campaignId = ARGV[6 + candidateCount + i]
    local campaignRaw = redis.call(
      'JSON.GET',
      campaignKey,
      '$.status',
      '$.dailyBudget',
      '$.totalBudget',
      '$.dailySpent',
      '$.totalSpent',
      '$.maxCpc'
    )

    if campaignRaw then
      local campaign = cjson.decode(campaignRaw)
      local status = firstValue(campaign, '$.status')
      local dailyBudget = tonumber(firstValue(campaign, '$.dailyBudget'))
      local totalBudget = tonumber(firstValue(campaign, '$.totalBudget'))
      local dailySpent = tonumber(firstValue(campaign, '$.dailySpent')) or 0
      local totalSpent = tonumber(firstValue(campaign, '$.totalSpent')) or 0
      local maxCpc = tonumber(firstValue(campaign, '$.maxCpc')) or cpc
      local dailyReserved = tonumber(redis.call('HGET', KEYS[3], campaignId)) or 0
      local totalReserved = tonumber(redis.call('HGET', KEYS[4], campaignId)) or 0
      local dailyEligible = status == 'ACTIVE'
        and dailyBudget
        and dailySpent + dailyReserved + cpc <= dailyBudget
      local totalEligible = status == 'ACTIVE'
        and (not totalBudget or totalSpent + totalReserved + cpc <= totalBudget)

      if dailyEligible and totalEligible then
        local nextDailyReserved = redis.call('HINCRBYFLOAT', KEYS[3], campaignId, cpc)
        local nextTotalReserved = redis.call('HINCRBYFLOAT', KEYS[4], campaignId, cpc)
        local reservation = {
          auctionId = ARGV[2],
          requestFingerprint = ARGV[1],
          campaignId = campaignId,
          blogId = tonumber(ARGV[3]),
          reservedAmount = cpc,
          budgetDate = ARGV[4],
          status = 'RESERVED',
          createdAt = nowMs,
          updatedAt = nowMs,
          expiresAt = tonumber(ARGV[5])
        }
        local reservationJson = cjson.encode(reservation)
        redis.call('SETEX', KEYS[1], tonumber(ARGV[6]), reservationJson)
        redis.call('ZADD', KEYS[2], tonumber(ARGV[5]), ARGV[2])

        if dailySpent + tonumber(nextDailyReserved) + maxCpc > dailyBudget then
          redis.call('SADD', KEYS[5], campaignId)
        else
          redis.call('SREM', KEYS[5], campaignId)
        end
        if totalBudget and totalSpent + tonumber(nextTotalReserved) + maxCpc > totalBudget then
          redis.call('SADD', KEYS[6], campaignId)
        else
          redis.call('SREM', KEYS[6], campaignId)
        end
        return {1, campaignId, i, reservationJson}
      end

      if status == 'ACTIVE' and dailyBudget
        and dailySpent + dailyReserved + maxCpc > dailyBudget then
        redis.call('SADD', KEYS[5], campaignId)
      end
      if status == 'ACTIVE' and totalBudget
        and totalSpent + totalReserved + maxCpc > totalBudget then
        redis.call('SADD', KEYS[6], campaignId)
      end
    end
  end
  return {0, '', candidateCount, ''}
`;

// KEYS: reservation, expiration zset, daily reserved hash, total reserved hash,
// daily/total exhausted set, campaign JSON, click abuse dedup key.
// ARGV: auctionId, currentBudgetDate, terminal TTL(sec), now(ms), click dedup TTL(sec)
export const REDIS_COMMIT_AUCTION_SCRIPT = `
  local raw = redis.call('GET', KEYS[1])
  if not raw then return {-99, ''} end
  local reservation = cjson.decode(raw)
  if reservation.status == 'COMMITTED' then return {2, raw} end
  if reservation.status == 'RELEASED' then return {-1, raw} end

  local amount = tonumber(reservation.reservedAmount)
  local campaignId = reservation.campaignId
  local function decrementHash(hashKey)
    local nextValue = tonumber(redis.call('HINCRBYFLOAT', hashKey, campaignId, -amount)) or 0
    if nextValue <= 0.000001 then redis.call('HDEL', hashKey, campaignId) end
  end

  local function refreshExhaustedHints()
    local values = redis.call('JSON.GET', KEYS[7],
      '$.dailyBudget', '$.totalBudget', '$.dailySpent', '$.totalSpent', '$.maxCpc')
    local decoded = values and cjson.decode(values) or {}
    local dailyBudget = decoded['$.dailyBudget'] and tonumber(decoded['$.dailyBudget'][1])
    local totalBudget = decoded['$.totalBudget'] and tonumber(decoded['$.totalBudget'][1])
    local dailySpent = decoded['$.dailySpent'] and tonumber(decoded['$.dailySpent'][1]) or 0
    local totalSpent = decoded['$.totalSpent'] and tonumber(decoded['$.totalSpent'][1]) or 0
    local maxCpc = decoded['$.maxCpc'] and tonumber(decoded['$.maxCpc'][1]) or amount
    local dailyReserved = tonumber(redis.call('HGET', KEYS[3], campaignId)) or 0
    local totalReserved = tonumber(redis.call('HGET', KEYS[4], campaignId)) or 0

    if dailyBudget and dailySpent + dailyReserved + maxCpc > dailyBudget then
      redis.call('SADD', KEYS[5], campaignId)
    else
      redis.call('SREM', KEYS[5], campaignId)
    end
    if totalBudget and totalSpent + totalReserved + maxCpc > totalBudget then
      redis.call('SADD', KEYS[6], campaignId)
    else
      redis.call('SREM', KEYS[6], campaignId)
    end
  end

  local function releaseReservation(code)
    decrementHash(KEYS[3])
    decrementHash(KEYS[4])
    refreshExhaustedHints()
    reservation.status = 'RELEASED'
    reservation.updatedAt = tonumber(ARGV[4])
    local releasedRaw = cjson.encode(reservation)
    redis.call('SETEX', KEYS[1], tonumber(ARGV[3]), releasedRaw)
    redis.call('ZREM', KEYS[2], ARGV[1])
    return {code, releasedRaw}
  end

  if reservation.budgetDate ~= ARGV[2] then
    return releaseReservation(-2)
  end

  if redis.call('EXISTS', KEYS[8]) == 1 then
    return releaseReservation(-3)
  end

  decrementHash(KEYS[3])
  decrementHash(KEYS[4])
  redis.call('JSON.NUMINCRBY', KEYS[7], '$.dailySpent', amount)
  redis.call('JSON.NUMINCRBY', KEYS[7], '$.totalSpent', amount)
  reservation.status = 'COMMITTED'
  reservation.updatedAt = tonumber(ARGV[4])
  local committedRaw = cjson.encode(reservation)
  redis.call('SETEX', KEYS[1], tonumber(ARGV[3]), committedRaw)
  redis.call('ZREM', KEYS[2], ARGV[1])
  redis.call('SETEX', KEYS[8], tonumber(ARGV[5]), ARGV[1])
  return {1, committedRaw}
`;

// ARGV: auctionId, terminal TTL(sec), now(ms)
export const REDIS_RELEASE_AUCTION_SCRIPT = `
  local raw = redis.call('GET', KEYS[1])
  if not raw then return {-99, ''} end
  local reservation = cjson.decode(raw)
  if reservation.status == 'COMMITTED' then return {-1, raw} end
  if reservation.status == 'RELEASED' then return {2, raw} end

  local amount = tonumber(reservation.reservedAmount)
  local campaignId = reservation.campaignId
  local function decrementHash(hashKey)
    local nextValue = tonumber(redis.call('HINCRBYFLOAT', hashKey, campaignId, -amount)) or 0
    if nextValue <= 0.000001 then redis.call('HDEL', hashKey, campaignId) end
  end
  decrementHash(KEYS[3])
  decrementHash(KEYS[4])

  local dailyBudgetRaw = redis.call('JSON.GET', KEYS[7], '$.dailyBudget')
  local totalBudgetRaw = redis.call('JSON.GET', KEYS[7], '$.totalBudget')
  local dailySpentRaw = redis.call('JSON.GET', KEYS[7], '$.dailySpent')
  local totalSpentRaw = redis.call('JSON.GET', KEYS[7], '$.totalSpent')
  local maxCpcRaw = redis.call('JSON.GET', KEYS[7], '$.maxCpc')
  local dailyBudget = dailyBudgetRaw and tonumber(string.match(dailyBudgetRaw, '%[([%d%.]+)%]'))
  local totalBudget = totalBudgetRaw and tonumber(string.match(totalBudgetRaw, '%[([%d%.]+)%]'))
  local dailySpent = dailySpentRaw and tonumber(string.match(dailySpentRaw, '%[([%d%.]+)%]')) or 0
  local totalSpent = totalSpentRaw and tonumber(string.match(totalSpentRaw, '%[([%d%.]+)%]')) or 0
  local maxCpc = maxCpcRaw and tonumber(string.match(maxCpcRaw, '%[([%d%.]+)%]')) or amount
  local dailyReserved = tonumber(redis.call('HGET', KEYS[3], campaignId)) or 0
  local totalReserved = tonumber(redis.call('HGET', KEYS[4], campaignId)) or 0

  if dailyBudget and dailySpent + dailyReserved + maxCpc > dailyBudget then
    redis.call('SADD', KEYS[5], campaignId)
  else
    redis.call('SREM', KEYS[5], campaignId)
  end
  if totalBudget and totalSpent + totalReserved + maxCpc > totalBudget then
    redis.call('SADD', KEYS[6], campaignId)
  else
    redis.call('SREM', KEYS[6], campaignId)
  end

  reservation.status = 'RELEASED'
  reservation.updatedAt = tonumber(ARGV[3])
  local releasedRaw = cjson.encode(reservation)
  redis.call('SETEX', KEYS[1], tonumber(ARGV[2]), releasedRaw)
  redis.call('ZREM', KEYS[2], ARGV[1])
  return {1, releasedRaw}
`;
