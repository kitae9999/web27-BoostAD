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
