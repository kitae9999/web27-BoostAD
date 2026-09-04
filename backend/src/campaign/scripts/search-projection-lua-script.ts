// RedisJSON is intentionally isolated to Search Redis projection scripts.

// KEYS: campaign JSON, search tombstone, campaign key set
// ARGV: incoming projection JSON, embedding model version
// return: {code, requiresEmbedding, indexReady}; code -1=stale, 0=idempotent, 1=applied
export const REDIS_APPLY_SEARCH_PROJECTION_SCRIPT = `
  local incoming = cjson.decode(ARGV[1])
  local incomingVersion = tonumber(incoming.servingVersion)
  local tombstoneVersion = tonumber(redis.call('GET', KEYS[2])) or -1
  if tombstoneVersion >= incomingVersion then return {-1, 0, 0} end

  local existingRaw = redis.call('JSON.GET', KEYS[1])
  local existing = nil
  local currentVersion = -1
  if existingRaw then
    existing = cjson.decode(existingRaw)
    currentVersion = tonumber(existing.servingVersion) or -1
  end
  if currentVersion > incomingVersion then return {-1, 0, 0} end

  if currentVersion == incomingVersion then
    local ready = existing.indexReady == true and 1 or 0
    return {0, ready == 1 and 0 or 1, ready}
  end

  local reusable = existing
    and existing.semanticHash == incoming.semanticHash
    and existing.embeddingModelVersion == ARGV[2]
    and existing.indexReady == true

  if reusable then
    incoming.embeddingDocument = existing.embeddingDocument
    incoming.embeddingModelVersion = existing.embeddingModelVersion
    incoming.indexReady = true
  else
    incoming.embeddingDocument = nil
    incoming.embeddingModelVersion = nil
    incoming.indexReady = false
  end

  redis.call('JSON.SET', KEYS[1], '$', cjson.encode(incoming))
  redis.call('SADD', KEYS[3], KEYS[1])
  redis.call('PERSIST', KEYS[1])
  redis.call('DEL', KEYS[2])
  return {1, reusable and 0 or 1, reusable and 1 or 0}
`;

// KEYS: campaign JSON, search tombstone, campaign key set
// ARGV: serving version
export const REDIS_APPLY_SEARCH_TOMBSTONE_SCRIPT = `
  local incomingVersion = tonumber(ARGV[1])
  local tombstoneVersion = tonumber(redis.call('GET', KEYS[2])) or -1
  local existingVersion = -1
  local existingRaw = redis.call('JSON.GET', KEYS[1])
  if existingRaw then
    existingVersion = tonumber(cjson.decode(existingRaw).servingVersion) or -1
  end
  local currentVersion = math.max(tombstoneVersion, existingVersion)
  if currentVersion > incomingVersion then return -1 end
  if tombstoneVersion == incomingVersion and not existingRaw then return 0 end
  redis.call('DEL', KEYS[1])
  redis.call('SREM', KEYS[3], KEYS[1])
  redis.call('SET', KEYS[2], tostring(incomingVersion))
  return 1
`;

// KEYS: campaign JSON, search tombstone
// ARGV: servingVersion, semanticHash, embedding payload JSON
export const REDIS_APPLY_SEARCH_EMBEDDING_SCRIPT = `
  local expectedVersion = tonumber(ARGV[1])
  local tombstoneVersion = tonumber(redis.call('GET', KEYS[2])) or -1
  if tombstoneVersion >= expectedVersion then return 0 end
  local raw = redis.call('JSON.GET', KEYS[1])
  if not raw then return 0 end
  local campaign = cjson.decode(raw)
  if tonumber(campaign.servingVersion) ~= expectedVersion then return 0 end
  if campaign.semanticHash ~= ARGV[2] then return 0 end
  local embedding = cjson.decode(ARGV[3])
  campaign.embeddingDocument = embedding.document
  campaign.embeddingModelVersion = embedding.modelVersion
  campaign.indexReady = true
  redis.call('JSON.SET', KEYS[1], '$', cjson.encode(campaign))
  redis.call('PERSIST', KEYS[1])
  return 1
`;
