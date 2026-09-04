export const SEARCH_REDIS_CLIENT = Symbol('SEARCH_REDIS_CLIENT');
export const BUDGET_REDIS_CLIENT = Symbol('BUDGET_REDIS_CLIENT');
export const QUEUE_REDIS_CLIENT = Symbol('QUEUE_REDIS_CLIENT');

/**
 * Transitional alias for cache code that belongs to the Search Redis role.
 * New code should inject the role-specific token directly.
 */
export const IOREDIS_CLIENT = SEARCH_REDIS_CLIENT;
