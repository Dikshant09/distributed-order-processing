// ============================================================
// Retry with Exponential Backoff + Jitter
// ============================================================

import { createModuleLogger } from "../shared/logger.js";

const log = createModuleLogger('retry');

export interface RetryConfig {
    maxRetries: number;
    baseDelayMs: number;
    maxDelayMs?: number;     // Cap the backoff (default: 30s)
}

export interface RetryResult {
    success: boolean;
    attempts: number;
    lastError: Error | null;
}

// ─────────────────────────────────────────────────
// Execute a function with retry + exponential backoff
// ─────────────────────────────────────────────────
export async function withRetry(
    fn: () => Promise<void>,
    retryConfig: RetryConfig,
    context?: Record<string, unknown>
): Promise<RetryResult> {
    const { maxRetries, baseDelayMs, maxDelayMs = 30000 } = retryConfig;
    let lastError: Error | null = null;

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
        try {
            await fn();
            return { success: true, attempts: attempt + 1, lastError: null };
        } catch (err) {
            lastError = err instanceof Error ? err : new Error(String(err));

            if (attempt === maxRetries) {
                log.warn(
                    { ...context, attempt: attempt + 1, maxRetries, error: lastError.message },
                    `❌ All ${maxRetries + 1} attempts failed — giving up`
                );
                break;
            }

            // Exponential backoff with full jitter: delay = random(0, min(maxDelay, baseDelay * 2^attempt))
            const exponentialDelay = Math.min(maxDelayMs, baseDelayMs * Math.pow(2, attempt));
            const jitteredDelay = Math.floor(Math.random() * exponentialDelay);

            log.debug(
                {
                    ...context,
                    attempt: attempt + 1,
                    maxRetries: maxRetries + 1,
                    delayMs: jitteredDelay,
                    error: lastError.message
                },
                `⏳ Retry ${attempt + 1}/${maxRetries + 1} in ${jitteredDelay}ms`
            );

            await sleep(jitteredDelay);
        }
    }
    return { success: false, attempts: maxRetries + 1, lastError };
}

function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}