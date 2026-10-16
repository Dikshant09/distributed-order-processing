// ============================================================
// HTTP Metrics — Fastify Request/Response Hooks
// ============================================================

import { FastifyInstance } from 'fastify';
import {
    httpRequestsTotal,
    httpRequestDurationSeconds,
    httpRequestsInFlight,
    register
} from './metrics.js';
import { config } from '../config/index.js';

// ─────────────────────────────────────────────────
// Register Metrics Hooks and Endpoint
// ─────────────────────────────────────────────────

export async function registerMetricsPlugin(app: FastifyInstance): Promise<void> {
    // GET /metrics — Prometheus scrape endpoint
    app.get(config.metricsPath, async (_request, reply) => {
        reply.header('Content-Type', register.contentType);
        return reply.send(await register.metrics());
    });

    // onRequest: Track in-flight and start timing
    app.addHook('onRequest', async (request) => {
        if (request.url === config.metricsPath || request.url === '/health') return;

        httpRequestsInFlight.inc();
        (request as any).__metricsStartTime = process.hrtime.bigint();
    });

    // onResponse: Record duration and count
    app.addHook('onResponse', async (request, reply) => {
        if (request.url === config.metricsPath || request.url === '/health') return;

        httpRequestsInFlight.dec();

        const route = request.routeOptions?.url || 'unmatched';
        const method = request.method;
        const statusCode = reply.statusCode.toString();

        httpRequestsTotal.inc({
            method,
            route,
            status_code: statusCode,
        });

        const startTime = (request as any).__metricsStartTime;
        if (startTime) {
            const durationNs = Number(process.hrtime.bigint() - startTime);
            const durationSeconds = durationNs / 1e9;

            httpRequestDurationSeconds.observe(
                { method, route, status_code: statusCode },
                durationSeconds
            );
        }
    });
}