// ============================================================
// Kafka Client Factory
// ============================================================

import { Kafka, logLevel as KafkaLogLevel } from 'kafkajs';
import { config } from '../config/index.js';
import { createModuleLogger } from '../shared/logger';

const log = createModuleLogger('kafka-client');

const kafkaLogLevelMap: Record<string, number> = {
    fatal: KafkaLogLevel.NOTHING,
    error: KafkaLogLevel.ERROR,
    warn: KafkaLogLevel.WARN,
    info: KafkaLogLevel.INFO,
    debug: KafkaLogLevel.DEBUG,
    trace: KafkaLogLevel.DEBUG,
};

export function createKafkaClient(): Kafka {
    const brokers = config.kafkaBrokers
        .split(',')
        .map((b) => b.trim())
        .filter((b) => b.length > 0);

    if (brokers.length === 0) {
        log.warn('No Kafka brokers configured - Kafka features will be disabled');

        return new Kafka({
            clientId: config.kafkaClientId,
            brokers: ['localhost:9092'],
            logLevel: KafkaLogLevel.WARN
        });
    }

    log.info(
        { clientId: config.kafkaClientId, brokers },
        '📡 Creating Kafka client'
    );

    return new Kafka({
        clientId: config.kafkaClientId,
        brokers,
        retry: {
            initialRetryTime: 300,
            retries: 300,
            maxRetryTime: 30000
        },
        logLevel: kafkaLogLevelMap[config.logLevel] ?? KafkaLogLevel.WARN,
        logCreator: () => {
            return ({ log: logEntry }) => {
                const { message, ...extra } = logEntry;
                log.debug({ ...extra }, `kafka: ${message}`);
            };
        }
    });
}