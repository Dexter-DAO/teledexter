import { createHash, timingSafeEqual } from 'node:crypto';
import fs from 'node:fs';
import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import type { CallLogEntry } from '../types/CallEntry.js';

const ADMIN_HEADER = 'x-dexter-phone-admin-secret';

function secretDigest(value: string): Buffer {
  return createHash('sha256').update(value, 'utf8').digest();
}

export function hasValidCallLogAdminSecret(
  request: FastifyRequest,
  expectedSecret: string,
): boolean {
  if (expectedSecret.length < 32) return false;
  const header = request.headers[ADMIN_HEADER];
  const supplied = Array.isArray(header) ? header[0] : header;
  if (typeof supplied !== 'string' || !supplied) return false;
  return timingSafeEqual(secretDigest(expectedSecret), secretDigest(supplied));
}

function concealUnlessAuthorized(
  request: FastifyRequest,
  reply: FastifyReply,
  expectedSecret: string,
): boolean {
  if (hasValidCallLogAdminSecret(request, expectedSecret)) return true;
  // Conceal the administrative surface rather than confirming it exists.
  reply.status(404).send({ error: 'not_found' });
  return false;
}

export function registerCallLogRoutes(
  fastify: FastifyInstance,
  options: {
    callLogPath: string;
    adminSecret: string;
  },
): void {
  fastify.get('/calls', async (request: FastifyRequest, reply: FastifyReply) => {
    if (!concealUnlessAuthorized(request, reply, options.adminSecret)) return;
    try {
      const log: CallLogEntry[] = JSON.parse(fs.readFileSync(options.callLogPath, 'utf8'));
      const recent = log.slice(-20).reverse();
      return reply.send({
        ok: true,
        totalCalls: log.length,
        recent: recent.map((call) => ({
          caller: call.callerNumber,
          attestation: call.stirVerstat,
          direction: call.direction,
          startedAt: call.startedAt,
          duration: call.durationSeconds,
          transcriptLines: call.transcriptSegments.length,
          toolCalls: call.toolCalls.length,
        })),
      });
    } catch {
      return reply.send({ ok: true, totalCalls: 0, recent: [] });
    }
  });

  fastify.get('/calls/:index', async (request: FastifyRequest, reply: FastifyReply) => {
    if (!concealUnlessAuthorized(request, reply, options.adminSecret)) return;
    try {
      const log: CallLogEntry[] = JSON.parse(fs.readFileSync(options.callLogPath, 'utf8'));
      const idx = Number.parseInt((request.params as { index: string }).index, 10);
      if (!Number.isSafeInteger(idx) || idx < 0) {
        return reply.status(400).send({ error: 'invalid_index' });
      }
      const call = log[log.length - 1 - idx];
      if (!call) return reply.status(404).send({ error: 'call_not_found' });
      return reply.send({ ok: true, call });
    } catch {
      return reply.status(500).send({ error: 'failed_to_read_log' });
    }
  });
}
