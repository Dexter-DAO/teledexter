import type { DexterApiReadinessSnapshot } from './DexterApiReadiness.js';
import type { RealtimeReadinessSnapshot } from './RealtimeReadinessGate.js';

export interface PhoneFinancialReadinessSnapshot {
  ok: boolean;
  reasons: string[];
}

export interface PhoneServiceReadinessSnapshot {
  healthOk: boolean;
  httpStatus: 200 | 503;
  notificationReady: boolean;
  conversationReady: boolean;
  notificationReasons: string[];
  conversationReasons: string[];
}

/**
 * Realtime is an enhancement to an outbound notification, not a prerequisite
 * for delivering it. API or ledger failure blocks a new dial. Realtime
 * failure keeps the service healthy in Twilio-only notification mode.
 */
export function phoneServiceReadiness(input: {
  financial: PhoneFinancialReadinessSnapshot;
  api: DexterApiReadinessSnapshot;
  realtime: RealtimeReadinessSnapshot;
}): PhoneServiceReadinessSnapshot {
  const notificationReasons = [
    ...input.financial.reasons,
    ...(input.api.status === 'ready'
      ? []
      : [input.api.reason ?? 'dexter_api_readiness_unknown']),
  ];
  const notificationReady = notificationReasons.length === 0;
  const conversationReasons = [
    ...notificationReasons,
    ...(input.realtime.status === 'ready'
      ? []
      : [input.realtime.reason ?? 'realtime_readiness_unknown']),
  ];
  const conversationReady = conversationReasons.length === 0;
  return {
    healthOk: notificationReady,
    httpStatus: notificationReady ? 200 : 503,
    notificationReady,
    conversationReady,
    notificationReasons,
    conversationReasons,
  };
}
