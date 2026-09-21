export type OutboundAuthorizationDecision =
  | { kind: 'authorize_server_binding' }
  | { kind: 'accept_legacy' }
  | {
      kind: 'reject';
      status: 401 | 403;
      error: 'unauthorized' | 'recipient_not_allowed';
    };

export type AuthorizedOutboundDispatchResult<T> =
  | { kind: 'dispatched'; value: T }
  | {
      kind: 'reject';
      status: 401 | 403 | 503;
      error:
        | 'unauthorized'
        | 'recipient_not_allowed'
        | 'authorization_unavailable';
    };

export function selectOutboundAuthorization(input: {
  harkBound: boolean;
  signedProducer: boolean;
  recipientAllowlisted: boolean;
  hasInternalAuthorization: boolean;
  hasServerAuthoredBinding: boolean;
}): OutboundAuthorizationDecision {
  if (input.hasInternalAuthorization) {
    return input.hasServerAuthoredBinding
      ? { kind: 'authorize_server_binding' }
      : { kind: 'reject', status: 403, error: 'recipient_not_allowed' };
  }
  if (input.harkBound || input.hasServerAuthoredBinding) {
    return { kind: 'reject', status: 401, error: 'unauthorized' };
  }
  if (!input.signedProducer) {
    return { kind: 'reject', status: 401, error: 'unauthorized' };
  }
  return input.recipientAllowlisted
    ? { kind: 'accept_legacy' }
    : { kind: 'reject', status: 403, error: 'recipient_not_allowed' };
}

/**
 * The route-level authorization fence. Provider dispatch is deliberately a
 * callback so every rejected path is mechanically unable to reach it.
 */
export async function dispatchAuthorizedOutbound<T>(input: {
  harkBound: boolean;
  signedProducer: boolean;
  recipientAllowlisted: boolean;
  internalToken: string;
  hasServerAuthoredBinding: boolean;
  authorizeServerBinding: () => Promise<{ ok: boolean; status: number }>;
  dispatch: () => Promise<T>;
}): Promise<AuthorizedOutboundDispatchResult<T>> {
  const decision = selectOutboundAuthorization({
    harkBound: input.harkBound,
    signedProducer: input.signedProducer,
    recipientAllowlisted: input.recipientAllowlisted,
    hasInternalAuthorization: Boolean(input.internalToken),
    hasServerAuthoredBinding: input.hasServerAuthoredBinding,
  });
  if (decision.kind === 'reject') return decision;
  if (decision.kind === 'authorize_server_binding') {
    const authorized = await input.authorizeServerBinding();
    if (!authorized.ok) {
      const status = authorized.status >= 500
        ? 503
        : authorized.status === 401
          ? 401
          : 403;
      return {
        kind: 'reject',
        status,
        error: status === 503
          ? 'authorization_unavailable'
          : status === 401
            ? 'unauthorized'
            : 'recipient_not_allowed',
      };
    }
  }
  return { kind: 'dispatched', value: await input.dispatch() };
}

export function isValidHarkCallbackSigningSecret(input: {
  producerSecret: string;
  callbackSecret: string;
}): boolean {
  return input.callbackSecret.length >= 32 && input.callbackSecret !== input.producerSecret;
}

export function hasOutboundMediaTokenKey(input: {
  serverAuthored: boolean;
  legacyProducerSecret: string;
  internalApiSecret: string;
}): boolean {
  const requiredSecret = input.serverAuthored
    ? input.internalApiSecret
    : input.legacyProducerSecret;
  return requiredSecret.length >= 32;
}
