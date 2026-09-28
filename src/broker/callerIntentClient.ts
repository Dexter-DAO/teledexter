import { createPrimaryApiTransport, PrimaryApiError, type PrimaryApiOptions } from './primaryApi.js';
import { z } from 'zod';
import { callerIntentInputSchema, callerIntentV2InputSchema, callerIntentInputDigest, validateCallerIntentResponse,
  callerIntentV3InputSchema, callerIntentV3TaskSchema, callerIntentV3TaskCoverageSchema,
  type CallerIntentInput, type CallerIntentV2Input, type CallerIntentV3Input, type CallerIntentResponse } from './callerIntentContract.js';

export interface CallerIntentClient {
  submit(bindingId: string, operationId: string, input: CallerIntentV2Input | CallerIntentV3Input, signal?: AbortSignal): Promise<CallerIntentResponse>;
  taskCandidates?(bindingId: string, source: CallerIntentV3Input['source'], signal?: AbortSignal): Promise<Pick<CallerIntentV3Input,'tasks'|'taskCoverage'> | null>;
  recover(bindingId: string, operationId: string, input: CallerIntentInput, signal?: AbortSignal): Promise<CallerIntentResponse>;
}

/** One HTTP operation per method. The durable host decides POST versus exact
 * GET; this client has no fallback submission, model contact, or task dispatch. */
export function createPrimaryCallerIntentClient(options: PrimaryApiOptions): CallerIntentClient {
  const request = createPrimaryApiTransport(options);
  const perform = async (method: 'GET' | 'POST', bindingId: string, operationId: string,
    value: CallerIntentInput, signal?: AbortSignal): Promise<CallerIntentResponse> => {
    let input: CallerIntentInput;
    try {
      input = method === 'POST' ? z.union([callerIntentV3InputSchema,callerIntentV2InputSchema]).parse(value) : callerIntentInputSchema.parse(value);
      // Validates canonical path identities before constructing the URL.
      callerIntentInputDigest(operationId, bindingId, input);
    } catch { throw new PrimaryApiError('invalid_request'); }
    const headers = { 'X-Dexter-Binding-Id': bindingId, 'X-Dexter-Phone-Context-Version': 'memory-eligible-v1',
      ...(method === 'POST' ? { 'Idempotency-Key': operationId } : {}) };
    const path = `/calls/${bindingId}/caller-intents${method === 'GET' ? `/${operationId}` : ''}`;
    let validated: CallerIntentResponse | undefined;
    await request(method, path, method === 'POST' ? input : undefined, headers,
      method === 'POST' ? [200, 202] : 200, false, { signal,
        validateResponse(status, body) {
          const response = validateCallerIntentResponse(bindingId, operationId, input, body);
          if (method === 'POST' && status !== (response.state === 'resolved' ? 200 : 202))
            throw new Error('caller_intent_http_state_mismatch');
          validated = response;
        },
      });
    if (!validated) throw new PrimaryApiError('invalid_response');
    return validated;
  };
  return {
    async taskCandidates(bindingId,source,signal) {
      const parsedSource=callerIntentV3InputSchema.shape.source.parse(source);
      const schema=z.object({v:z.literal('phone.caller-intent.v3'),tasks:z.array(callerIntentV3TaskSchema).max(16),taskCoverage:callerIntentV3TaskCoverageSchema}).strict();
      try {
        const result=schema.parse(await request('POST',`/calls/${bindingId}/caller-intents/candidates`,{source:parsedSource},
          {'X-Dexter-Binding-Id':bindingId,'X-Dexter-Phone-Context-Version':'memory-eligible-v1'},200,false,{signal}));
        if(result.tasks.some(task=>task.execution.bindingId!==bindingId))throw new PrimaryApiError('invalid_response');
        return {tasks:result.tasks,taskCoverage:result.taskCoverage};
      } catch(error) {
        // A predecessor API has no v3 candidate port. No v3 operation is created.
        if(error instanceof PrimaryApiError && error.status===404)return null;
        throw error;
      }
    },
    submit: (bindingId, operationId, input, signal) => perform('POST', bindingId, operationId, input, signal),
    recover: (bindingId, operationId, input, signal) => perform('GET', bindingId, operationId, input, signal),
  };
}
