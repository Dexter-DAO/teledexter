import { z } from 'zod';
import type { FunctionTool } from 'openai/resources/live/live';
import type { LiveBackendChannel, LiveDelegationRequest } from './types.js';

export const restrictedSetupToolSchema = z.discriminatedUnion('name', [
  z.object({ name: z.literal('phone_setup_status'), arguments: z.object({}).strict() }).strict(),
  z.object({ name: z.literal('request_verified_callback'), arguments: z.object({ explicitlyRequested: z.literal(true) }).strict() }).strict(),
  z.object({ name: z.literal('complete_phone_setup'), arguments: z.object({ accepted: z.literal(true) }).strict() }).strict(),
  z.object({ name: z.literal('decline_phone_setup'), arguments: z.object({}).strict() }).strict(),
]);
export type RestrictedSetupTool = z.output<typeof restrictedSetupToolSchema>;
export const restrictedSetupToolResultSchema = z.object({ phase: z.enum([
  'account_authentication_required', 'verification_available', 'standing_callback_available',
  'checking', 'verification_requested', 'callback_requested', 'complete', 'declined', 'unavailable',
]) }).strict();
export type RestrictedSetupToolResult = z.output<typeof restrictedSetupToolResultSchema>;
export interface RestrictedSetupTools {
  model: string;
  execute(input: { request: LiveDelegationRequest; tool: RestrictedSetupTool; toolCallId: string },
    channel: Pick<LiveBackendChannel, 'getContext' | 'subscribeContext' | 'signal'>): Promise<RestrictedSetupToolResult>;
}
const empty = { type: 'object', properties: {}, required: [], additionalProperties: false };
export const restrictedSetupFunctionTools: FunctionTool[] = [
  { type: 'function', name: 'phone_setup_status', description: 'Check phone setup or explain its next available step.', parameters: empty, strict: true },
  { type: 'function', name: 'request_verified_callback', description: 'Request a callback only when the caller explicitly asks to continue on their verified number. The server checks prior account consent and chooses the number.',
    parameters: { type: 'object', properties: { explicitlyRequested: { type: 'boolean', enum: [true] } }, required: ['explicitlyRequested'], additionalProperties: false }, strict: true },
  { type: 'function', name: 'complete_phone_setup', description: 'Complete this verification step only after the caller explicitly accepts. The host checks the same call and protected verification input.',
    parameters: { type: 'object', properties: { accepted: { type: 'boolean', enum: [true] } }, required: ['accepted'], additionalProperties: false }, strict: true },
  { type: 'function', name: 'decline_phone_setup', description: 'Record that the caller wants to stop this setup step.', parameters: empty, strict: true },
];
export const restrictedSetupBackendInstructions = `Help with phone setup using only the supplied functions. Select a function only for the caller's explicit request; ordinary greetings need no function. Ask for clarification when their intent is uncertain.
Use phone_setup_status for status or setup guidance. request_verified_callback asks the server to check a previously permitted number; it never accepts an owner or destination from conversation. complete_phone_setup requires the caller's affirmative acceptance and the host's protected verification step. decline_phone_setup records a change of mind.
Report the returned phase plainly. A requested callback remains pending. Account approval, possession and financial permission are checked separately by the server. This session has no account balances, history or private account tools. Never request a password, recovery phrase, passkey secret or spoken verification code. Keep verification codes out of function arguments.`;
