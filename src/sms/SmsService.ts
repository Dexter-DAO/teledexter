import { createHash, randomBytes, randomUUID } from 'node:crypto';
import fs from 'node:fs';
import { z } from 'zod';
import { bindDurableFileParent, durablyReplaceJsonFile, type DurableParentBinding } from '../outbound/DurableFile.js';

export const SMS_DISCLOSURE_VERSION = 'dexter-sms-2026-09-16';
export const SMS_DISCLOSURE = 'I agree to receive SMS from Dexter about my requests, follow-ups, service links, and task and account activity. Message frequency varies. Message and data rates may apply. Reply HELP for help or STOP to opt out. Consent is optional and is not a condition of purchase.';
export const SMS_TERMS_URL = 'https://dexter.cash/terms#sms';
export const SMS_PRIVACY_URL = 'https://dexter.cash/privacy';
const MINUTE = 60_000;
const DAY = 24 * 60 * MINUTE;
export const SMS_TURN_MAX_AGE_MS = 10 * MINUTE;
export const phoneSchema = z.string().regex(/^\+1[2-9]\d{2}[2-9]\d{6}$/);
const iso = z.string().datetime();
const consentSchema = z.object({
  phone: phoneSchema, enrolled: z.boolean(), carrierOptedOut: z.boolean(),
  disclosureVersion: z.string().nullable(), updatedAt: iso,
  verifiedAt: iso.nullable(), withdrawnAt: iso.nullable(),
  signupReplyAttempted: z.boolean().optional(),
});
const challengeSchema = z.object({ id: z.string(), phone: phoneSchema, client: z.string(), created: z.number(), expires: z.number(), attempts: z.number().int().min(0), state: z.enum(['pending', 'sent', 'checking', 'used', 'failed']), verified: z.number().optional() });
const proofSchema = z.object({ hash: z.string(), phone: phoneSchema, client: z.string(), verified: z.number(), expires: z.number() });
const eventSchema = z.object({ id: z.string(), phone: phoneSchema, type: z.string(), at: iso, disclosureVersion: z.string().nullable(), disclosure: z.string().nullable(), client: z.string().nullable(), verifiedAt: iso.nullable() });
const messageSchema = z.object({ id: z.string(), phone: phoneSchema, callSid: z.string(), bodyHash: z.string(), created: z.number(), sid: z.string().nullable(), status: z.string(), errorCode: z.string().nullable() });
const stateSchema = z.object({
  version: z.literal(1), consents: z.array(consentSchema), challenges: z.array(challengeSchema), proofs: z.array(proofSchema),
  events: z.array(eventSchema), messages: z.array(messageSchema),
  inbox: z.array(z.object({ sid: z.string(), phone: phoneSchema, body: z.string(), received: z.number(), retries: z.number().int().min(0).optional(), retryAt: z.number().optional(), state: z.enum(['pending_dispatch', 'pending_signup', 'replied', 'suppressed', 'uncertain']) })),
  rates: z.array(z.object({ key: z.string(), at: z.number() })),
  webhooks: z.array(z.object({ id: z.string(), at: z.number() })),
});
type State = z.infer<typeof stateSchema>;
type Consent = z.infer<typeof consentSchema>;

export class SmsError extends Error {
  constructor(public code: string, public status = 400, public retryAfterSeconds?: number) { super(code); }
}
export interface SmsProvider {
  startVerification(phone: string): Promise<void>;
  checkVerification(phone: string, code: string): Promise<boolean>;
  send(input: { to: string; body: string; messagingServiceSid: string; statusCallback: string }): Promise<{ sid: string; status: string }>;
}
export interface SmsOptions {
  statePath: string;
  enrollmentEnabled: boolean;
  sendEnabled: boolean;
  campaignApproved: boolean;
  advancedOptOutConfigured: boolean;
  messagingServiceSid: string;
  publicBaseUrl: string;
  provider: SmsProvider;
  consentRetentionDays?: number;
  messageRetentionDays?: number;
  now?: () => number;
}
const hash = (text: string) => createHash('sha256').update(text).digest('hex');

/** Single-writer ledger. Writes are fsynced before a provider action or success response. */
export class SmsService {
  private state: State;
  private parent: DurableParentBinding;
  private poisoned = false;
  private now: () => number;
  private lockFd: number;
  private lockPath: string;
  private onExit = () => this.close();
  constructor(private options: SmsOptions) {
    if (![options.consentRetentionDays ?? 1461, options.messageRetentionDays ?? 90].every(x => Number.isInteger(x) && x > 0)) throw new Error('invalid_sms_retention');
    this.now = options.now ?? Date.now;
    this.parent = bindDurableFileParent(options.statePath);
    this.lockPath = `${options.statePath}.lock`;
    const acquire = () => fs.openSync(this.lockPath, 'wx', 0o600);
    try { this.lockFd = acquire(); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error;
      const previous = JSON.parse(fs.readFileSync(this.lockPath, 'utf8')) as { pid: number };
      if (!Number.isInteger(previous.pid) || previous.pid < 1) throw new Error('Invalid SMS writer lock');
      try { process.kill(previous.pid, 0); throw error; }
      catch (probe) { if ((probe as NodeJS.ErrnoException).code !== 'ESRCH') throw probe; }
      fs.unlinkSync(this.lockPath);
      this.lockFd = acquire();
    }
    try {
      fs.writeFileSync(this.lockFd, JSON.stringify({ pid: process.pid, acquiredAt: new Date(this.now()).toISOString() }));
      fs.fsyncSync(this.lockFd);
      this.state = fs.existsSync(options.statePath)
        ? stateSchema.parse(JSON.parse(fs.readFileSync(options.statePath, 'utf8')))
        : { version: 1, consents: [], challenges: [], proofs: [], events: [], messages: [], inbox: [], rates: [], webhooks: [] };
      this.change(s => {
        // A previous owner may have exited after dispatch but before receiving the result.
        for (const m of s.messages) if (m.status === 'dispatching') m.status = 'uncertain';
      });
      process.once('exit', this.onExit);
    } catch (e) { this.close(); throw e; }
  }
  close() {
    process.removeListener('exit', this.onExit);
    if (this.lockFd < 0) return;
    try {
      const held = fs.fstatSync(this.lockFd); const current = fs.lstatSync(this.lockPath);
      if (held.ino === current.ino && held.dev === current.dev) fs.unlinkSync(this.lockPath);
    } catch {}
    try { fs.closeSync(this.lockFd); } catch {}
    this.lockFd = -1;
    this.poisoned = true;
  }
  config() {
    return { ok: true, enrollmentAvailable: this.options.enrollmentEnabled && !this.poisoned, deliveryActive: this.deliveryActive(), disclosureVersion: SMS_DISCLOSURE_VERSION };
  }
  private deliveryActive() {
    return !this.poisoned && this.options.sendEnabled && this.options.campaignApproved && this.options.advancedOptOutConfigured && /^MG[0-9a-f]{32}$/i.test(this.options.messagingServiceSid) && this.options.publicBaseUrl.startsWith('https://');
  }
  private change<T>(action: (s: State) => T): T {
    if (this.poisoned) throw new SmsError('sms_unavailable', 503);
    const next = structuredClone(this.state);
    const now = this.now();
    next.challenges = next.challenges.filter(x => x.expires > now);
    next.proofs = next.proofs.filter(x => x.expires > now);
    next.rates = next.rates.filter(x => x.at > now - DAY);
    const messageDays = this.options.messageRetentionDays ?? 90;
    const consentDays = this.options.consentRetentionDays ?? 1461;
    next.messages = next.messages.filter(x => x.created > now - messageDays * DAY);
    next.inbox = next.inbox.filter(x => x.received > now - DAY);
    for (const item of next.inbox) if (!['pending_dispatch', 'pending_signup'].includes(item.state)) item.body = '';
    next.webhooks = next.webhooks.filter(x => x.at > now - 90 * DAY);
    // Expire withdrawn consent evidence; retain minimal suppression until recipient resubscribes.
    const expired = new Set(next.consents.filter(x => !x.enrolled && x.withdrawnAt && Date.parse(x.withdrawnAt) < now - consentDays * DAY).map(x => x.phone));
    for (const c of next.consents) if (expired.has(c.phone)) { c.disclosureVersion = null; c.verifiedAt = null; }
    next.events = next.events.filter(x => !expired.has(x.phone));
    const result = action(next);
    try { durablyReplaceJsonFile(this.options.statePath, JSON.stringify(next), fs, this.parent); }
    catch { this.poisoned = true; throw new SmsError('sms_unavailable', 503); }
    this.state = next;
    return result;
  }
  private requireEnrollment() {
    if (!this.config().enrollmentAvailable) throw new SmsError('sms_unavailable', 503);
  }
  private phone(value: string) {
    const p = phoneSchema.safeParse(value);
    if (!p.success) throw new SmsError('invalid_request');
    return p.data;
  }
  private limit(s: State, key: string, max: number, period: number) {
    const entries = s.rates.filter(x => x.key === key && x.at > this.now() - period);
    if (entries.length >= max) throw new SmsError('rate_limited', 429, Math.max(1, Math.ceil((entries[0].at + period - this.now()) / 1000)));
  }
  async start(phoneValue: string, client: string) {
    this.requireEnrollment();
    const phone = this.phone(phoneValue);
    const now = this.now();
    const id = randomUUID();
    this.change(s => {
      this.limit(s, `start:phone:${hash(phone)}`, 1, MINUTE);
      this.limit(s, `start:phone:${hash(phone)}`, 3, 60 * MINUTE);
      this.limit(s, `start:client:${client}`, 10, 60 * MINUTE);
      this.limit(s, 'start:global', 100, 60 * MINUTE);
      for (const key of [`start:phone:${hash(phone)}`, `start:client:${client}`, 'start:global']) s.rates.push({ key, at: now });
      s.challenges.push({ id, phone, client, created: now, expires: now + 10 * MINUTE, attempts: 0, state: 'pending' });
    });
    try { await this.options.provider.startVerification(phone); }
    catch {
      this.change(s => { const c = s.challenges.find(x => x.id === id); if (c) c.state = 'failed'; });
      throw new SmsError('verification_unavailable', 502);
    }
    this.change(s => { const c = s.challenges.find(x => x.id === id); if (c) c.state = 'sent'; });
    return { ok: true, challengeId: id, expiresAt: new Date(now + 10 * MINUTE).toISOString(), resendAfterSeconds: 60 };
  }
  async check(challengeId: string, code: string, client: string) {
    this.requireEnrollment();
    if (!/^\d{6}$/.test(code)) throw new SmsError('invalid_request');
    const c = this.change(s => {
      this.limit(s, `check:client:${client}`, 30, 60 * MINUTE);
      const c = s.challenges.find(x => x.id === challengeId && x.client === client);
      if (!c || c.state !== 'sent' || c.attempts >= 5) throw new SmsError('invalid_verification', 400);
      c.attempts++; c.state = 'checking';
      s.rates.push({ key: `check:client:${client}`, at: this.now() });
      return structuredClone(c);
    });
    let approved = false;
    try { approved = await this.options.provider.checkVerification(c.phone, code); }
    catch {
      this.change(s => { const c = s.challenges.find(x => x.id === challengeId); if (c) c.state = 'sent'; });
      throw new SmsError('verification_unavailable', 502);
    }
    const proofToken = randomBytes(32).toString('base64url');
    const now = this.now();
    this.change(s => {
      const c = s.challenges.find(x => x.id === challengeId);
      if (!c || c.state !== 'checking') throw new SmsError('invalid_verification');
      c.state = approved ? 'used' : 'sent';
      if (approved) c.verified = now;
      if (approved) s.proofs.push({ hash: hash(proofToken), phone: c.phone, client, verified: now, expires: now + 15 * MINUTE });
    });
    if (!approved) throw new SmsError('invalid_verification');
    return { ok: true, proofToken, expiresAt: new Date(now + 15 * MINUTE).toISOString(), status: this.status(proofToken, client) };
  }
  private proof(token: string, client: string) {
    if (!/^[A-Za-z0-9_-]{43}$/.test(token)) throw new SmsError('invalid_proof', 401);
    const proof = this.state.proofs.find(x => x.hash === hash(token) && x.client === client);
    if (!proof || proof.expires <= this.now()) throw new SmsError('proof_expired', 401);
    return proof;
  }
  /** Internal owner-bound account enrollment; these results never grant SMS consent. */
  verifiedPhone(token: string, client: string) {
    const proof = this.proof(token, client);
    return { phone: proof.phone, verifiedAt: new Date(proof.verified).toISOString() };
  }
  verifiedChallenge(challengeId: string, client: string) {
    const c = this.state.challenges.find(value => value.id === challengeId && value.client === client
      && value.state === 'used' && value.verified !== undefined && value.expires > this.now());
    return c ? { phone: c.phone, verifiedAt: new Date(c.verified!).toISOString() } : null;
  }
  private summary(phone: string) {
    const c = this.state.consents.find(x => x.phone === phone);
    return { enrolled: c?.enrolled ?? false, carrierOptedOut: c?.carrierOptedOut ?? false, disclosureVersion: c?.disclosureVersion ?? null, updatedAt: c?.updatedAt ?? null, deliveryActive: this.deliveryActive(), maskedPhone: `+1 *** *** ${phone.slice(-4)}` };
  }
  status(token: string, client: string) { return this.summary(this.proof(token, client).phone); }
  private record(s: State, phone: string): Consent {
    let c = s.consents.find(x => x.phone === phone);
    if (!c) {
      c = { phone, enrolled: false, carrierOptedOut: false, disclosureVersion: null, updatedAt: new Date(this.now()).toISOString(), verifiedAt: null, withdrawnAt: new Date(this.now()).toISOString() };
      s.consents.push(c);
    }
    return c;
  }
  async enroll(token: string, client: string, consent: boolean, version: string) {
    this.requireEnrollment();
    const proof = this.proof(token, client);
    if (consent !== true || version !== SMS_DISCLOSURE_VERSION) throw new SmsError('invalid_consent');
    const enrollmentId = this.change(s => {
      const c = this.record(s, proof.phone);
      if (c.carrierOptedOut) throw new SmsError('carrier_opted_out', 409);
      if (c.enrolled && c.disclosureVersion === version) return null;
      const at = new Date(this.now()).toISOString();
      c.enrolled = true; c.disclosureVersion = version; c.updatedAt = at; c.verifiedAt = new Date(proof.verified).toISOString(); c.withdrawnAt = null;
      const id = randomUUID();
      s.events.push({ id, phone: c.phone, type: 'web_enroll', at, disclosureVersion: version, disclosure: `${SMS_DISCLOSURE} Terms: ${SMS_TERMS_URL} Privacy: ${SMS_PRIVACY_URL} Source: https://dexter.cash/sms`, client, verifiedAt: c.verifiedAt });
      return id;
    });
    if (enrollmentId && this.deliveryActive()) {
      // Consent remains recorded if a welcome message cannot be delivered.
      try { await this.sendMessage(proof.phone, `enrollment:${enrollmentId}`, 'You are subscribed for requests, follow-ups, service links, and task and account activity. Message frequency varies. Message and data rates may apply.'); } catch {}
    }
    return this.summary(proof.phone);
  }
  withdraw(token: string, client: string) {
    const proof = this.proof(token, client);
    this.change(s => {
      const c = this.record(s, proof.phone); const at = new Date(this.now()).toISOString();
      c.enrolled = false; c.updatedAt = at; c.withdrawnAt = at;
      s.events.push({ id: randomUUID(), phone: c.phone, type: 'web_withdraw', at, disclosureVersion: c.disclosureVersion, disclosure: null, client, verifiedAt: new Date(proof.verified).toISOString() });
    });
    return this.summary(proof.phone);
  }
  inbound(input: { phone: string; sid: string; body: string; optOutType?: string }) {
    if (!input.body.trim() || /[\x00-\x08\x0b\x0c\x0e-\x1f]/.test(input.body)) throw new SmsError('invalid_request');
    const phone = this.phone(input.phone);
    const keyword = input.body.trim().toUpperCase();
    const type = input.optOutType || (['STOP', 'STOPALL', 'UNSUBSCRIBE', 'CANCEL', 'END', 'QUIT', 'REVOKE', 'OPTOUT'].includes(keyword) ? 'STOP' : ['START', 'UNSTOP', 'YES'].includes(keyword) ? 'START' : ['HELP', 'INFO'].includes(keyword) ? 'HELP' : 'OTHER');
    this.change(s => {
      if (s.webhooks.some(x => x.id === input.sid) || s.events.some(x => x.id === input.sid)) return;
      s.webhooks.push({ id: input.sid, at: this.now() });
      if (type !== 'STOP' && type !== 'START') {
        if (type === 'OTHER') {
          const c = s.consents.find(x => x.phone === phone);
          s.inbox.push({ sid: input.sid, phone, body: input.body.slice(0, 1600), received: this.now(), state: c?.enrolled && !c.carrierOptedOut && c.disclosureVersion === SMS_DISCLOSURE_VERSION ? 'pending_dispatch'
            : !c?.carrierOptedOut && !c?.withdrawnAt && !c?.signupReplyAttempted ? 'pending_signup' : 'suppressed' });
          if (s.inbox.at(-1)!.state === 'suppressed') s.inbox.at(-1)!.body = '';
        }
        return;
      }
      const c = this.record(s, phone); const at = new Date(this.now()).toISOString();
      const resubscribed = type === 'START' && c.disclosureVersion === SMS_DISCLOSURE_VERSION && c.verifiedAt !== null;
      c.enrolled = resubscribed; c.carrierOptedOut = type === 'STOP'; c.updatedAt = at; c.withdrawnAt = resubscribed ? null : at;
      s.events.push({ id: input.sid, phone, type: `sms_${type.toLowerCase()}`, at, disclosureVersion: c.disclosureVersion, disclosure: resubscribed ? SMS_DISCLOSURE : null, client: null, verifiedAt: null });
    });
    // Twilio Advanced Opt Out owns keyword replies. The broker drains ordinary texts after acknowledgment.
    return '';
  }
  pendingTexts() {
    return this.state.inbox.filter(x => x.state === 'pending_dispatch' || x.state === 'pending_signup').map(x => ({ ...x,
      expired: this.now() - x.received >= SMS_TURN_MAX_AGE_MS || (x.retries ?? 0) >= 6,
      deferred: (x.retryAt ?? 0) > this.now(),
    }));
  }
  retryText(sid: string) {
    this.change(s => {
      const item = s.inbox.find(x => x.sid === sid);
      if (item) { item.retries = (item.retries ?? 0) + 1; item.retryAt = this.now() + Math.min(60_000, 2000 * 2 ** item.retries); }
    });
  }
  finishText(sid: string, state: 'replied' | 'suppressed' | 'uncertain') {
    this.change(s => { const item = s.inbox.find(x => x.sid === sid); if (item) { item.state = state; item.body = ''; } });
  }
  async sendThreadReply(phoneValue: string, messageSid: string, text: string) {
    const phone = this.phone(phoneValue);
    if (!/^SM[0-9a-f]{32}$/i.test(messageSid) || !text.trim() || text.length > 1000
      || /[\x00-\x08\x0b\x0c\x0e-\x1f]/.test(text)) throw new SmsError('invalid_request');
    if (!this.state.inbox.some(x => x.sid === messageSid && x.phone === phone && x.state === 'pending_dispatch'))
      throw new SmsError('invalid_request');
    this.requestedEligibility(phone);
    // Only Dexter's HTTPS origin may appear in a carrier answer. The API renders
    // replies from trusted portfolio values; this also refuses stale unsafe answers.
    const links = text.match(/(?:[a-z][a-z0-9+.-]*:\/\/|mailto:|tel:|www\.|(?:[\p{L}\p{N}-]+\.)+[\p{L}]{2,})[^\s<>]*/giu) ?? [];
    if (links.some(link => {
      try { const url = new URL(link.replace(/[.,!?;:)]+$/, '')); return url.origin !== 'https://dexter.cash' || !!url.username || !!url.password; }
      catch { return true; }
    })) text = 'Open https://dexter.cash and sign in with your passkey to continue.';
    return this.sendMessage(phone, messageSid, text);
  }
  /** A single inbound-requested signup response, never a substitute for subscription consent. */
  async sendSignupReply(phone: string, messageSid: string, setup = false) {
    if (!this.deliveryActive()) throw new SmsError('sms_delivery_pending', 503);
    const item = this.state.inbox.find(x => x.sid === messageSid && x.phone === phone);
    if (!item || !['pending_signup', 'pending_dispatch'].includes(item.state)) throw new SmsError('invalid_request');
    return this.sendMessage(phone, messageSid, setup ? 'Finish setting up Dexter at https://dexter.cash' : 'Sign up for Dexter texting at https://dexter.cash/sms', true);
  }
  /** Read-only admission check; does not consume a rate slot or send an enrollment text. */
  requestedEligibility(phoneValue: string) {
    const parsed = phoneSchema.safeParse(phoneValue);
    if (!parsed.success) throw new SmsError('sms_unsupported_number', 400);
    if (!this.deliveryActive()) throw new SmsError('sms_delivery_pending', 503);
    const consent = this.state.consents.find(value => value.phone === parsed.data);
    if (consent?.carrierOptedOut) throw new SmsError('carrier_opted_out', 409);
    if (!consent?.enrolled || consent.disclosureVersion !== SMS_DISCLOSURE_VERSION)
      throw new SmsError('sms_consent_required', 403);
    return { ok: true as const };
  }
  /** Immutable receipt lookup remains available after STOP or provider disablement. */
  requestedReceipt(phoneValue: string, callSid: string, message: string, messageId: string) {
    const phone = this.phone(phoneValue);
    if (!/^CA[0-9a-f]{32}$/i.test(callSid) || !message.trim() || message.length > 1000)
      throw new SmsError('invalid_request');
    const bodyHash = hash(`Dexter: ${message.trim()}\nReply STOP to opt out. HELP for help.`);
    const id = hash(`${callSid}:${phone}:${bodyHash}`);
    if (id !== messageId) throw new SmsError('invalid_request');
    const existing = this.state.messages.find(value => value.id === id && value.phone === phone
      && value.callSid === callSid && value.bodyHash === bodyHash);
    if (existing) return { ok: true as const, messageId: id, status: existing.status, duplicate: true };
    // A failed durable write can make absence in memory inconclusive.
    if (this.poisoned) throw new SmsError('sms_unavailable', 503);
    return null;
  }
  async sendRequested(phoneValue: string, callSid: string, message: string) {
    if (!this.deliveryActive()) throw new SmsError('sms_delivery_pending', 503);
    const phone = this.phone(phoneValue);
    if (!/^CA[0-9a-f]{32}$/i.test(callSid) || !message.trim() || message.length > 1000 || /[\x00-\x08\x0b\x0c\x0e-\x1f]/.test(message)) throw new SmsError('invalid_request');
    return this.sendMessage(phone, callSid, message);
  }
  private async sendMessage(phone: string, callSid: string, message: string, signup = false) {
    if (!this.deliveryActive()) throw new SmsError('sms_delivery_pending', 503);
    const body = `Dexter: ${message.trim()}\nReply STOP to opt out. HELP for help.`;
    const bodyHash = hash(body);
    const thread = callSid.startsWith('SM');
    const id = hash(thread ? `sms:${callSid}:${phone}` : `${callSid}:${phone}:${bodyHash}`);
    const existing = this.state.messages.find(x => x.id === id);
    if (existing) return { ok: true, messageId: id, status: existing.status, duplicate: true };
    this.change(s => {
      let c = s.consents.find(x => x.phone === phone);
      if (c?.carrierOptedOut || (signup ? !!c?.signupReplyAttempted || !!c?.withdrawnAt
        : !c?.enrolled || c.disclosureVersion !== SMS_DISCLOSURE_VERSION)) throw new SmsError('sms_consent_required', 403);
      this.limit(s, `send:phone:${hash(phone)}`, 60, 60 * MINUTE);
      if (!thread) this.limit(s, `send:call:${callSid}`, 5, DAY);
      this.limit(s, 'send:global', 2000, DAY);
      for (const key of [`send:phone:${hash(phone)}`, `send:call:${callSid}`, 'send:global']) s.rates.push({ key, at: this.now() });
      if (signup) {
        if (!c) { c = this.record(s, phone); c.withdrawnAt = null; }
        c.signupReplyAttempted = true;
      }
      s.messages.push({ id, phone, callSid, bodyHash, created: this.now(), sid: null, status: 'dispatching', errorCode: null });
    });
    try {
      const result = await this.options.provider.send({ to: phone, body, messagingServiceSid: this.options.messagingServiceSid, statusCallback: `${this.options.publicBaseUrl}/sms/status/${id}` });
      this.change(s => { const m = s.messages.find(x => x.id === id)!; m.sid = result.sid; if (m.status === 'dispatching') m.status = result.status; });
      return { ok: true, messageId: id, status: this.state.messages.find(x => x.id === id)!.status, duplicate: false };
    } catch (error) {
      const optedOut = !!error && typeof error === 'object' && 'code' in error && error.code === 21610;
      this.change(s => {
        const m = s.messages.find(x => x.id === id)!;
        if (m.status === 'dispatching') m.status = optedOut ? 'failed' : 'uncertain';
        if (optedOut) {
          m.errorCode = '21610';
          const c = this.record(s, phone); const at = new Date(this.now()).toISOString();
          c.enrolled = false; c.carrierOptedOut = true; c.updatedAt = at; c.withdrawnAt = at;
          s.events.push({ id: randomUUID(), phone, type: 'provider_opt_out', at, disclosureVersion: c.disclosureVersion, disclosure: null, client: null, verifiedAt: null });
        }
      });
      if (optedOut) throw new SmsError('carrier_opted_out', 409);
      throw new SmsError('sms_dispatch_uncertain', 502);
    }
  }
  delivery(id: string, phone: string, sid: string, status: string, errorCode: string | null) {
    const order: Record<string, number> = { dispatching: 0, uncertain: 0, accepted: 1, scheduled: 1, queued: 2, sending: 3, sent: 4, failed: 5, undelivered: 5, canceled: 5, delivered: 6, read: 7 };
    if (!Object.hasOwn(order, status)) throw new SmsError('invalid_request');
    this.change(s => {
      const m = s.messages.find(x => x.id === id && x.phone === phone);
      if (!m || (m.sid && m.sid !== sid)) throw new SmsError('unknown_message', 404);
      m.sid = sid;
      if ((order[m.status] ?? 0) <= order[status]) { m.status = status; m.errorCode = errorCode; }
      if (errorCode === '21610') {
        const c = this.record(s, phone); const at = new Date(this.now()).toISOString();
        if (!c.carrierOptedOut) s.events.push({ id: randomUUID(), phone, type: 'provider_opt_out', at, disclosureVersion: c.disclosureVersion, disclosure: null, client: null, verifiedAt: null });
        c.enrolled = false; c.carrierOptedOut = true; c.updatedAt = at; c.withdrawnAt = at;
      }
    });
  }
}
