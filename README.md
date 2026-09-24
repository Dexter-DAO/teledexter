# Dexter Broker

Dexter Broker is the Stocklana project for a personal stock broker you can call, and that calls you when a saved watch condition occurs. GPT-Live handles the voice conversation; Primary Dexter, the continuing agent attached to your account, handles research and account requests through OpenDexter and your existing permissions.

The product covers stock research, portfolio discussion, Buy and Sell instructions, watch alerts and saved results you can reopen. Portfolio answers start with the useful summary, with details available on request. Dexter can ask a question while handling a request and keep your reply attached to that work. Calls and completed conversation turns are saved with the requests they produced.

This repository contains the voice transport, Twilio calling, and the connection to Primary. It builds on Dexter Phone. The main implementation lives in [src/broker](src/broker) and [src/live](src/live); account execution lives in the shared Dexter API.

As of 24 September, arranged outbound calls have connected callers to Primary and returned spoken answers. Earlier trials exposed stale portfolio answers, long waits and a lost follow-up. The shared portfolio tools now return current prices and daily changes, with summary and detail views. The full conversation still needs to demonstrate dependable research, current account answers, authorized Buy and Sell, and useful follow-ups together.

The saved-watch call adapter and its API integration are deployed. Connecting the existing watch service to this adapter, configuring the demonstration's stock watch and verifying its source evaluation and useful call remain delivery work. See [watch calls](docs/HARK.md) for the integration details.

The existing telephone number still reaches legacy Dexter Phone. The new incoming-call implementation keeps the caller on the same call while account access is approved; its public approval experience and release remain pending. Saved-work web pages, later-call replies and voice-session refresh are also being completed. These are part of the product, along with the recorded demonstration and Stocklana submission. Core conversation continuity and saved work remain required; richer long-term recall is optional for the hackathon.

For development, use Node.js 22 and npm:

```sh
npm ci
cp .env.example .env
```

Configure Twilio, GPT-Live access, and the Dexter API connection using the [configuration reference](docs/BROKER_OPERATIONS.md#configuration). Set `BROKER_STATE_DIR` to a private absolute directory outside the checkout; it holds call records and transcripts.

```sh
npm run typecheck
npm test
npm run dev
```

For a compiled build, run `npm run build` followed by `npm start`. The inherited Phone entrypoint is available through `npm run start:legacy`.

The [operations guide](docs/BROKER_OPERATIONS.md) covers deployment and call recovery. Its [controlled call procedure](docs/BROKER_OPERATIONS.md#controlled-call) explains the operator CLI; preflight consumes provider credits, and the call command dials a telephone. Use the [demo procedure](docs/DEMO.md) to verify audible conversation and account results together.
