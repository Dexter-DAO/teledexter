# Dexter Broker

Talk with Dexter about your portfolio over the phone. Dexter Broker connects a GPT-Live conversation to your Primary Dexter, the continuing assistant for your account. Primary supplies account information and handles requests under your existing permissions.

The voice instructions keep replies brief, let you interrupt, and ask Primary for current facts and action results. An interruption stops speech; the assistant checks an action's actual state before reporting that it stopped or completed.

## Current status

The broker service, GPT-Live adapter, and authenticated Primary client are implemented in this repository. Local tests use injected provider connections. The latest provider preflight was blocked by exhausted credits. A completed call through GPT-Live and Primary has not been demonstrated.

Outbound calls require an allowed destination, account readiness, and a successful audio preflight before dialing. The API admits the account through a verified outbound call. Inbound transport is prepared, but inbound account access remains disabled until per-call possession is established. Primary's question-and-answer continuation contract is still pending; a request waiting for an answer cannot be presented as completed.

The deployment configuration prepares a separate service on `127.0.0.1:3213`, exposed through `/broker/` on `phone.dexter.cash`. Existing Phone runs on port `3212`. Applying this configuration and changing a Twilio number's inbound routing are separate operations. See [operations](docs/BROKER_OPERATIONS.md) for deployment state, configuration, and call recovery.

## Development

Use Node.js 22 and npm:

```sh
npm ci
cp .env.example .env
```

Fill the values in `.env`. `BROKER_STATE_DIR` must be an absolute, private directory outside the checkout. The [configuration table](docs/BROKER_OPERATIONS.md#configuration) explains each value.

```sh
npm run typecheck
npm test
npm run build
npm start
```

`npm run dev` runs the broker with TypeScript reloads. `npm start` runs `dist/broker/main.js`. The inherited Phone server remains available through `npm run start:legacy`; the broker deployment uses the compiled broker entrypoint.

The local operator CLI supports `status`, `preflight`, `call`, `end`, and `reconcile`. Preflight creates a billable provider session; `call` can dial a telephone. Read the [controlled call procedure](docs/BROKER_OPERATIONS.md#controlled-call) before using either command.

## Source provenance

The initial source snapshot comes from Dexter-DAO/dexter-phone commit `e80627a4073f7adf325a5ddf9dbb31b259d97d66`. GPT-Live transport work reuses patterns from the fantasy football interviewer at `fbd79dc886c52895dda2a3144977da3e18693c6b`. Credentials and historical repository data are excluded from this repository.
