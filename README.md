# Dexter Broker

Talk with Dexter about your portfolio over the phone. Dexter Broker connects a GPT-Live conversation to your Primary Dexter, the continuing assistant for your account. Primary supplies account information and handles requests under your existing permissions.

The voice instructions keep replies brief, let you interrupt, and ask Primary for current facts and action results. An interruption stops speech; the assistant checks an action's actual state before reporting that it stopped or completed.

## Current status

Broker release `10c8115` is running at [phone.dexter.cash/broker/health](https://phone.dexter.cash/broker/health), including callback and question continuation. All 274 Broker tests pass. The shared Primary Phone API was deployed in release `d781d4f` on September 22, 2026. The latest GPT-Live preflight was blocked by exhausted credits; a completed call through GPT-Live and Primary has not been demonstrated.

Outbound calls require an allowed destination, account readiness, and a successful audio preflight before dialing. The API admits the account through a verified outbound call. When you call Dexter, the inbound flow offers a callback: press 1, hang up, and Dexter can call the verified number saved to your account. Account access begins on that outbound call. The inbound leg reads no private account information.

Callback consent expires after five minutes, and repeated callbacks recover the same saved request. When Primary needs an answer during a call, Dexter presents the question and its choices, then sends your answer back to that same request. It can recover an answer after a connection drops and handle another question without starting a second request. Answering an older question from a later callback remains a separate integration task.

The separate service on `127.0.0.1:3213` is exposed through `/broker/` on `phone.dexter.cash`. Existing Phone runs on port `3212`, and the telephone number still uses its existing inbound routing. See [operations](docs/BROKER_OPERATIONS.md) for release state, configuration, and call recovery, and the [demo procedure](docs/DEMO.md) for the first complete call trial.

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
