# Dexter Broker

Dexter Broker is the Stocklana project for a personal stock broker you can call, and that calls you when a saved watch condition occurs. GPT-Live handles the voice conversation; Primary Dexter, the continuing agent attached to your account, handles research and account requests through OpenDexter and your existing permissions.

The complete experience covers stock research, portfolio discussion, Buy and Sell instructions, watch alerts and saved results you can reopen. Portfolio answers start with the useful summary, with details available on request. Dexter can ask a question while handling a request and keep your reply attached to that work. Calls and completed conversation turns are saved with the requests they produced.

This repository contains the voice transport, Twilio calling, and the connection to Primary. It builds on Dexter Phone. The main implementation lives in [src/broker](src/broker) and [src/live](src/live); account execution lives in the shared Dexter API.

The phone preview is being tested through arranged outbound calls. The latest trial returned answers from Primary and carried them into the voice conversation. It also exposed remaining problems: the opening reused an older portfolio balance, replies were slow, and a follow-up spoken during another request was saved without being submitted. A fresh, complete portfolio readout remains unqualified.

The existing telephone number still reaches legacy Dexter Phone. An inbound callback handler is implemented here; the incoming-call experience remains under review. This branch adds calls from saved Hark watches, including the saved event, call recovery and billing records. That integration still requires its coordinated API and Broker release and a useful customer demonstration. See [watch calls](docs/HARK.md) for the integration details. Core conversation continuity and saved work remain required; richer long-term recall is optional for the hackathon.

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
