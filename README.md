# Dexter Broker

Dexter Broker is a phone interface to your continuing Dexter agent. GPT-Live handles the voice conversation; Primary Dexter, the agent attached to your account, handles account requests through Dexter's shared tools and your existing permissions.

The aim is to talk through your portfolio, ask follow-up questions, and request actions in the same conversation. Dexter can ask for an answer while handling a request and keep your reply attached to that work. Calls and completed conversation turns are saved with the requests they produced.

This repository contains the voice transport, Twilio calling, and the connection to Primary. It builds on Dexter Phone. The main implementation lives in [src/broker](src/broker) and [src/live](src/live); account execution lives in the shared Dexter API.

The phone preview is being tested through arranged outbound calls. The latest trial returned answers from Primary and carried them into the voice conversation. It also exposed remaining problems: the opening reused an older portfolio balance, replies were slow, and a follow-up spoken during another request was saved without being submitted. A fresh, complete portfolio readout remains unqualified.

The existing telephone number still reaches legacy Dexter Phone. An inbound callback handler is implemented here; the incoming-call experience remains under review. Automatic opportunity calls are planned, and broader conversation recall awaits activation of the memory provider.

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
