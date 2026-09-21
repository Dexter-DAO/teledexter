# Dexter Broker

Call your Dexter assistant to discuss your portfolio and act on your instructions. Dexter Broker connects GPT-Live voice conversations to Primary Dexter, so calls can share the account context and capabilities used in the Dexter app.

## Build status

Implementation is underway. The starting source is the accepted Dexter Phone release from September 21, 2026. The GPT-Live adapter and Primary connection are being integrated. A completed call through that combined path has not been demonstrated yet.

## Development

Use Node.js 22 and npm. Install dependencies with `npm ci`, copy `.env.example` to `.env`, and fill the values for your environment. Run `npm run typecheck` and `npm test` to check changes.

The inherited Phone entrypoint remains available while the broker entrypoint is built. Configuration and deployment instructions will follow the implemented behavior.

## Source provenance

The initial source snapshot comes from Dexter-DAO/dexter-phone commit `e80627a4073f7adf325a5ddf9dbb31b259d97d66`. GPT-Live transport work reuses patterns from the fantasy football interviewer at `fbd79dc886c52895dda2a3144977da3e18693c6b`. Credentials and historical repository data are excluded from this repository.
