# Broker demonstration

The first useful demonstration is a phone conversation with Branch's existing Primary Dexter: hear an account result, ask a follow-up, and see that the work remains attached to the account. This runbook prepares qualification evidence. Submission and publication are separate work.

## Release prerequisites

The broker service, including callback and question handling, is running through the public /broker/ route. Shared API release `d781d4f` is deployed with Phone ingress and question hooks; its compatibility correction for existing Phone links still needs qualification and release. Restored GPT-Live credits and a successful audio preflight are also required for the combined call trial. Inbound number routing remains unchanged. A passing local test or health endpoint does not satisfy these steps. Use the accepted broker/API releases and the [controlled-call procedure](BROKER_OPERATIONS.md#controlled-call); retain their release receipts with the trial evidence.

## First conversation

Place the already-authorized outbound test call to Branch's current linked number. Save its request ID before dispatch. Let Dexter introduce the purpose, then speak naturally: ask what is in the portfolio, listen to the answer, and ask a follow-up about something it actually returned. Judge the pace, pauses, interruptions, and whether the follow-up preserves context. Compare the spoken account facts with the identified Primary tool result and terminal assistant output.

Exercise a saved question using a read-only preference, such as a concise portfolio overview versus position detail. Confirm that Primary saved the question, then answer on the phone and let the work finish. Keep the question ID, reply ID, and run ID before and after the answer. The answer must resume the same run. A question improvised only by the voice model does not qualify this step. If the saved question expires, Dexter should explain that state without claiming the answer was accepted.

After inbound routing is enabled for the accepted release, call Dexter, press 1, and hang up. Verify that the inbound leg disclosed no private account information and the callback reached the current linked number. Provider evidence must show the inbound call ended before the outbound dial. Repeat the exact consent callback in a controlled fixture to confirm it cannot create another call; the caller need not perform that technical replay during the demonstration.

An optional trade demonstration uses only an exact order supplied by Branch and the existing grant that permits it. This runbook supplies no ticker, amount, or new trading authorization. Match any spoken execution claim to the saved action result and its transaction or receipt evidence.

## Recovery rehearsal

Use a read-only request for a controlled disconnect or restart rehearsal. Preserve the original call, request, and run identities. Recover an uncertain accepted request through its saved result, and confirm there was no second submission or dial. Check transcript capture and call closeout afterward; pending capture remains pending until its exact replay is acknowledged. Injected timeout tests support this rehearsal but do not prove a production restart succeeded.

Once root has accepted and released the shared receipt/resolver feature, reopen the exact Phone result in the app and compare it with the spoken result. Until then, leave app reopening unqualified.

## Evidence record

Fill these fields only from the completed trial. Keep full journals privately; shared evidence should redact phone numbers, wallet/account identifiers, credentials, and unrelated portfolio details.

| Proof | Observed result and private evidence pointer |
| --- | --- |
| Trial time; accepted broker/API release receipts | |
| Outbound call identity; audible portfolio result and follow-up | |
| Question/reply identities; run before and after; terminal output | |
| Inbound termination; callback identity and destination match | |
| Restart/timeout recovery; submission/dial counts; capture closeout | |
| App reopened result, after shared feature acceptance | |
| Optional exact order and execution receipt, if exercised | |
