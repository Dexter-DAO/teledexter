# Broker demonstration

The first useful demonstration is a phone conversation with Branch's existing Primary Dexter: hear an account result, ask a follow-up, and see that the work remains attached to the account. This runbook prepares qualification evidence. Submission and publication are separate work.

## Release prerequisites

The broker service runs through the public `/broker/` route. The conversation repair is deployed as Broker `c714a75`, alongside API `e93fd190` and MCP `2cffa8d`. It passed a GPT-Live rehearsal with simulated Twilio and Primary responses. A subsequent telephone trial returned spoken answers, but its opening reused old portfolio figures and a follow-up was saved without submission. A fresh, complete portfolio readout remains unqualified. The original uncertain request was reconciled as `not_admitted` without resubmission. Inbound number routing remains unchanged. Use the [controlled-call procedure](BROKER_OPERATIONS.md#controlled-call) and retain release receipts with the trial evidence.

## First conversation

Place the already-authorized outbound test call to Branch's current linked number. Save its request ID before dispatch. Let Dexter introduce the purpose, then speak naturally: ask what is in the portfolio and listen to the answer. Compare the spoken account facts with the identified Primary tool result and terminal assistant output. Judge the pace, pauses, and interruptions. Follow-up context is a separate acceptance item: the planned explicit link from Broker's status speech to an earlier request is not deployed, so a successful portfolio answer alone does not qualify phrases such as "then review it."

Write the call purpose as help Dexter will provide, such as "Read the caller's current portfolio and discuss their questions." Keep internal repair instructions out of that brief. Dexter must retrieve the balance rather than ask the caller to supply it. Include a natural interjection while the request runs and confirm the returned answer is still spoken afterward. A provider acknowledgment or audio byte count alone does not prove this.

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
