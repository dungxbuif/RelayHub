# Authentication and safe credentials

RelayHub uses different credentials for operators, trusted applications, agents and browsers. Keep each credential in the environment that needs it.

## Trusted application requests

Application calls use a key and signature. Sign the exact request target and body bytes with the algorithm documented by the API contract. Include the timestamp and reject stale or altered requests.

## Callback receivers

Verify the callback signature against the raw body before parsing JSON. Enforce a timestamp window, record the event and delivery IDs, and make the business effect idempotent.

## Browser clients

The trusted part of your application requests a short-lived token for the current user. The token contains only the allowed channel or stream permissions. Never expose an application key, signing secret or agent credential to browser JavaScript.

## Agent credentials

An agent credential is scoped to its application and should live in the agent's private environment. Rotate it through the operator's credential workflow when it may have been exposed.

## Workspace boundaries

Every request is checked against the workspace and application associated with its credential. A client-supplied workspace or application ID does not grant access.
