# Browser and live updates

Your application grants a browser a short-lived token after authenticating the user. The browser never receives an application key or signing secret.

```js
const token = await fetch("/api/my-relayhub-token", {
  method: "POST",
  credentials: "same-origin",
}).then(response => response.json());

const live = new RelayHubBrowserClient({
  url: token.url,
  token: token.token,
});

await live.connect();
live.subscribe("orders:current", message => renderProgress(message));
```

Give each token only the channels the user can see. Reconnect with the SDK's built-in lifecycle and refresh the token when it expires. Live messages can be missed during a disconnect; refresh your application's state after reconnecting.
