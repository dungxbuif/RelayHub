export const CONTRACT_VERSION = 'v1';

export function publishRequest({baseUrl, apiKey, signingSecret, targetAppId}) {
  return {
    contractVersion: CONTRACT_VERSION,
    baseUrl,
    headers: {
      'X-RelayHub-Api-Key': apiKey,
      'X-RelayHub-Signing-Secret': signingSecret,
    },
    targetAppId,
    note: 'Sign the exact request described by the v1 authentication reference before sending it.',
  };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  console.log(JSON.stringify(publishRequest({
    baseUrl: process.env.RELAYHUB_URL ?? 'https://relayhub.example',
    apiKey: '<server-only-api-key>',
    signingSecret: '<server-only-signing-secret>',
    targetAppId: '<target-app-id>',
  }), null, 2));
}
