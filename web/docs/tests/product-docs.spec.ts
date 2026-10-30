import {test, expect} from '@playwright/test';

const forbidden = /\b(?:postgres(?:ql)?|redis|nats|jetstream|centrifugo|partykit|yjs|docker|swarm|traefik|axum|sqlx|rust)\b/i;

async function publicText(page, path: string) {
  const response = await page.goto(path);
  expect(response?.ok(), path).toBeTruthy();
  const text = await page.locator('body').innerText();
  expect(text).not.toMatch(forbidden);
  return text;
}

test('new_user_can_integrate_without_hosting_components', async ({page, request}) => {
  const home = await publicText(page, '/');
  expect(home).toContain('managed');
  expect(home).toContain('What you can build');
  expect(await publicText(page, '/product/use-cases')).toContain('local application');
  expect(await publicText(page, '/user/get-started')).toContain('Send your first event');
  expect(await publicText(page, '/integrate/node')).toContain('Node.js');
  expect(await publicText(page, '/integrate/go')).toContain('Go SDK');
  expect(await publicText(page, '/security/auth')).toContain('trusted');
  const skills = await publicText(page, '/integrate/skills');
  expect(skills).toContain('Developer Kit');
  for (const path of ['/product/managed-service', '/user/first-event', '/integrate/browser', '/integrate/webhooks', '/concepts/overview', '/concepts/delivery', '/api/v1']) {
    await publicText(page, path);
  }
  expect(await publicText(page, '/security/auth')).toContain('Never expose');

  const manifestResponse = await request.get('/downloads/manifest.json');
  expect(manifestResponse.ok()).toBeTruthy();
  const manifest = await manifestResponse.json();
  expect(manifest.product_version).toBe('v1');
  expect(manifest.components.some((component: {name: string}) => component.name === 'relayhub-developer-kit')).toBeTruthy();
  for (const component of manifest.components) {
    expect(component.relative_url).toMatch(/^\/(downloads|skills)\//);
    expect(component.sha256).toMatch(/^[a-f0-9]{64}$/);
  }

  const llms = await (await request.get('/llms-full.txt')).text();
  expect(llms).toContain('Send your first event');
  expect(llms).toContain('Delivery is at least once');
  expect(llms).not.toMatch(forbidden);

  for (const path of ['/search-index.json', '/sitemap.xml', '/versions/index.json', '/versions/v1/llms-full.txt', '/versions/legacy-v2/llms-full.txt']) {
    const response = await request.get(path);
    expect(response.ok(), path).toBeTruthy();
    expect(await response.text()).not.toMatch(forbidden);
  }

  const legacy = await request.get('/legacy/overview');
  expect(legacy.ok()).toBeTruthy();
  expect(await legacy.text()).toContain('legacy');
  const legacyV2 = await request.get('/legacy-v2/overview');
  expect(legacyV2.ok()).toBeTruthy();
  const aliases = await request.get('/aliases.json');
  expect(aliases.ok()).toBeTruthy();
  expect(await aliases.text()).toContain('/developer/quick-integrate');
  const retired = await request.get('/deploy/README.md');
  expect(retired.status()).toBe(404);
  const retiredOperator = await request.get('/developer/streaming-protocol.md');
  expect(retiredOperator.status()).toBe(404);
  expect((await request.get('/developer/skills-tab')).status()).toBe(404);

  const catalog = await (await request.get('/llms.txt')).text();
  expect(catalog).toContain('Collaborative rooms');
  expect(catalog).toContain('planned');
});
