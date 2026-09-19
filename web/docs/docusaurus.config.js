/** @type {import('@docusaurus/types').DocusaurusConfig} */
module.exports = {
  title: 'RelayHub Docs',
  tagline: 'Reliable event relay platform for self-hosted integration',
  url: 'https://relayhub.dungxbuif.com',
  baseUrl: '/docs/',
  organizationName: 'dungxbuif',
  projectName: 'RelayHub',
  onBrokenLinks: 'throw',
  onBrokenAnchors: 'throw',
  markdown: {
    hooks: {
      onBrokenMarkdownLinks: 'throw',
    },
  },
  trailingSlash: false,
  i18n: { defaultLocale: 'en', locales: ['en', 'vi'] },
  presets: [
    [
      'classic',
      {
        docs: {
          routeBasePath: '/',
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl: 'https://github.com/dungxbuif/RelayHub/edit/main/web/docs/',
        },
        blog: false,
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      },
    ],
  ],
  themeConfig: {
    navbar: {
      title: 'RelayHub',
      items: [
        { to: '/', label: 'Docs', position: 'left' },
        { to: '/developer/skills-tab', label: 'Skills', position: 'left' },
        { to: '/control-panel/overview', label: 'Control Panel', position: 'left' },
        {
          href: 'https://relayhub.dungxbuif.com/docs',
          label: 'Public Docs (/docs)',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'References',
          items: [
            { label: 'OpenAPI', href: 'https://relayhub.dungxbuif.com/docs/openapi.json' },
            { label: 'Schema: event-envelope', href: 'https://relayhub.dungxbuif.com/docs/schemas/event-envelope.schema.json' },
            { label: 'AsyncAPI', href: 'https://relayhub.dungxbuif.com/docs/asyncapi.yaml' },
          ],
        },
        {
          title: 'Control',
          items: [
            { label: 'Management Console', href: 'https://relayhub.dungxbuif.com/docs/console.html' },
            { label: 'Skill ZIP', href: 'https://relayhub.dungxbuif.com/docs/skills/relayhub-integration.zip' },
            { label: 'llms.txt', href: 'https://relayhub.dungxbuif.com/docs/llms.txt' },
          ],
        },
      ],
      copyright: `RelayHub © ${new Date().getFullYear()}`,
    },
  },
};
