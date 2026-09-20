/** @type {import('@docusaurus/types').DocusaurusConfig} */
module.exports = {
  title: 'RelayHub Docs',
  tagline: 'Connect apps, deliver work, and build live experiences',
  url: 'https://relayhub.dungxbuif.com',
  baseUrl: '/',
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
  i18n: { defaultLocale: 'en', locales: ['en'] },
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
        { to: '/user/get-started', label: 'Quickstart', position: 'left' },
        { to: '/developer/skills-tab', label: 'SDKs & Agents', position: 'left' },
        {
          href: 'https://relayhub.dungxbuif.com/admin/',
          label: 'Control Panel',
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
            { label: 'OpenAPI', href: 'https://relayhub.dungxbuif.com/openapi.json' },
            { label: 'Schema: event-envelope', href: 'https://relayhub.dungxbuif.com/schemas/event-envelope.schema.json' },
            { label: 'AsyncAPI', href: 'https://relayhub.dungxbuif.com/asyncapi.yaml' },
          ],
        },
        {
          title: 'Control',
          items: [
            { label: 'Management Console', href: 'https://relayhub.dungxbuif.com/admin/' },
            { label: 'Skill ZIP', href: 'https://relayhub.dungxbuif.com/skills/relayhub-integration.zip' },
            { label: 'llms.txt', href: 'https://relayhub.dungxbuif.com/llms.txt' },
          ],
        },
      ],
      copyright: `RelayHub © ${new Date().getFullYear()}`,
    },
  },
};
