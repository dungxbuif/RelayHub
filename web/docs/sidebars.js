module.exports = {
  relayhubSidebar: [
    {
      type: 'doc',
      id: 'intro',
    },
    {
      type: 'category',
      label: 'Product',
      items: [
        'product/use-cases',
        'product/managed-service',
        'user/get-started',
        'user/first-event',
      ],
    },
    {
      type: 'category',
      label: 'Integrate',
      items: [
        'integrate/node',
        'integrate/go',
        'integrate/browser',
        'integrate/webhooks',
        'integrate/skills',
      ],
    },
    {
      type: 'category',
      label: 'Concepts',
      items: [
        'concepts/overview',
        'concepts/delivery',
        'security/auth',
      ],
    },
    {
      type: 'category',
      label: 'API Reference',
      items: ['api/v1', 'legacy/overview'],
    },
  ],
};
