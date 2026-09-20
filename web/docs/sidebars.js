module.exports = {
  relayhubSidebar: [
    {
      type: 'doc',
      id: 'intro',
    },
    {
      type: 'category',
      label: 'Get started',
      items: [
        'user/get-started',
        'user/first-event',
      ],
    },
    {
      type: 'category',
      label: 'Build with RelayHub',
      items: [
        'developer/quick-integrate',
        'developer/webhooks',
        'developer/realtime',
        'developer/queue',
        'developer/streaming',
        'developer/functions',
        'developer/skills-tab',
      ],
    },
    {
      type: 'category',
      label: 'Manage and troubleshoot',
      items: [
        'control-panel/overview',
        'control-panel/track',
      ],
    },
    {
      type: 'category',
      label: 'API Reference',
      items: ['api/open-api-overview', 'api/signature-and-streaming'],
    },
  ],
};
