module.exports = {
  relayhubSidebar: [
    {
      type: 'doc',
      id: 'intro',
    },
    {
      type: 'category',
      label: 'User',
      items: [
        'user/get-started',
        'user/first-event',
      ],
    },
    {
      type: 'category',
      label: 'Developer',
      items: [
        'developer/quick-integrate',
        'developer/realtime-v2',
        'developer/queue-v2',
        'developer/skills-tab',
      ],
    },
    {
      type: 'category',
      label: 'Control Panel',
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
