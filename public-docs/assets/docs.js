/* Optional progressive enhancement; every resource remains a normal link. */
(() => {
  const status = document.getElementById('copy-status');
  async function copy(button) {
    const target = document.getElementById(button.dataset.copy);
    let keepManualFocus = false;
    button.disabled = true;
    try {
      let text = target.value || target.textContent;
      if (button.dataset.source) {
        const response = await fetch(button.dataset.source);
        if (!response.ok) throw new Error('Source unavailable');
        text = await response.text();
        target.value = text;
      }
      try {
        if (!navigator.clipboard || !window.isSecureContext) throw new Error('Clipboard unavailable');
        await navigator.clipboard.writeText(text);
      } catch (_) {
        // Selection-based fallback for older browsers and denied clipboard access.
        const fallback = document.createElement('textarea');
        fallback.value = text;
        fallback.setAttribute('aria-label', 'Text to copy');
        document.body.appendChild(fallback);
        fallback.select();
        let copied = false;
        try { copied = document.execCommand('copy'); }
        finally { fallback.remove(); }
        if (!copied) {
          if (target.tagName === 'TEXTAREA') {
            target.hidden = false; target.focus(); target.select(); keepManualFocus = true;
          }
          button.textContent = 'Select text to copy';
          status.textContent = 'Automatic copy unavailable. Select the text above, or open the linked Markdown and copy manually.';
          return;
        }
      }
      button.textContent = 'Copied';
      status.textContent = 'Copied. Ready to paste.';
    } catch (_) {
      button.textContent = 'Open Markdown to copy';
      status.textContent = 'Could not load the text. Open the linked Markdown to copy manually.';
    } finally {
      button.disabled = false;
      if (!keepManualFocus) button.focus();
    }
  }
  document.querySelectorAll('button[data-copy]').forEach(button => {
    button.addEventListener('click', () => copy(button));
  });
})();
