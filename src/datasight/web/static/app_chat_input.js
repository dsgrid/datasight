// ---------------------------------------------------------------------------
// Auto-grow textarea
// ---------------------------------------------------------------------------
inputEl.addEventListener('input', function() {
  this.style.height = 'auto';
  this.style.height = Math.min(this.scrollHeight, window.innerHeight * 0.5) + 'px';
});

inputEl.addEventListener('keydown', function(e) {
  if (e.key === 'Enter' && !e.shiftKey) {
    e.preventDefault();
    handleSubmit(e);
  }
});

// ---------------------------------------------------------------------------
// Send message
// ---------------------------------------------------------------------------
function handleSubmit(e) {
  e.preventDefault();
  const text = inputEl.value.trim();
  if (!text || isStreaming) return false;
  sendMessage(text);
  return false;
}

function sendExample(text) {
  if (isStreaming) return;
  sendMessage(text);
}

