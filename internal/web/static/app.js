(function () {
  'use strict';

  const form = document.getElementById('order-form');
  const statusEl = document.getElementById('status');
  const statusLine = document.getElementById('status-line');
  const eventIdEl = document.getElementById('event-id');
  const orderIdEl = document.getElementById('echo-order-id');
  const amountEl = document.getElementById('echo-amount');
  const timestampEl = document.getElementById('timestamp');

  form.addEventListener('submit', async function (e) {
    e.preventDefault();

    const orderId = document.getElementById('orderId').value.trim();
    const amountRaw = document.getElementById('amount').value;
    const amount = Number(amountRaw);

    statusEl.hidden = false;
    statusLine.textContent = 'Publishing…';
    statusLine.className = '';
    eventIdEl.textContent = '';
    orderIdEl.textContent = '';
    amountEl.textContent = '';
    timestampEl.textContent = '';

    try {
      const res = await fetch('/api/orders', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ orderId: orderId, amount: amount }),
      });

      const text = await res.text();

      if (!res.ok) {
        statusLine.textContent = '✗ Error ' + res.status + ': ' + text;
        statusLine.className = 'error';
        return;
      }

      let data;
      try { data = JSON.parse(text); } catch (_) { data = {}; }

      statusLine.textContent = '✓ OrderCreated published';
      statusLine.className = 'ok';
      eventIdEl.textContent = data.eventId || '';
      orderIdEl.textContent = orderId;
      amountEl.textContent = amount.toString();
      timestampEl.textContent = new Date().toISOString();
    } catch (err) {
      statusLine.textContent = '✗ Network error: ' + err.message;
      statusLine.className = 'error';
    }
  });
})();