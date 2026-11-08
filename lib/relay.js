/* Kaltot shared code: the message relay between the projector and the phones, and the name judge.
   The relay talks to two free public MQTT brokers at once, so the game keeps working if one of them is down. */
(function (g) {
  'use strict';

  var DEFAULT_BROKERS = ['wss://broker.hivemq.com:8884/mqtt', 'wss://broker.emqx.io:8084/mqtt'];
  var ALPHA = 'abcdefghjkmnpqrstuvwxyz23456789';

  function rid(n) {
    var a = new Uint8Array(n), s = '';
    (g.crypto || g.msCrypto).getRandomValues(a);
    for (var i = 0; i < n; i++) s += ALPHA[a[i] % ALPHA.length];
    return s;
  }

  function brokerParam() {
    try { return new URLSearchParams(g.location.search).get('broker') || ''; } catch (e) { return ''; }
  }
  function brokerList() {
    var b = brokerParam();
    return b ? b.split(',').filter(Boolean) : DEFAULT_BROKERS.slice();
  }

  /* opts: { subs: [topic filters], onMessage(topic, data), onStatus(connected, total) } */
  function Relay(opts) {
    var self = this;
    this.subs = opts.subs || [];
    this.onMessage = opts.onMessage || function () {};
    this.onStatus = opts.onStatus || function () {};
    this.clients = [];
    this.seen = new Map();
    var urls = opts.brokers || brokerList();
    var cid = 'kt_' + rid(12);
    urls.forEach(function (url, i) {
      var c;
      try {
        c = g.mqtt.connect(url, {
          clientId: cid + '_' + i, clean: true, keepalive: 30, reconnectPeriod: 2500,
          connectTimeout: 10000, protocolVersion: 4, resubscribe: false
        });
      } catch (e) { return; }
      c.on('connect', function () {
        if (self.subs.length) c.subscribe(self.subs, { qos: 1 });
        self._status();
      });
      ['close', 'offline', 'end', 'reconnect'].forEach(function (ev) { c.on(ev, function () { self._status(); }); });
      c.on('error', function () { self._status(); });
      c.on('message', function (topic, payload) { self._in(topic, payload); });
      self.clients.push(c);
    });
    setTimeout(function () { self._status(); }, 0);
  }
  Relay.prototype.connected = function () {
    return this.clients.filter(function (c) { return c.connected; }).length;
  };
  Relay.prototype._status = function () {
    this.onStatus(this.connected(), this.clients.length);
  };
  Relay.prototype._in = function (topic, payload) {
    var text = '';
    try { text = typeof payload === 'string' ? payload : new TextDecoder().decode(payload); } catch (e) { return; }
    if (!text) return;
    var key = topic + '\n' + text;
    if (this.seen.has(key)) return;
    this.seen.set(key, Date.now());
    if (this.seen.size > 3000) {
      var cut = Date.now() - 15 * 60 * 1000;
      this.seen.forEach(function (t, k, m) { if (t < cut) m.delete(k); });
    }
    var data;
    try { data = JSON.parse(text); } catch (e) { return; }
    if (!data || typeof data !== 'object') return;
    try { this.onMessage(topic, data); } catch (e) { if (g.console) console.error(e); }
  };
  /* Publishes to every broker; resolves when at least one broker confirmed, rejects after 10 s. */
  Relay.prototype.publish = function (topic, data, retain) {
    var text = JSON.stringify(data), clients = this.clients;
    return new Promise(function (res, rej) {
      var left = clients.length, done = false;
      if (!left) { rej(new Error('no brokers')); return; }
      var timer = setTimeout(function () { if (!done) { done = true; rej(new Error('timeout')); } }, 10000);
      clients.forEach(function (c) {
        try {
          c.publish(topic, text, { qos: 1, retain: !!retain }, function (err) {
            if (done) return;
            if (!err) { done = true; clearTimeout(timer); res(); return; }
            if (--left === 0) { done = true; clearTimeout(timer); rej(err); }
          });
        } catch (e) {
          if (!done && --left === 0) { done = true; clearTimeout(timer); rej(e); }
        }
      });
    });
  };

  /* ---------- names ---------- */
  var FINALS = { 'ך': 'כ', 'ם': 'מ', 'ן': 'נ', 'ף': 'פ', 'ץ': 'צ' };
  var STOP = { 'זה': 1, 'זאת': 1, 'זו': 1, 'אולי': 1, 'נראה': 1, 'לי': 1, 'שזה': 1, 'חושב': 1, 'חושבת': 1, 'בטוח': 1 };
  function isLetter(ch) { return /[\p{L}\p{N}]/u.test(ch); }
  function norm(s) {
    return String(s || '').normalize('NFKC').toLowerCase()
      .replace(/[֑-ׇ]/g, '')
      .replace(/[ךםןףץ]/g, function (c) { return FINALS[c]; })
      .replace(/[^\p{L}\p{N}\s]/gu, '')
      .replace(/(.)\1+/gu, '$1')
      .replace(/\s+/g, ' ').trim();
  }
  function lev(a, b) {
    if (a === b) return 0;
    var m = a.length, n = b.length, prev = [], cur = [], i, j;
    for (j = 0; j <= n; j++) prev[j] = j;
    for (i = 1; i <= m; i++) {
      cur = [i];
      for (j = 1; j <= n; j++) cur[j] = Math.min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (a[i - 1] === b[j - 1] ? 0 : 1));
      prev = cur;
    }
    return prev[n];
  }
  function close(a, b) {
    if (a === b) return true;
    var L = Math.min([...a].length, [...b].length);
    var tol = L >= 9 ? 2 : L >= 5 ? 1 : 0;
    return tol > 0 && lev([...a], [...b]) <= tol;
  }
  /* A guess is right if it matches the full name or an accepted alternative (small typos allowed),
     or if every word in it is a word of the name (first name alone, last name alone, reversed order). */
  function judge(guess, name, aliases) {
    var gw = norm(guess).split(' ').filter(function (w) { return w && !STOP[w]; });
    if (!gw.length) return false;
    var gj = gw.join(' '), gs = gw.join('');
    var full = norm(name);
    if (!full) return false;
    if (gs === full.replace(/ /g, '')) return true;
    var alts = String(aliases || '').split(/[,\n،;]/).map(norm).filter(Boolean);
    for (var i = 0; i < alts.length; i++) {
      if (close(gj, alts[i]) || gs === alts[i].replace(/ /g, '')) return true;
    }
    var words = full.split(' ').filter(function (w) { return [...w].length >= 2; });
    return gw.every(function (w) { return words.some(function (n) { return close(w, n); }); });
  }
  /* "אור וודיס" -> "___ _____": letters become blanks, spaces and punctuation stay. */
  function pattern(name) {
    return [...String(name || '').trim()].map(function (ch) { return /\s/.test(ch) ? ' ' : isLetter(ch) ? '_' : ch; }).join('').replace(/ +/g, ' ');
  }

  g.Kaltot = { Relay: Relay, rid: rid, brokerParam: brokerParam, norm: norm, judge: judge, pattern: pattern, isLetter: isLetter };
})(window);
