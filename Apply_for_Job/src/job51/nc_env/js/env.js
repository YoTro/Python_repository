// env.js - 51job NC 滑块验证补环境
// 目标: 在 Node.js 中运行 AWSC 全套 JS，得到 u_atoken/u_asession/u_asig

const https = require('https');
const http  = require('http');
const path  = require('path');
const fs    = require('fs');

// 加载真实浏览器指纹数据
const _canvasReal = (() => {
    try { return JSON.parse(fs.readFileSync(path.join(__dirname, '../data/canvas_real.json'), 'utf8')); } catch(e) { return {}; }
})();
const _measuretextReal = (() => {
    try { return JSON.parse(fs.readFileSync(path.join(__dirname, '../data/measuretext_real.json'), 'utf8')); } catch(e) { return {}; }
})();
const _fp = (() => {
    try { return JSON.parse(fs.readFileSync(path.join(__dirname, '../data/browser_fingerprint.json'), 'utf8')); } catch(e) { return {}; }
})();
const _lswucnData = (() => {
    try { return JSON.parse(fs.readFileSync(path.join(__dirname, '../data/lswucn_real.json'), 'utf8')); } catch(e) { return {}; }
})();

// 指纹快捷引用
const _fpNav    = _fp.navigator    || {};
const _fpScreen = _fp.screen      || {};
const _fpWin    = _fp.window      || {};
const _fpAudio  = _fp.audio       || {};
const _fpWebgl  = _fp.webgl       || {};
const _fpWebglParams = _fpWebgl.params || {};

// ─────────────────────────────────────────────
// 1. 基础 window / global 环境
// ─────────────────────────────────────────────
const TARGET_URL = 'https://cupidjob.51job.com/open/noauth/search-pc';
const CAPTCHA_ID = '2ced30cb2fb58660c6920675d39a48c1';

global.window = global;
global.self   = global;
global.globalThis = global;

// location
global.location = {
    href: TARGET_URL,
    hostname: 'cupidjob.51job.com',
    host: 'cupidjob.51job.com',
    pathname: '/open/noauth/search-pc',
    protocol: 'https:',
    search: '',
    hash: '',
    origin: 'https://cupidjob.51job.com'
};

// navigator - 优先使用真实浏览器采集值，回退到合理默认
const _navigator = {
    userAgent:           _fpNav.userAgent           || 'Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Mobile Safari/537.36',
    platform:            _fpNav.platform            || 'Win32',
    language:            _fpNav.language            || 'zh-CN',
    languages:           _fpNav.languages           || ['zh-CN'],
    cookieEnabled:       _fpNav.cookieEnabled       !== undefined ? _fpNav.cookieEnabled : true,
    hardwareConcurrency: _fpNav.hardwareConcurrency || 8,
    deviceMemory:        _fpNav.deviceMemory        || 8,
    maxTouchPoints:      _fpNav.maxTouchPoints      !== undefined ? _fpNav.maxTouchPoints : 5,
    vendor:              _fpNav.vendor              || 'Google Inc.',
    appName:             _fpNav.appName             || 'Netscape',
    appCodeName:         _fpNav.appCodeName         || 'Mozilla',
    appVersion:          _fpNav.appVersion          || '5.0 (Linux; Android 13; Pixel 7)',
    product:             _fpNav.product             || 'Gecko',
    webdriver:           false,
    javaEnabled:         function javaEnabled() { return false; },
    plugins:             (function() {
        const p = [{ name: 'PDF Viewer', filename: 'internal-pdf-viewer', description: 'Portable Document Format', length: 1 },
                   { name: 'Chrome PDF Viewer', filename: 'internal-pdf-viewer', description: '', length: 1 },
                   { name: 'Chromium PDF Viewer', filename: 'internal-pdf-viewer', description: '', length: 1 },
                   { name: 'Microsoft Edge PDF Viewer', filename: 'internal-pdf-viewer', description: '', length: 1 },
                   { name: 'WebKit built-in PDF', filename: 'internal-pdf-viewer', description: '', length: 1 }];
        p.item = (i) => p[i] || null; p.namedItem = (n) => p.find(x=>x.name===n)||null; p.length = p.length;
        return p;
    })(),
    mimeTypes:           (function() {
        const m = [{ type: 'application/pdf', description: 'Portable Document Format', suffixes: 'pdf' },
                   { type: 'text/pdf', description: '', suffixes: 'pdf' }];
        m.item = (i) => m[i] || null; m.namedItem = (n) => m.find(x=>x.type===n)||null; m.length = m.length;
        return m;
    })(),
    connection:          _fpNav.connection          || { effectiveType: '4g', rtt: 100, downlink: 10 },
    onLine:              true,
    doNotTrack:          _fpNav.doNotTrack          !== undefined ? _fpNav.doNotTrack : null,
    getBattery:          function getBattery() { return Promise.resolve({ level: 1, charging: true }); },
    sendBeacon:          function sendBeacon(url, data) { return true; },
    mediaDevices:        { enumerateDevices: () => Promise.resolve([]) },
    permissions:         { query: () => Promise.resolve({ state: 'prompt' }) },
};
try {
    Object.defineProperty(global, 'navigator', { value: _navigator, writable: true, configurable: true });
} catch(e) {
    global.navigator = _navigator;
}

// screen - 优先使用真实采集值
global.screen = {
    width:       _fpScreen.width       || 412,
    height:      _fpScreen.height      || 915,
    availWidth:  _fpScreen.availWidth  || 412,
    availHeight: _fpScreen.availHeight || 915,
    colorDepth:  _fpScreen.colorDepth  || 24,
    pixelDepth:  _fpScreen.pixelDepth  || 24,
    orientation: (_fpScreen.orientation) || { type: 'landscape-primary', angle: 0 }
};

// ─────────────────────────────────────────────
// 2. Cookie 管理
// ─────────────────────────────────────────────
const _cookies = {};

// 预加载真实浏览器 cookies（tfstk / uid 等关键 cookie）
(function preloadCookies() {
    const rawCookies = _lswucnData.cookies || '';
    if (!rawCookies) return;
    rawCookies.split(';').forEach(pair => {
        const eq = pair.indexOf('=');
        if (eq < 0) return;
        const k = pair.slice(0, eq).trim();
        const v = pair.slice(eq + 1).trim();
        if (k && v) _cookies[k] = v;
    });
    const count = Object.keys(_cookies).length;
    if (count > 0) console.log('[ENV] 预加载真实 cookies:', count, '项, tfstk:', (_cookies.tfstk || '').substring(0, 20) + '...');
})();

global.document = {
    get cookie() {
        return Object.entries(_cookies).map(([k,v]) => `${k}=${v}`).join('; ');
    },
    set cookie(str) {
        const [kv] = str.split(';');
        if (!kv) return;
        const eq = kv.indexOf('=');
        if (eq < 0) return;
        const k = kv.slice(0, eq).trim();
        const v = kv.slice(eq + 1).trim();
        if (v === '' && str.includes('1970')) { delete _cookies[k]; return; }
        // 防止 um.js 把真实 cookie 覆盖为 "undefined" 或 "null"
        if ((v === 'undefined' || v === 'null') && _cookies[k] && _cookies[k].length > 10) {
            console.log('[ENV] 阻止覆盖真实 cookie:', k, '→ 值为', v);
            return;
        }
        _cookies[k] = v;
    },
    domain: 'cupidjob.51job.com',
    referrer: '',
    title: '51job',
    documentElement: { style: { WebkitAppearance: '' } },
    getElementsByClassName(cls) {
        // 在 _mockDivCache 中按 className 搜索
        return Object.values(document._mockDivCache || {}).filter(el => {
            if (!el || !el.className) return false;
            const classes = el.className.split(/\s+/);
            return cls.split(/\s+/).every(c => classes.includes(c));
        });
    },
    getElementsByTagName(tag) {
        if (tag === 'script') {
            // nc.js 需要至少一个 script 元素（用于 insertBefore）
            const mockScript = {
                tagName: 'SCRIPT',
                id: '', className: '', type: '', src: '',
                getAttribute(k) { return this[k] || null; },
                setAttribute(k, v) { this[k] = v; },
                hasAttribute(k) { return k in this && this[k] !== ''; },
                removeAttribute(k) { delete this[k]; },
                addEventListener() {}, removeEventListener() {},
                parentNode: {
                    insertBefore(node, ref) { return node; },
                    appendChild(node) { return node; },
                    removeChild(node) { return node; },
                    style: {}
                }
            };
            return [mockScript];
        }
        if (tag === 'head') return [{ appendChild: () => {} }];
        return [];
    },
    createElement(tag) {
        const et = _makeEventTarget();
        const el = Object.assign(et, {
            tagName: tag.toUpperCase(),
            style: {},
            id: '',
            src: '',
            className: '',
            innerHTML: '',
            async: false,
            onload: null,
            onerror: null,
            children: [],
            childNodes: [],
            ariaLabel: '',
            tabIndex: 0,
            role: '',
            onselectstart: null,
            onmousedown: null,
            setAttribute(k, v) {
                this[k] = v;
                if (k === 'id') _registerEl(this);
            },
            getAttribute(k) { return this[k] != null ? String(this[k]) : null; },
            hasAttribute(k) { return k in this && this[k] !== ''; },
            removeAttribute(k) { delete this[k]; },
            appendChild(node) {
                this.children.push(node); this.childNodes.push(node);
                if (node && node.id) _registerEl(node);
                return node;
            },
            insertBefore(node, ref) { this.children.push(node); if (node && node.id) _registerEl(node); return node; },
            removeChild(node) { return node; },
            getElementsByTagName(tag) {
                const t = tag.toUpperCase();
                return this.children.filter(c => c && c.tagName === t);
            },
            getElementsByClassName(cls) { return []; },
            querySelector(sel) { return null; },
            querySelectorAll(sel) { return []; },
            getBoundingClientRect() { return {x:395,y:296.4375,left:395,top:296.4375,width:300,height:48,right:695,bottom:344.4375}; },
            parentNode: {
                insertBefore(node, ref) { return node; },
                appendChild(node) { return node; },
                removeChild(node) { return node; },
                style: {}
            },
            offsetWidth: 300, offsetHeight: 48,
            offsetLeft: 0, offsetTop: 0,
            offsetParent: null,
        });
        if (tag === 'script') {
            Object.defineProperty(el, 'src', {
                get() { return el._src || ''; },
                set(url) {
                    el._src = url;
                    if (!url) return;
                    setTimeout(() => loadAndExec(url, el), 0);
                }
            });
        }
        return el;
    },
    body: { appendChild() {}, clientWidth: _fpWin.innerWidth || 1920, clientHeight: _fpWin.innerHeight || 1080 },
    head: { appendChild() {} },
    write(html) {},
    ontouchstart: null,
    currentScript: null,
};

// ─────────────────────────────────────────────
// 3. JSONP 拦截器（核心：把 <script src=jsonp_url> 变成真实 HTTP 请求）
// ─────────────────────────────────────────────
const JSONP_CALLBACKS = {};   // 存储临时回调

function extractJsonpCallback(url) {
    try {
        const u = new URL(url.startsWith('//') ? 'https:' + url : url);
        return u.searchParams.get('callback') || u.searchParams.get('cb');
    } catch { return null; }
}

// JSONP 共享 Cookie jar（保存 Set-Cookie，在后续请求中带上）
const _httpCookieJar = {};

function _buildCookieHeader(url) {
    // 从 document.cookie + httpCookieJar 合并 cookie
    const docCookies = document.cookie;
    const jarCookies = Object.entries(_httpCookieJar).map(([k,v]) => `${k}=${v}`).join('; ');
    return [docCookies, jarCookies].filter(Boolean).join('; ');
}

function _saveSetCookie(res) {
    const raw = res.headers['set-cookie'];
    if (!raw) return;
    (Array.isArray(raw) ? raw : [raw]).forEach(line => {
        const part = line.split(';')[0];
        const eq = part.indexOf('=');
        if (eq < 0) return;
        const k = part.slice(0, eq).trim();
        const v = part.slice(eq + 1).trim();
        _httpCookieJar[k] = v;
    });
}

function httpGet(url) {
    return new Promise((resolve, reject) => {
        const mod = url.startsWith('https') ? https : http;
        const req = mod.get(url, {
            headers: {
                'User-Agent': navigator.userAgent,
                'Referer': TARGET_URL,
                'Accept': '*/*',
                'Cookie': _buildCookieHeader(url)
            }
        }, res => {
            _saveSetCookie(res);
            let data = '';
            res.on('data', d => data += d);
            res.on('end', () => resolve(data));
        });
        req.on('error', reject);
        req.setTimeout(8000, () => { req.destroy(); reject(new Error('timeout')); });
    });
}

function loadAndExec(url, scriptEl) {
    if (!url || url === 'undefined') return;
    const fullUrl = url.startsWith('//') ? 'https:' + url : url;

    // 检查是否是本地文件
    const localFiles = {
        'awsc.js': 'awsc.js',
        'fireyejs.js': 'fireyejs_debug.js',
        'nc.js': 'nc.js',
        'um.js': 'um.js',
        'collina.js': 'collina.js',
        'et_f.js': 'et_f.js',
    };
    for (const [key, file] of Object.entries(localFiles)) {
        if (fullUrl.includes(key) || fullUrl.includes(file.replace('.js', '/'))) {
            const localPath = path.join(__dirname, file);
            if (fs.existsSync(localPath)) {
                try {
                    const code = fs.readFileSync(localPath, 'utf8');
                    eval(code);
                    scriptEl && scriptEl.onload && scriptEl.onload();
                    return;
                } catch(e) {
                    console.error('[ENV] 本地文件执行错误:', file, e.message);
                    scriptEl && scriptEl.onerror && scriptEl.onerror(e);
                    return;
                }
            }
        }
    }

    // JSONP 请求（initialize/analyze）
    const cbName = extractJsonpCallback(fullUrl);
    if (cbName && fullUrl.includes('.jsonp')) {
        console.log('[JSONP]', fullUrl.split('?')[0]);
        if (fullUrl.includes('analyze')) {
            console.log('[JSONP ANALYZE URL]', fullUrl.substring(0, 4000));
            // 诊断：解码 t 参数中的时间戳
            try {
                const tParam = new URL(fullUrl).searchParams.get('t') || '';
                const tParts = tParam.split(':');
                if (tParts.length >= 3) {
                    const ts = parseInt(tParts[2]);
                    console.log('[DIAG] t.timestamp =', ts, '≈', new Date(ts).toISOString(), '  Date.now()=', Date.now(), '≈', new Date().toISOString());
                }
            } catch(e) {}
            // 诊断：打印当前 cookie jar
            const cookieStr = _buildCookieHeader(fullUrl);
            console.log('[DIAG] Cookie jar sending:', cookieStr.substring(0, 200) || '(empty)');
        }
        if (fullUrl.includes('initialize')) console.log('[JSONP INIT RESP will follow]');
        httpGet(fullUrl).then(body => {
            try {
                if (fullUrl.includes('analyze')) console.log('[JSONP ANALYZE RESP]', body.substring(0, 400));
                if (fullUrl.includes('initialize')) console.log('[JSONP INIT RESP]', body.substring(0, 600));
                eval(body); // 执行 callback(data)
                scriptEl && scriptEl.onload && scriptEl.onload();
            } catch(e) {
                console.error('[JSONP] 执行错误:', e.message);
            }
        }).catch(e => {
            console.error('[JSONP] 请求失败:', e.message);
            scriptEl && scriptEl.onerror && scriptEl.onerror(e);
        });
        return;
    }

    // 非本地文件且非JSONP - 静默跳过（避免加载未知CDN JS产生噪音）
    // console.log('[SKIP REMOTE]', fullUrl.substring(0, 80));
    scriptEl && setTimeout(() => scriptEl.onerror && scriptEl.onerror(new Error('skipped')), 0);
}

// ─────────────────────────────────────────────
// 4. XMLHttpRequest Mock
// ─────────────────────────────────────────────
global.XMLHttpRequest = class XMLHttpRequest {
    constructor() {
        this.readyState = 0;
        this.status = 0;
        this.responseText = '';
        this._headers = {};
        this._method = 'GET';
        this._url = '';
    }
    open(method, url) { this._method = method; this._url = url; this.readyState = 1; }
    setRequestHeader(k, v) { this._headers[k] = v; }
    send(body) {
        const url = this._url.startsWith('//') ? 'https:' + this._url : this._url;
        const method = this._method || 'GET';
        const isPost = method.toUpperCase() === 'POST';
        const makeReq = (resolve, reject) => {
            const urlObj = new (require('url').URL)(url);
            const mod = urlObj.protocol === 'https:' ? require('https') : require('http');
            const postData = isPost && body ? (typeof body === 'string' ? body : JSON.stringify(body)) : null;
            const options = {
                hostname: urlObj.hostname,
                port: urlObj.port || (urlObj.protocol === 'https:' ? 443 : 80),
                path: urlObj.pathname + urlObj.search,
                method: method,
                headers: Object.assign({
                    'User-Agent': navigator.userAgent,
                    'Referer': TARGET_URL,
                    'Accept': '*/*',
                    'Origin': 'https://cupidjob.51job.com',
                    'Cookie': _buildCookieHeader(url)
                }, this._headers, postData ? {
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Content-Length': Buffer.byteLength(postData)
                } : {})
            };
            const req = mod.request(options, res => {
                _saveSetCookie(res);
                let data = '';
                res.on('data', d => data += d);
                res.on('end', () => resolve(data));
            });
            req.on('error', reject);
            req.setTimeout(10000, () => { req.destroy(); reject(new Error('timeout')); });
            if (postData) req.write(postData);
            req.end();
        };
        new Promise(makeReq).then(data => {
            this.status = 200;
            this.readyState = 4;
            this.responseText = data;
            this.response = data;
            this.onreadystatechange && this.onreadystatechange();
            this.onload && this.onload();
        }).catch(e => {
            this.status = 0;
            this.onerror && this.onerror(e);
        });
    }
    getResponseHeader(k) { return null; }
    abort() {}
};

// ─────────────────────────────────────────────
// 5. 其他 Browser API Mock
// ─────────────────────────────────────────────
global.Image = class Image {
    constructor() { this._src = ''; }
    get src() { return this._src; }
    set src(v) { this._src = v; setTimeout(() => this.onload && this.onload(), 50); }
};

const _perfStart = Date.now();
global.performance = {
    now: () => Date.now() - _perfStart,
    timing: {
        navigationStart: _perfStart,
        domContentLoadedEventEnd: _perfStart + 800,
        loadEventEnd: _perfStart + 1200
    },
    getEntriesByType: () => [],
    getEntriesByName: () => [],
    mark: () => {},
    measure: () => {}
};

global.localStorage = (() => {
    // lswucn 优先用真实采集值（每次运行 collect_trajectory.py 后更新）
    const realLswucn = _lswucnData.lswucn || _fp.lswucn || '';
    const store = {
        'lswucn': realLswucn || ('T2gAp6tgMf-XP8fuhSOWL9rflyUEKpVNOMIYgP5QmmdqjhDpWVRtAf1ujDAMukiRuXA=@@' + Math.round(Date.now() / 1000)),
        'ETLCD': _lswucnData.ETLCD || 'false'
    };
    return {
        getItem: k => store[k] ?? null,
        setItem: (k, v) => { store[k] = String(v); },
        removeItem: k => { delete store[k]; },
        clear: () => { Object.keys(store).forEach(k => delete store[k]); },
        key: i => Object.keys(store)[i] ?? null,
        get length() { return Object.keys(store).length; }
    };
})();
global.sessionStorage = (() => {
    const store = {};
    return {
        getItem: k => store[k] ?? null,
        setItem: (k, v) => { store[k] = String(v); },
        removeItem: k => { delete store[k]; },
        clear: () => {},
        get length() { return Object.keys(store).length; }
    };
})();

global.history = { length: 1, state: null, pushState() {}, replaceState() {}, back() {}, forward() {} };

// safeStub: 一个既是函数又有安全属性的通用 stub
// 用于 WebGL 等 API 的未知返回值
const safeStub = new Proxy(function safeStub() { return safeStub; }, {
    get(t, prop) {
        if (prop === 'length') return 0;
        if (prop === 'toString') return () => '';
        if (prop === 'valueOf') return () => 0;
        if (prop === Symbol.toPrimitive) return () => 0;
        if (prop === Symbol.iterator) return function*() {};
        if (typeof prop === 'symbol') return undefined;
        return safeStub;
    },
    apply() { return safeStub; },
    construct() { return safeStub; }
});

// Canvas Mock（返回固定值）
global.HTMLCanvasElement = class {};
let _canvasSeq = 0;
function makeCanvas() {
    const _id = ++_canvasSeq;
    const _ops = [];
    const ctx2d = {
        fillStyle: '', font: '', textBaseline: '',
        fillRect(...a)  { _ops.push(['fillRect',...a]); },
        clearRect(...a) { _ops.push(['clearRect',...a]); },
        fillText(...a)  { _ops.push(['fillText', ...a, 'font:'+ctx2d.font]); },
        measureText: t => {
            const fontKey = ctx2d.font;
            const tStr = String(t);
            let w;
            if (_measuretextReal[fontKey] && _measuretextReal[fontKey][tStr] !== undefined) {
                w = _measuretextReal[fontKey][tStr];
            } else {
                // 回退：按字符数估算
                w = tStr.length * 8;
            }
            return { width: w };
        },
        getImageData: (x, y, w, h) => ({ data: new Uint8ClampedArray(w * h * 4) }),
        putImageData() {}, drawImage() {}, save() {}, restore() {},
        scale() {}, rotate() {}, translate() {}, beginPath() {},
        arc() {}, fill() {}, stroke() {}, moveTo() {}, lineTo() {},
        createLinearGradient: () => ({ addColorStop() {} }),
        shadowBlur: 0, shadowColor: '', lineWidth: 1, strokeStyle: ''
    };
    return {
        width: 300, height: 150,
        style: {},
        _id,
        getContext(type) {
            _ops.push(['getContext', type]);
            if (type === '2d') return ctx2d;
            if (type === 'webgl' || type === 'experimental-webgl') {
                    const glCtx = {
                    getParameter: p => {
                        // 优先使用真实浏览器采集的 WebGL 参数
                        const strKey = String(p);
                        if (_fpWebglParams[strKey] !== undefined) {
                            const v = _fpWebglParams[strKey];
                            if (Array.isArray(v)) {
                                const arr = new Float32Array(2); arr[0] = v[0]; arr[1] = v[1]; return arr;
                            }
                            return v;
                        }
                        const map = {
                            // Vendor/Renderer (via WEBGL_debug_renderer_info ext)
                            0x9245: _fpWebglParams['37445'] || 'Google Inc. (Intel)',
                            0x9246: _fpWebglParams['37446'] || 'ANGLE (Intel, Intel(R) UHD Graphics (0x0000A788) Direct3D11 vs_5_0 ps_5_0, D3D11)',
                            // Standard params
                            0x1F00: _fpWebglParams['7936'] || 'WebKit',
                            0x1F01: _fpWebglParams['7937'] || 'WebKit WebGL',
                            0x1F02: _fpWebglParams['7938'] || 'WebGL 1.0',
                            0x8B8C: _fpWebglParams['35724'] || 'GLSL ES 1.0',
                            0x0D33: _fpWebglParams['3379']  || 16384,
                            0x0D57: 8, 0x8869: 8,
                            0x84E8: _fpWebglParams['34024'] || 16384,
                            0x8872: _fpWebglParams['34930'] || 16,
                            0x8A35: 8,
                            3379: _fpWebglParams['3379']  || 16384,
                            34930: _fpWebglParams['34930'] || 16,
                            35660: _fpWebglParams['35660'] || 16,
                            36347: _fpWebglParams['36347'] || 4096,
                            3413: 8, 3412: 8, 3414: 24, 3411: 8,
                            35661: _fpWebglParams['35661'] || 32,
                            34076: _fpWebglParams['34076'] || 16384,
                            36349: _fpWebglParams['36349'] || 1024,
                            34024: _fpWebglParams['34024'] || 16384,
                            36348: _fpWebglParams['36348'] || 30,
                            34921: _fpWebglParams['34921'] || 16,
                        };
                        if (p === 33902 || p === 33901 || p === 3386) {
                            const arr = new Float32Array(2); arr[0] = 1; arr[1] = 1024; return arr;
                        }
                        return map[p] !== undefined ? map[p] : 0;
                    },
                    getExtension: (name) => {
                        if (name === 'WEBGL_debug_renderer_info') {
                            return { UNMASKED_VENDOR_WEBGL: 37445, UNMASKED_RENDERER_WEBGL: 37446 };
                        }
                        // Return non-null for common extensions so fingerprint checks pass
                        const supported = ['OES_texture_float','OES_element_index_uint','OES_standard_derivatives',
                            'WEBGL_lose_context','WEBGL_depth_texture','WEBGL_draw_buffers','EXT_color_buffer_half_float'];
                        if (supported.includes(name)) return {};
                        return null;
                    },
                    createBuffer: () => ({}), bindBuffer() {}, bufferData() {},
                    enable() {}, disable() {}, clearColor() {}, clear() {}, viewport() {},
                    createShader: () => ({}), shaderSource() {}, compileShader() {},
                    createProgram: () => ({}), attachShader() {}, linkProgram() {},
                    useProgram() {},
                    enableVertexAttribArray() {}, vertexAttribPointer() {}, drawArrays() {},
                    getShaderParameter: () => true, getProgramParameter: () => true,
                    createTexture: () => ({}), bindTexture() {}, texImage2D() {},
                    texParameteri() {}, generateMipmap() {},
                    getUniformLocation: () => ({}),
                    getAttribLocation: () => 0,
                    uniform1f() {}, uniform2f() {}, uniform3f() {}, uniform4f() {},
                    uniform1i() {}, uniform2i() {}, uniform3i() {}, uniform4i() {},
                    uniform1fv() {}, uniform2fv() {}, uniform3fv() {}, uniform4fv() {},
                    uniformMatrix2fv() {}, uniformMatrix3fv() {}, uniformMatrix4fv() {},
                    getActiveAttrib: () => ({ name: 'attr', type: 35664, size: 1 }),
                    getActiveUniform: () => ({ name: 'uNm', type: 35664, size: 1 }),
                    getProgramInfoLog: () => '', getShaderInfoLog: () => '',
                    deleteShader() {}, deleteProgram() {}, deleteBuffer() {}, deleteTexture() {},
                    isContextLost: () => false,
                    pixelStorei() {}, readPixels() {},
                    blendFunc() {}, depthFunc() {}, stencilFunc() {},
                    scissor() {}, colorMask() {},
                    getError: () => 0,
                    FRAGMENT_SHADER: 35632, VERTEX_SHADER: 35633,
                    COMPILE_STATUS: 35713, LINK_STATUS: 35714,
                    ARRAY_BUFFER: 34962, STATIC_DRAW: 35044, FLOAT: 5126,
                    TRIANGLES: 4, COLOR_BUFFER_BIT: 16384, DEPTH_BUFFER_BIT: 256,
                    TEXTURE_2D: 3553, TEXTURE_MIN_FILTER: 10241, LINEAR: 9729,
                    RGBA: 6408, UNSIGNED_BYTE: 5121, CLAMP_TO_EDGE: 33071,
                };
                // Proxy: 任何未知方法都返回 noop
                return new Proxy(glCtx, {
                    get(t, prop) {
                        if (prop in t) return t[prop];
                        if (typeof prop === 'symbol') return undefined;
                        // 返回一个安全的 stub 函数（也可当对象用）
                        const stub = function(...args) { return safeStub; };
                        return stub;
                    }
                });
            }
            return null;
        },
        toDataURL(...a) {
            // 使用真实浏览器采集的 canvas 指纹（已注入）
            if (_canvasReal.canvas_fireyejs) return _canvasReal.canvas_fireyejs;
            return 'data:image/png;base64,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';
        },
        addEventListener() {}, removeEventListener() {}
    };
}
document.createElement = (function(orig) {
    return function(tag) {
        if (tag === 'canvas') return makeCanvas();
        return orig.call(document, tag);
    };
})(document.createElement.bind(document));

// AudioContext Mock - sampleRate 使用真实采集值
global.AudioContext = global.webkitAudioContext = class AudioContext {
    constructor() {
        this.sampleRate = _fpAudio.sampleRate || 44100;
        this.state = 'suspended';
        this.destination = {
            maxChannelCount: _fpAudio.maxChannelCount || 2,
            channelCount:    _fpAudio.channelCount    || 2,
            channelCountMode: _fpAudio.channelCountMode || 'max'
        };
    }
    createOscillator() { return { type: 'triangle', frequency: { value: 10000, setValueAtTime() {} }, connect() {}, start() {}, stop() {} }; }
    createDynamicsCompressor() { return { threshold: { value: -50, setValueAtTime() {} }, knee: { value: 40, setValueAtTime() {} }, ratio: { value: 12, setValueAtTime() {} }, attack: { value: 0, setValueAtTime() {} }, release: { value: 0.25, setValueAtTime() {} }, connect() {} }; }
    createAnalyser() { return { fftSize: 2048, getFloatFrequencyData(arr) { arr.fill(-100); }, connect() {} }; }
    resume() { return Promise.resolve(); }
    close() { return Promise.resolve(); }
};

// OfflineAudioContext - 使用真实音频指纹哈希值重建 buffer
global.OfflineAudioContext = class OfflineAudioContext {
    constructor(channels, length, sampleRate) {
        this.length   = length   || 44100;
        this.sampleRate = sampleRate || 44100;
        this.currentTime = 0;
        this.destination = { maxChannelCount: 2, channelCount: 2, channelCountMode: 'max' };
    }
    createOscillator() {
        return { type: 'triangle', frequency: { value: 10000, setValueAtTime() {} }, connect() {}, start() {}, stop() {} };
    }
    createDynamicsCompressor() {
        return {
            threshold: { value: -50,  setValueAtTime() {} },
            knee:      { value: 40,   setValueAtTime() {} },
            ratio:     { value: 12,   setValueAtTime() {} },
            attack:    { value: 0,    setValueAtTime() {} },
            release:   { value: 0.25, setValueAtTime() {} },
            connect() {}
        };
    }
    startRendering() {
        return new Promise(resolve => {
            const fpAudioFP = _fp.audioFingerprint || {};
            const targetHash = parseFloat(fpAudioFP.hash || '46.88183549186215');
            const s0 = typeof fpAudioFP.sample0 === 'number' ? fpAudioFP.sample0 : 0.000035;
            const s1 = typeof fpAudioFP.sample1 === 'number' ? fpAudioFP.sample1 : 0.000038;
            const data = new Float32Array(this.length || 44100);
            data[0] = s0;
            data[1] = s1;
            for (let i = 2; i < 500; i++) {
                const decay = Math.exp(-(i - 2) / 80);
                data[i] = (i % 2 === 0 ? 1 : -1) * 0.1 * decay;
            }
            let sum = 0;
            for (let i = 0; i < 500; i++) sum += Math.abs(data[i]);
            if (sum > 0) {
                const scale = targetHash / sum;
                for (let i = 2; i < 500; i++) data[i] *= scale;
            }
            resolve({
                length: data.length, sampleRate: this.sampleRate, numberOfChannels: 1,
                getChannelData: (ch) => (ch === 0 ? data : new Float32Array(data.length))
            });
        });
    }
};

// MutationObserver, requestAnimationFrame
global.MutationObserver = class { observe() {} disconnect() {} };
global.requestAnimationFrame = cb => setTimeout(cb, 16);
global.cancelAnimationFrame = id => clearTimeout(id);
global.getComputedStyle = () => ({ getPropertyValue: () => '' });
global.matchMedia = () => ({ matches: false, addListener() {}, removeListener() {} });
global.devicePixelRatio = _fpWin.devicePixelRatio || 2.625;
global.innerWidth   = _fpWin.innerWidth   || 412;
global.innerHeight  = _fpWin.innerHeight  || 915;
global.outerWidth   = _fpWin.outerWidth   || 412;
global.outerHeight  = _fpWin.outerHeight  || 915;
global.screenX      = _fpWin.screenX      || 0;
global.screenY      = _fpWin.screenY      || 0;
global.screenTop    = _fpWin.screenTop    || 0;
global.screenLeft   = _fpWin.screenLeft   || 0;
global.pageXOffset  = 0;
global.pageYOffset  = 0;
global.scrollX      = 0;
global.scrollY      = 0;
global.scrollTo     = () => {};
global.scroll       = () => {};
global.scrollBy     = () => {};
// window event system - must be real, fireyejs registers handlers on window
(function() {
    const winET = _makeEventTarget();
    global.addEventListener = winET.addEventListener.bind(winET);
    global.removeEventListener = winET.removeEventListener.bind(winET);
    global.dispatchEvent = winET.dispatchEvent.bind(winET);
    global._windowEventTarget = winET;
})();

// ─────────────────────────────────────────────
// 6. 原生方法 patch（fireyejs 安全检测兼容）
// ─────────────────────────────────────────────

// 防止 getOwnPropertyDescriptor(null/undefined) 崩溃
const _getOPD = Object.getOwnPropertyDescriptor;
Object.getOwnPropertyDescriptor = function(obj, prop) {
    if (obj == null) return undefined;
    try { return _getOPD(obj, prop); } catch(e) { return undefined; }
};

const _getOPDNames = Object.getOwnPropertyNames;
Object.getOwnPropertyNames = function(obj) {
    if (obj == null) return [];
    try { return _getOPDNames(obj); } catch(e) { return []; }
};

const _getProto = Object.getPrototypeOf;
Object.getPrototypeOf = function(obj) {
    if (obj == null) return null;
    try { return _getProto(obj); } catch(e) { return null; }
};

// Proxy 检测兼容（fireyejs 会检测是否运行在 Proxy 中）
global.Proxy = global.Proxy || Proxy;
global.Reflect = global.Reflect || Reflect;

// 事件相关
global.Event = global.Event || class Event { constructor(type) { this.type = type; } };
global.CustomEvent = global.CustomEvent || class CustomEvent extends global.Event {
    constructor(type, init) { super(type); this.detail = init && init.detail; }
};
global.MouseEvent = global.MouseEvent || class MouseEvent extends global.Event {
    constructor(type, init) {
        super(type);
        Object.assign(this, { clientX: 0, clientY: 0, pageX: 0, pageY: 0, screenX: 0, screenY: 0,
            movementX: 0, movementY: 0, button: 0, buttons: 0, target: null }, init || {});
    }
};
global.TouchEvent = global.TouchEvent || class TouchEvent extends global.Event {};

// Worker / Blob 兼容
global.Worker = global.Worker || class Worker {
    constructor(url) { this._url = url; }
    postMessage() {} terminate() {}
    addEventListener() {} removeEventListener() {}
};
global.Blob = global.Blob || class Blob {
    constructor(parts, opts) { this._parts = parts; this.type = opts && opts.type || ''; }
    get size() { return (this._parts || []).join('').length; }
};
global.URL = global.URL || require('url').URL;
global.URL.createObjectURL = () => 'blob:mock-url';
global.URL.revokeObjectURL = () => {};

// crypto 兼容（fireyejs 可能用到）
if (!global.crypto) {
    const nodeCrypto = require('crypto');
    global.crypto = {
        getRandomValues(arr) {
            const bytes = nodeCrypto.randomBytes(arr.byteLength);
            for (let i = 0; i < arr.length; i++) {
                arr[i] = bytes[i % bytes.length];
            }
            return arr;
        },
        subtle: {}
    };
}

// 工具：生成 mock div 元素（nc.js 需要挂载点）
// 全局元素 ID 注册表（document.getElementById 可以找到 createElement 创建的元素）
document._mockDivCache = {};
function _registerEl(el) {
    if (el && el.id && el.id !== '_parent' && el.id !== '_qs') {
        document._mockDivCache[el.id] = el;
    }
    return el;
}
function _makeEventTarget() {
    const _handlers = {};
    return {
        _handlers,
        addEventListener(evt, fn) {
            if (!_handlers[evt]) _handlers[evt] = [];
            _handlers[evt].push(fn);
        },
        removeEventListener(evt, fn) {
            if (_handlers[evt]) {
                _handlers[evt] = _handlers[evt].filter(h => h !== fn);
            }
        },
        dispatchEvent(e) {
            const type = e && e.type;
            if (type && _handlers[type]) {
                _handlers[type].forEach(h => { try { h.call(document, e); } catch(err) { console.error('[dispatchEvent err]', type, err.message, err.stack ? err.stack.split('\n').slice(0,2).join(' | ') : ''); } });
            }
            return true;
        }
    };
}
function makeMockDiv(id) {
    const et = _makeEventTarget();
    const el = Object.assign(et, {
        tagName: 'DIV', id: id || '', className: '',
        innerHTML: '', style: {},
        children: [], childNodes: [],
        setAttribute(k, v) {
            this[k] = v;
            if (k === 'id') _registerEl(this);
        },
        getAttribute(k) { return this[k] != null ? String(this[k]) : null; },
        hasAttribute(k) { return k in this && this[k] !== ''; },
        removeAttribute(k) { delete this[k]; },
        appendChild(node) {
            this.children.push(node); this.childNodes.push(node);
            if (node && node.id) _registerEl(node);
            return node;
        },
        insertBefore(node, ref) { this.children.push(node); if (node && node.id) _registerEl(node); return node; },
        removeChild(node) { return node; },
        getElementsByTagName(tag) {
            const t = tag.toUpperCase();
            return this.children.filter(c => c && c.tagName === t);
        },
        getElementsByClassName(cls) { return []; },
        querySelector(sel) { return null; },
        querySelectorAll(sel) { return []; },
        getBoundingClientRect() { return {x:395,y:296.4375,left:395,top:296.4375,width:300,height:48,right:695,bottom:344.4375}; },
        parentNode: null,
        offsetParent: null,
        offsetWidth: 300, offsetHeight: 48,
        offsetLeft: 0, offsetTop: 0,
    });
    if (id) _registerEl(el);
    return el;
}
document.getElementById = function(id) {
    if (!document._mockDivCache[id]) document._mockDivCache[id] = makeMockDiv(id);
    return document._mockDivCache[id];
};
document.querySelector = function(sel) {
    // 对 # 选择器提取 id
    const m = sel && sel.match(/^#(.+)$/);
    if (m) return document.getElementById(m[1]);
    return makeMockDiv('_qs');
};
document.querySelectorAll = (sel) => [];
// document 自身也需要事件支持（nc.js 在 document 上注册 mousemove/mouseup）
(function() {
    const et = _makeEventTarget();
    document.addEventListener = et.addEventListener.bind(et);
    document.removeEventListener = et.removeEventListener.bind(et);
    document.dispatchEvent = et.dispatchEvent.bind(et);
    document._eventTarget = et;
})();
document.createEvent = (type) => ({
    initEvent() {}, initMouseEvent() {},
    type: '', clientX: 0, clientY: 0, preventDefault() {}, stopPropagation() {}
});
document.hidden = false;
document.visibilityState = 'visible';
document.documentElement = {
    style: { WebkitAppearance: '', contentVisibility: '' },
    clientWidth: _fpWin.innerWidth || 1920, clientHeight: _fpWin.innerHeight || 1080
};

// 补充 window.top / parent
global.top = global;
global.parent = global;
global.frames = [];
global.opener = null;
global.closed = false;

// ─────────────────────────────────────────────
// 7. AWSC 注册函数
// ─────────────────────────────────────────────
global.AWSCInner = { register: () => {} };
global.AWSC = null; // 由 awsc.js 初始化

// ─────────────────────────────────────────────
// 8. 常见 DOM 方法补全（fireyejs 可能调用）
// ─────────────────────────────────────────────
document.createRange = () => ({
    setStart() {}, setEnd() {}, collapse() {},
    cloneContents: () => ({ childNodes: [] }),
    getBoundingClientRect: () => ({ top:0,left:0,right:0,bottom:0,width:0,height:0 }),
    getClientRects: () => [],
    selectNode() {}, selectNodeContents() {},
    commonAncestorContainer: document
});
document.createTreeWalker = (root, whatToShow, filter) => ({
    root, currentNode: root,
    nextNode() { return null; },
    previousNode() { return null; },
    parentNode() { return null; },
    firstChild() { return null; }
});
document.createDocumentFragment = () => ({
    appendChild() {}, querySelector: () => null,
    querySelectorAll: () => [], childNodes: []
});
document.createElementNS = (ns, tag) => document.createElement(tag);
document.createTextNode = (text) => ({ data: text, nodeType: 3, textContent: text });
document.getSelection = () => ({
    rangeCount: 0, type: 'None', isCollapsed: true,
    getRangeAt: () => null, addRange() {}, removeAllRanges() {},
    toString: () => ''
});
document.execCommand = () => false;
document.hasFocus = () => false;
document.elementFromPoint = () => null;
document.caretRangeFromPoint = () => null;

// 补充 window 的常用方法
global.getSelection = document.getSelection;
global.clearInterval = clearInterval;
global.clearTimeout = clearTimeout;
global.fetch = (url) => Promise.resolve({
    ok: true, status: 200,
    text: () => Promise.resolve(''),
    json: () => Promise.resolve({}),
    arrayBuffer: () => Promise.resolve(new ArrayBuffer(0))
});

// HTMLElement mock（getBoundingClientRect等）
global.HTMLElement = global.HTMLElement || class HTMLElement {
    getBoundingClientRect() { return { top:0,left:0,right:0,bottom:0,width:100,height:30 }; }
    setAttribute() {} getAttribute() { return null; }
    addEventListener() {} removeEventListener() {}
};

// 补充 window.screen 方法
screen.availLeft = 0;
screen.availTop = 0;
screen.orientation = _fpScreen.orientation || { type: 'landscape-primary', angle: 0 };

// Chrome-specific properties (fireyejs checks for these)
global.chrome = {
    runtime: { id: undefined, connect: () => {}, sendMessage: () => {} },
    loadTimes: function() { return { connectionInfo: 'h2', firstPaintTime: 0 }; },
    csi: function() { return { startE: Date.now(), onloadT: Date.now(), pageT: Date.now() }; }
};

// webkitRequestAnimationFrame alias
global.webkitRequestAnimationFrame = global.requestAnimationFrame;

// requestIdleCallback
global.requestIdleCallback = (cb) => setTimeout(() => cb({ didTimeout: false, timeRemaining: () => 50 }), 1);
global.cancelIdleCallback = (id) => clearTimeout(id);

// Notification API (desktop Chrome)
global.Notification = global.Notification || {
    permission: 'default',
    requestPermission: () => Promise.resolve('default')
};

// WebSocket stub
global.WebSocket = global.WebSocket || class WebSocket {
    constructor(url) { this.readyState = 3; this.url = url; }
    send() {} close() {} addEventListener() {} removeEventListener() {}
    static get CONNECTING() { return 0; } static get OPEN() { return 1; }
    static get CLOSING() { return 2; } static get CLOSED() { return 3; }
};

// indexedDB stub
global.indexedDB = global.indexedDB || {
    open: (name) => {
        const req = {};
        req.result = { objectStoreNames: { contains: () => false }, createObjectStore: () => {}, transaction: () => ({ objectStore: () => ({}) }) };
        setTimeout(() => { req.onsuccess && req.onsuccess({ target: req }); }, 0);
        return req;
    }
};

// Intl is Node.js built-in, just ensure it exists
global.Intl = global.Intl || {};

module.exports = {
    ready: false,
    loadAndExec,
    TARGET_URL,
    CAPTCHA_ID
};
