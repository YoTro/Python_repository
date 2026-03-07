"""
collect_fingerprints.py - 一次性采集真实浏览器指纹
采集内容: WebGL 全参数 / AudioContext 指纹 / navigator / screen / window / performance
保存到 browser_fingerprint.json
"""
import json, time, os
from DrissionPage import ChromiumPage, ChromiumOptions

OUT_FILE = os.path.join(os.path.dirname(__file__), '../data/browser_fingerprint.json')
REAL_URL = 'https://cupidjob.51job.com/open/noauth/search-pc'

COLLECT_JS = r"""
(function() {
    var result = {};

    // ── 1. navigator ──────────────────────────────────────────────
    var nav = {};
    ['userAgent','platform','language','languages','hardwareConcurrency',
     'deviceMemory','maxTouchPoints','vendor','appName','appVersion',
     'appCodeName','product','cookieEnabled','doNotTrack','onLine',
     'webdriver'].forEach(function(k) {
        try { nav[k] = navigator[k]; } catch(e) {}
    });
    try {
        var conn = navigator.connection || navigator.mozConnection || navigator.webkitConnection;
        if (conn) nav.connection = { effectiveType: conn.effectiveType, rtt: conn.rtt, downlink: conn.downlink };
    } catch(e) {}
    result.navigator = nav;

    // ── 2. screen / window ───────────────────────────────────────
    result.screen = {
        width: screen.width, height: screen.height,
        availWidth: screen.availWidth, availHeight: screen.availHeight,
        colorDepth: screen.colorDepth, pixelDepth: screen.pixelDepth,
        orientation: screen.orientation ? { type: screen.orientation.type, angle: screen.orientation.angle } : null
    };
    result.window = {
        innerWidth: window.innerWidth, innerHeight: window.innerHeight,
        outerWidth: window.outerWidth, outerHeight: window.outerHeight,
        devicePixelRatio: window.devicePixelRatio,
        screenX: window.screenX, screenY: window.screenY,
        screenTop: window.screenTop, screenLeft: window.screenLeft,
        pageXOffset: window.pageXOffset, pageYOffset: window.pageYOffset
    };

    // ── 3. performance.timing ────────────────────────────────────
    try {
        var t = performance.timing;
        result.performanceTiming = {
            navigationStart: t.navigationStart,
            domContentLoadedEventEnd: t.domContentLoadedEventEnd,
            loadEventEnd: t.loadEventEnd
        };
        result.performanceNowOffset = performance.now();
    } catch(e) {}

    // ── 4. WebGL ─────────────────────────────────────────────────
    try {
        var c = document.createElement('canvas');
        var gl = c.getContext('webgl') || c.getContext('experimental-webgl');
        if (gl) {
            var dbg = gl.getExtension('WEBGL_debug_renderer_info');
            var glParams = {};
            var paramList = [
                [0x1F00,'VENDOR'],[0x1F01,'RENDERER'],[0x1F02,'VERSION'],
                [0x8B8C,'SHADING_LANGUAGE_VERSION'],
                [0x0D33,'MAX_TEXTURE_SIZE'],[0x84E8,'MAX_RENDERBUFFER_SIZE'],
                [0x8872,'MAX_TEXTURE_IMAGE_UNITS'],[0x8869,'MAX_VERTEX_ATTRIBS'],
                [0x0D57,'MAX_VIEWPORT_DIMS'],[0x8A35,'MAX_FRAGMENT_UNIFORM_VECTORS'],
                [35660,'MAX_VERTEX_TEXTURE_IMAGE_UNITS'],[36347,'MAX_VERTEX_UNIFORM_VECTORS'],
                [36349,'MAX_FRAGMENT_UNIFORM_VECTORS'],[34024,'MAX_TEXTURE_IMAGE_UNITS'],
                [34076,'MAX_CUBE_MAP_TEXTURE_SIZE'],[36348,'MAX_VARYING_VECTORS'],
                [34921,'MAX_COMBINED_TEXTURE_IMAGE_UNITS'],
                [3413,'MAX_ELEMENTS_INDICES'],[3412,'MAX_ELEMENTS_VERTICES'],
                [3414,'DEPTH_BITS'],[3411,'STENCIL_BITS'],[35661,'MAX_FRAGMENT_UNIFORM_COMPONENTS'],
                [3379,'MAX_TEXTURE_SIZE_2']
            ];
            paramList.forEach(function(p) {
                try {
                    var v = gl.getParameter(p[0]);
                    if (v !== null && v !== undefined) {
                        glParams[p[0]] = (v && v.length === 2) ? [v[0], v[1]] : v;
                    }
                } catch(e) {}
            });
            if (dbg) {
                try { glParams[0x9245] = gl.getParameter(dbg.UNMASKED_VENDOR_WEBGL); } catch(e) {}
                try { glParams[0x9246] = gl.getParameter(dbg.UNMASKED_RENDERER_WEBGL); } catch(e) {}
            }
            // Extensions
            var exts = [];
            try { exts = gl.getSupportedExtensions() || []; } catch(e) {}
            result.webgl = { params: glParams, extensions: exts };
        }
    } catch(e) { result.webgl = { error: String(e) }; }

    // ── 5. AudioContext 指纹 ──────────────────────────────────────
    // 同步部分（sampleRate、state 等）
    try {
        var AC = window.AudioContext || window.webkitAudioContext;
        if (AC) {
            var ac = new AC();
            result.audio = {
                sampleRate: ac.sampleRate,
                state: ac.state,
                maxChannelCount: ac.destination ? ac.destination.maxChannelCount : 2,
                channelCount: ac.destination ? ac.destination.channelCount : 2,
                channelCountMode: ac.destination ? ac.destination.channelCountMode : 'max'
            };
            try { ac.close(); } catch(e) {}
        }
    } catch(e) { result.audio = { error: String(e) }; }

    return result;
})();
"""

# OfflineAudioContext 指纹需要 async，用单独 JS 采集
AUDIO_ASYNC_JS = r"""
(function() {
    return new Promise(function(resolve) {
        try {
            var ctx = new OfflineAudioContext(1, 44100, 44100);
            var osc = ctx.createOscillator();
            var cmp = ctx.createDynamicsCompressor();
            cmp.threshold.setValueAtTime(-50, ctx.currentTime);
            cmp.knee.setValueAtTime(40, ctx.currentTime);
            cmp.ratio.setValueAtTime(12, ctx.currentTime);
            cmp.attack.setValueAtTime(0, ctx.currentTime);
            cmp.release.setValueAtTime(0.25, ctx.currentTime);
            osc.type = 'triangle';
            osc.frequency.setValueAtTime(10000, ctx.currentTime);
            osc.connect(cmp);
            cmp.connect(ctx.destination);
            osc.start(0);
            ctx.startRendering().then(function(buf) {
                var data = buf.getChannelData(0);
                var sum = 0.0;
                for (var i = 0; i < Math.min(data.length, 500); i++) {
                    sum += Math.abs(data[i]);
                }
                resolve({ hash: sum.toString(), sample0: data[0], sample1: data[1] });
            }).catch(function(e) { resolve({ error: String(e) }); });
        } catch(e) { resolve({ error: String(e) }); }
    });
})();
"""

def collect():
    opts = ChromiumOptions()
    opts.headless(False)
    opts.set_argument('--disable-blink-features=AutomationControlled')

    page = ChromiumPage(addr_or_opts=opts)
    print(f'[INFO] 打开: {REAL_URL}')
    page.get(REAL_URL)
    time.sleep(3)

    # 同步指纹
    print('[INFO] 采集同步指纹...')
    result = page.run_js(COLLECT_JS, as_expr=True)
    if not result:
        result = page.run_js(COLLECT_JS)
    print(f'[INFO] navigator.userAgent: {result.get("navigator", {}).get("userAgent", "?")}')
    print(f'[INFO] screen: {result.get("screen", {})}')
    print(f'[INFO] WebGL vendor: {result.get("webgl", {}).get("params", {}).get(37445, result.get("webgl", {}).get("params", {}).get("0x9245", "?"))}')

    # 异步 AudioContext 指纹
    print('[INFO] 采集 AudioContext 指纹（异步）...')
    try:
        audio_fp = page.run_js(AUDIO_ASYNC_JS, as_expr=True)
        if audio_fp:
            result['audioFingerprint'] = audio_fp
            print(f'[INFO] 音频指纹 hash: {audio_fp.get("hash", "?")}')
    except Exception as e:
        print(f'[WARN] 音频指纹采集失败: {e}')

    # lswucn
    try:
        lswucn = page.run_js('return localStorage.getItem("lswucn");')
        result['lswucn'] = lswucn or ''
        print(f'[INFO] lswucn: {(lswucn or "")[:40]}...')
    except Exception as e:
        print(f'[WARN] lswucn 采集失败: {e}')

    with open(OUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f'[INFO] 指纹已保存: {OUT_FILE}')

    page.quit()
    print('[INFO] 浏览器已关闭')

if __name__ == '__main__':
    collect()
