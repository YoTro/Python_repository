"""
collect_trajectory.py - 打开真实页面，采集人工滑动轨迹，同时拦截 analyze.jsonp 直接拿 token
"""
import json
import time
import os
from DrissionPage import ChromiumPage, ChromiumOptions
from DrissionPage.errors import PageDisconnectedError

REAL_URL      = 'https://cupidjob.51job.com/open/noauth/search-pc'
OUT_TRAJ      = os.path.join(os.path.dirname(__file__), '../data/trajectory.json')
OUT_TOKEN     = os.path.join(os.path.dirname(__file__), '../data/nc_token_result.json')
OUT_LSWUCN    = os.path.join(os.path.dirname(__file__), '../data/lswucn_real.json')
OUT_PREACT    = os.path.join(os.path.dirname(__file__), '../data/pre_activity.json')

INJECT_JS = """
(function() {
    if (window._trajCollecting) return;
    window._trajCollecting = true;
    window._traj = null;
    window._trajRaw = [];
    window._trajDone = false;
    window._ncToken  = null;

    // 从页面加载起记录所有鼠标移动（pre-slide activity）
    var _preStart = Date.now();
    window._preActivity = [];
    function onPreMove(e) {
        if (window._trajRaw && window._trajRaw.length > 0) return; // 滑块已开始则停止
        var cx = e.clientX !== undefined ? e.clientX : 0;
        var cy = e.clientY !== undefined ? e.clientY : 0;
        window._preActivity.push({ x: cx, y: cy, t: Date.now() - _preStart });
        if (window._preActivity.length > 800) window._preActivity.shift(); // 只保留最近800点
    }
    document.addEventListener('mousemove', onPreMove, true);

    // 拦截 script JSONP（NC 用 script 标签加载 analyze.jsonp）
    var _origCreate = document.createElement.bind(document);
    document.createElement = function(tag) {
        var el = _origCreate(tag);
        if (tag.toLowerCase() === 'script') {
            var srcVal = '';
            Object.defineProperty(el, 'src', {
                configurable: true,
                enumerable: true,
                set: function(url) {
                    srcVal = url || '';
                    if (url && url.indexOf('analyze.jsonp') !== -1) {
                        fetch(url, {credentials:'include'}).then(function(r){ return r.text(); }).then(function(text){
                            var m = text.match(/\\((.+)\\)/s);
                            if (m) {
                                try {
                                    var obj = JSON.parse(m[1]);
                                    var r = obj.result || {};
                                    if (r.code === 0 || (r.token && r.sessionId)) {
                                        window._ncToken = { u_atoken: r.token, u_asession: r.sessionId, u_asig: r.sig };
                                        console.log('[NC_TOKEN]', JSON.stringify(window._ncToken));
                                    }
                                } catch(e) {}
                            }
                            try { (0, eval)(text); } catch(e) {}
                        }).catch(function(){});
                        return;
                    }
                    el.setAttribute('src', url);
                },
                get: function() { return srcVal; }
            });
        }
        return el;
    };

    // 鼠标轨迹采集
    var t0 = null;
    function onDown(e) {
        var cx = e.clientX !== undefined ? e.clientX : (e.touches && e.touches[0] ? e.touches[0].clientX : 0);
        var cy = e.clientY !== undefined ? e.clientY : (e.touches && e.touches[0] ? e.touches[0].clientY : 0);
        t0 = Date.now();
        window._trajRaw = [{ type:'mousedown', x:cx, y:cy, t:0 }];
        console.log('[TRAJ] mousedown', cx, cy);
    }
    function onMove(e) {
        if (t0 === null) return;
        var cx = e.clientX !== undefined ? e.clientX : (e.touches && e.touches[0] ? e.touches[0].clientX : 0);
        var cy = e.clientY !== undefined ? e.clientY : (e.touches && e.touches[0] ? e.touches[0].clientY : 0);
        window._trajRaw.push({ type:'mousemove', x:cx, y:cy, t:Date.now()-t0 });
    }
    function onUp(e) {
        if (t0 === null) return;
        var cx = e.clientX !== undefined ? e.clientX : (e.changedTouches && e.changedTouches[0] ? e.changedTouches[0].clientX : 0);
        var cy = e.clientY !== undefined ? e.clientY : (e.changedTouches && e.changedTouches[0] ? e.changedTouches[0].clientY : 0);
        window._trajRaw.push({ type:'mouseup', x:cx, y:cy, t:Date.now()-t0 });
        window._traj     = window._trajRaw.slice();
        window._trajDone = true;
        t0 = null;
        console.log('[TRAJ] done, points:', window._traj.length);
    }
    document.addEventListener('mousedown',  onDown,  true);
    document.addEventListener('mousemove',  onMove,  true);
    document.addEventListener('mouseup',    onUp,    true);
    document.addEventListener('touchstart', onDown,  true);
    document.addEventListener('touchmove',  onMove,  true);
    document.addEventListener('touchend',   onUp,    true);
    console.log('[TRAJ] collector injected on', location.href);
})();
"""

def try_inject(page):
    """注入到主页面及所有 iframe"""
    injected = 0
    # 主页面
    try:
        page.run_js(INJECT_JS)
        injected += 1
    except Exception as e:
        print(f'[WARN] 主页面注入失败: {e}')

    # 所有 iframe
    try:
        frames = page.get_frames()
        for frame in frames:
            try:
                frame.run_js(INJECT_JS)
                injected += 1
            except Exception:
                pass
    except Exception:
        pass

    print(f'[INFO] 注入成功: {injected} 个上下文, url={page.url}')
    return injected > 0

def read_traj_token(page):
    """从主页面和所有 iframe 读取 traj/token"""
    traj, token = None, None
    contexts = [page]
    try:
        contexts += list(page.get_frames())
    except Exception:
        pass
    for ctx in contexts:
        try:
            if not traj:
                t = ctx.run_js('return window._traj;')
                if t and len(t) >= 3:
                    traj = t
            if not token:
                tk = ctx.run_js('return window._ncToken;')
                if tk and isinstance(tk, dict) and tk.get('u_atoken'):
                    token = tk
            done = ctx.run_js('return !!window._trajDone;')
            if done and not traj:
                t = ctx.run_js('return window._traj;')
                if t:
                    traj = t
        except Exception:
            pass
    return traj, token

def collect():
    opts = ChromiumOptions()
    opts.headless(False)
    opts.set_argument('--disable-blink-features=AutomationControlled')

    page = ChromiumPage(addr_or_opts=opts)
    print(f'[INFO] 打开: {REAL_URL}')
    page.get(REAL_URL)
    time.sleep(3)
    try_inject(page)

    print('[INFO] 请在浏览器中手动触发滑块并完成滑动（验证成功后浏览器自动关闭）...')

    timeout = 120
    start    = time.time()
    last_url = page.url
    injected_urls = {last_url}

    while time.time() - start < timeout:
        time.sleep(0.5)
        try:
            cur_url = page.url
            # 页面跳转时重新注入
            if cur_url != last_url and cur_url not in injected_urls:
                print(f'[INFO] 检测到跳转: {cur_url}')
                time.sleep(1.5)
                try_inject(page)
                injected_urls.add(cur_url)
                last_url = cur_url

            traj, token = read_traj_token(page)

            if token:
                print('[INFO] 检测到 NC token，退出等待')
                break
            if traj:
                # 轨迹已完成，再等0.5秒让 fetch 完成
                time.sleep(0.5)
                _, token = read_traj_token(page)
                break

        except PageDisconnectedError:
            print('[WARN] 页面断开，等待重连...')
            time.sleep(2)
            try:
                last_url = page.url
                if last_url not in injected_urls:
                    try_inject(page)
                    injected_urls.add(last_url)
            except Exception:
                pass
        except Exception as e:
            print(f'[WARN] {e}')

    # 最终读取
    traj, token = read_traj_token(page)

    # 保存 pre-activity（滑块前的鼠标移动）
    preact = None
    contexts = [page]
    try:
        contexts += list(page.get_frames())
    except Exception:
        pass
    for ctx in contexts:
        try:
            pa = ctx.run_js('return window._preActivity;')
            if pa and len(pa) >= 10:
                preact = pa
                break
        except Exception:
            pass
    if preact:
        with open(OUT_PREACT, 'w', encoding='utf-8') as f:
            json.dump(preact, f, ensure_ascii=False, indent=2)
        print(f'[INFO] pre-activity 已保存: {len(preact)}点, 时长={preact[-1]["t"]}ms → {OUT_PREACT}')
    else:
        print('[WARN] 未采集到 pre-activity（可能移动鼠标太少）')

    # 保存轨迹
    if traj and len(traj) >= 3:
        with open(OUT_TRAJ, 'w', encoding='utf-8') as f:
            json.dump(traj, f, ensure_ascii=False, indent=2)
        downs = [p for p in traj if p['type']=='mousedown']
        ups   = [p for p in traj if p['type']=='mouseup']
        dx = ups[-1]['x'] - downs[0]['x'] if downs and ups else '?'
        dt = ups[-1]['t'] if ups else '?'
        print(f'[INFO] 轨迹已保存: {len(traj)}点, dx={dx}, 耗时={dt}ms → {OUT_TRAJ}')
    else:
        print('[WARN] 未采集到轨迹')

    # 保存 Token
    if token and token.get('u_atoken'):
        with open(OUT_TOKEN, 'w', encoding='utf-8') as f:
            json.dump(token, f, ensure_ascii=False, indent=2)
        print(f'\n====== NC TOKEN ======')
        print(f'u_atoken   : {token["u_atoken"]}')
        print(f'u_asession : {token["u_asession"]}')
        print(f'u_asig     : {token["u_asig"]}')
        print(f'[INFO] 已保存: {OUT_TOKEN}')
    else:
        print('[INFO] 未捕获到 NC token')

    # 采集 lswucn + cookies
    try:
        lswucn  = page.run_js('return localStorage.getItem("lswucn");')
        etlcd   = page.run_js('return localStorage.getItem("ETLCD");')
        cookies = page.run_js('return document.cookie;')
        lswucn_data = {
            'lswucn': lswucn or '',
            'ETLCD':  etlcd  or 'false',
            'cookies': cookies or '',
        }
        with open(OUT_LSWUCN, 'w', encoding='utf-8') as f:
            json.dump(lswucn_data, f, ensure_ascii=False, indent=2)
        print(f'[INFO] lswucn 已保存: {lswucn[:40] if lswucn else "空"}... → {OUT_LSWUCN}')
    except Exception as e:
        print(f'[WARN] 采集 lswucn 失败: {e}')

    page.quit()
    print('[INFO] 浏览器已关闭')

if __name__ == '__main__':
    collect()
