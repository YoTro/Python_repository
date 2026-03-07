// simulate_slide.js - 模拟 NC 滑块滑动，获取 u_atoken/u_asession/u_asig
// 优先从 trajectory.json 加载真实人工鼠标轨迹
require('./env');
const fs = require('fs'), path = require('path');

// 拦截 eval 显示 JSONP 响应
const origEval = global.eval;
global.eval = function interceptEval(code) {
    if (typeof code === 'string' && code.length < 30000 &&
        (code.includes('sessionId') || code.includes('sig') || code.includes('nvcPrepare') || code.includes('analyze'))) {
        console.log('[EVAL analyze/session]', code.substring(0, 400));
    }
    return origEval.call(this, code);
};

// 拦截 loadAndExec 中的所有 JSONP/script src
const origLoadAndExec = require('./env').loadAndExec;
// Also intercept document.createElement to log script src
const _origCreate = document.createElement.bind(document);
let scriptCreateCount = 0;
document.createElement = function(tag) {
    const el = _origCreate(tag);
    if (tag === 'script') {
        scriptCreateCount++;
        const myCount = scriptCreateCount;
        const origSrcDescriptor = Object.getOwnPropertyDescriptor(el, 'src');
        if (origSrcDescriptor) {
            const origSet = origSrcDescriptor.set;
            Object.defineProperty(el, 'src', {
                ...origSrcDescriptor,
                set(url) {
                    if (url) console.log('[SCRIPT SRC]', myCount, url.substring(0, 100));
                    origSet && origSet.call(this, url);
                }
            });
        }
    }
    return el;
};

function loadLocal(file) {
    const code = fs.readFileSync(path.join(__dirname, file), 'utf8');
    origEval.call(global, code);
    console.log('[OK]', file);
}

loadLocal('awsc.js');

// 辅助：创建鼠标事件对象
function makeMouseEvent(type, clientX, clientY) {
    return {
        type, clientX: clientX || 0, clientY: clientY || 0,
        touches: [{ clientX: clientX || 0, clientY: clientY || 0 }],
        preventDefault() {}, stopPropagation() {}
    };
}

// 辅助：模拟滑块滑动
// 优先从 trajectory.json 加载真实人工轨迹；否则回退到合成轨迹
function loadRealTrajectory() {
    const trajFile = path.join(__dirname, '../data/trajectory.json');
    if (!fs.existsSync(trajFile)) return null;
    try {
        const raw = JSON.parse(fs.readFileSync(trajFile, 'utf8'));
        if (!Array.isArray(raw) || raw.length < 3) return null;
        console.log('[TRAJ] 加载真实轨迹:', trajFile, '共', raw.length, '点');
        return raw;
    } catch(e) {
        console.log('[TRAJ] 读取失败:', e.message);
        return null;
    }
}

function simulateSlide(wrapperId) {
    console.log('[SLIDE] Simulating slide for wrapperId:', wrapperId);
    const btnId = 'nc_' + wrapperId + '_n1z';
    const btnEl = document._mockDivCache[btnId];
    if (!btnEl) {
        console.log('[SLIDE] Button not found:', btnId, ', cached IDs:', Object.keys(document._mockDivCache));
        return false;
    }
    console.log('[SLIDE] Found button element:', btnId, 'onmousedown:', typeof btnEl.onmousedown);

    if (typeof btnEl.onmousedown !== 'function') {
        console.log('[SLIDE] onmousedown not set yet');
        return false;
    }

    // 真实浏览器断点捕获的按钮尺寸: left=395, top=296.4375, width=48, height=48
    btnEl.offsetWidth = 48;
    btnEl.offsetHeight = 48;
    btnEl.offsetLeft = 0;
    btnEl.offsetTop = 0;
    btnEl.getBoundingClientRect = () => ({x:395, y:296.4375, left:395, top:296.4375, width:48, height:48, right:443, bottom:344.4375});

    // 尝试加载真实人工轨迹
    const realTraj = loadRealTrajectory();

    let startX, startY, delays, slideEnd;

    if (realTraj) {
        // === 使用真实人工轨迹 ===
        const downPt = realTraj.find(p => p.type === 'mousedown') || realTraj[0];
        const upPt   = realTraj.slice().reverse().find(p => p.type === 'mouseup') || realTraj[realTraj.length - 1];

        // 将真实坐标平移到 mock 按钮中心（x=100, y=446），保留相对位移
        // mock 按钮: getBoundingClientRect x=93,width=44 → 中心=115，取100(靠左)
        // 真实按钮中心: x=395+24=419, y=296.4375+24=320
        const MOCK_START_X = 419;
        const MOCK_START_Y = 320;
        const xOffset = MOCK_START_X - downPt.x;
        const yOffset = MOCK_START_Y - downPt.y;

        const translated = realTraj.map(p => ({
            ...p,
            x: p.x + xOffset,
            y: p.y + yOffset
        }));

        startX   = MOCK_START_X;
        startY   = MOCK_START_Y;
        slideEnd = upPt.x + xOffset;

        console.log('[SLIDE] 使用真实轨迹(已平移): startX=' + startX + ' slideEnd=' + slideEnd
            + ' xOffset=' + xOffset + ' 共' + translated.length + '点');

        // mousedown
        try {
            btnEl.onmousedown(makeMouseEvent('mousedown', startX, startY));
            console.log('[SLIDE] mousedown fired OK');
        } catch(e) {
            console.error('[SLIDE] mousedown error:', e.message);
            return false;
        }

        // mousemove 点（使用平移后坐标）
        const moves = translated.filter(p => p.type === 'mousemove');
        moves.forEach(({ x, y, t }) => {
            setTimeout(() => {
                try {
                    const evt = makeMouseEvent('mousemove', x, y);
                    global.dispatchEvent(evt);
                    document.dispatchEvent(evt);
                } catch(e) { console.error('[SLIDE mousemove err]', e.message); }
            }, t);
        });

        // mouseup
        const upDelay = upPt.t + 30;
        setTimeout(() => {
            try {
                const upEvt = makeMouseEvent('mouseup', slideEnd, upPt.y);
                global.dispatchEvent(upEvt);
                document.dispatchEvent(upEvt);
                console.log('[SLIDE] mouseup fired at x=' + slideEnd);
            } catch(e) { console.error('[SLIDE mouseup err]', e.message); }
        }, upDelay);

    } else {
        // === 回退：合成轨迹 ===
        console.log('[SLIDE] 未找到 trajectory.json，使用合成轨迹');
        startX = 419;
        startY = 320;
        slideEnd = startX + 260;
        const steps = 35;
        delays = [];
        let cumTime = 0;
        for (let i = 1; i <= steps; i++) {
            const progress = i / steps;
            const eased = progress < 0.5
                ? 2 * progress * progress
                : -1 + (4 - 2 * progress) * progress;
            const x = Math.round(startX + (slideEnd - startX) * eased);
            const yJitter = Math.round((Math.random() - 0.5) * 3);
            const baseStep = i < 5 ? 25 : (i > 30 ? 22 : 16);
            cumTime += Math.round(baseStep + Math.random() * 8);
            delays.push({ x, y: startY + yJitter, t: cumTime });
        }

        try {
            btnEl.onmousedown(makeMouseEvent('mousedown', startX, startY));
            console.log('[SLIDE] mousedown fired OK');
        } catch(e) {
            console.error('[SLIDE] mousedown error:', e.message);
            return false;
        }

        delays.forEach(({ x, y, t: delay }) => {
            setTimeout(() => {
                try {
                    const evt = makeMouseEvent('mousemove', x, y);
                    global.dispatchEvent(evt);
                    document.dispatchEvent(evt);
                } catch(e) { console.error('[SLIDE mousemove err]', e.message); }
            }, delay);
        });

        const totalTime = delays[delays.length - 1].t + 60;
        setTimeout(() => {
            try {
                const upEvt = makeMouseEvent('mouseup', slideEnd, startY);
                global.dispatchEvent(upEvt);
                document.dispatchEvent(upEvt);
                console.log('[SLIDE] mouseup fired at x=' + slideEnd);
            } catch(e) { console.error('[SLIDE mouseup err]', e.message); }
        }, totalTime);
    }

    return true;
}

setTimeout(() => {
    // 读取 challenge.json（由 get_challenge.py 生成）
    const challengeFile = path.join(__dirname, '../data/challenge.json');
    let challenge = {};
    if (fs.existsSync(challengeFile)) {
        try {
            challenge = JSON.parse(fs.readFileSync(challengeFile, 'utf8'));
            console.log('[CHALLENGE] u_atoken:', challenge.u_atoken);
            console.log('[CHALLENGE] u_aref  :', challenge.u_aref);
        } catch(e) { console.log('[CHALLENGE] 读取失败:', e.message); }
    } else {
        console.log('[CHALLENGE] 未找到 challenge.json，token 将由 NC 自动生成');
    }

    global.AWSC.use('nc', (state, ncMod) => {
        if (state !== 'loaded') return;
        console.log('[NC] loaded, TEST_PASS:', ncMod.TEST_PASS);

        const ncInst = ncMod.init({
            renderTo: 'nocaptcha',
            appkey: 'CF_APP_WAF',
            scene: 'register',
            href: 'https://we.51job.com/api/job/search-pc',
            token: challenge.u_atoken || undefined,  // 使用服务端预生成的 token
            comm: {},
            success: (data) => {
                const u_atoken   = challenge.u_atoken || data.token;
                const u_asession = data.sessionId;
                const u_asig     = data.sig;
                const u_aref     = challenge.u_aref   || data.aref || '';
                console.log('\n====== NC SUCCESS ======');
                console.log('u_atoken  :', u_atoken);
                console.log('u_asession:', u_asession);
                console.log('u_asig    :', u_asig);
                console.log('u_aref    :', u_aref);
                // 输出可直接拼接到 URL 的字符串
                const params = `u_atoken=${encodeURIComponent(u_atoken)}&u_asession=${encodeURIComponent(u_asession)}&u_asig=${encodeURIComponent(u_asig)}&u_aref=${encodeURIComponent(u_aref)}`;
                console.log('\n[URL PARAMS]', params);
                // 保存到文件供 Python 读取
                const result = { u_atoken, u_asession, u_asig, u_aref };
                fs.writeFileSync(path.join(__dirname, '../data/nc_result.json'), JSON.stringify(result, null, 2));
                console.log('[INFO] 已保存 nc_result.json');
            },
            fail: (data) => { console.log('[NC FAIL]', data); },
            error: (e) => { console.log('[NC ERROR]', e); }
        });

        // 等待滑块 UI 加载（loadFY 完成后 n() 被调用设置 onmousedown）
        // 优先加载真实 pre-activity；否则回退合成轨迹
        (function replayPreActivity() {
            const preFile = path.join(__dirname, '../data/pre_activity.json');
            let moves = null;
            if (fs.existsSync(preFile)) {
                try {
                    const raw = JSON.parse(fs.readFileSync(preFile, 'utf8'));
                    if (Array.isArray(raw) && raw.length >= 10) {
                        moves = raw;
                        console.log('[PRE] 加载真实 pre-activity:', raw.length, '点, 时长', raw[raw.length-1].t, 'ms');
                    }
                } catch(e) { console.log('[PRE] 读取失败:', e.message); }
            }

            if (moves) {
                // === 真实 pre-activity 回放 ===
                // 坐标平移：将真实屏幕坐标平移到 mock 坐标系
                // 真实按钮中心: (419, 320)
                const last = moves[moves.length - 1];
                const MOCK_BTN_X = 419, MOCK_BTN_Y = 320;
                const xOff = MOCK_BTN_X - last.x;
                const yOff = MOCK_BTN_Y - last.y;
                moves.forEach(({ x, y, t }) => {
                    setTimeout(() => {
                        try {
                            const e = makeMouseEvent('mousemove', x + xOff, y + yOff);
                            global.dispatchEvent(e);
                            document.dispatchEvent(e);
                        } catch(err) {}
                    }, t);
                });
            } else {
                // === 回退：合成轨迹 ===
                console.log('[PRE] 未找到 pre_activity.json，使用合成轨迹');
                let t = 200;
                const moves2 = [];
                let cx = 200, cy = 300;
                for (let i = 0; i < 60; i++) {
                    cx += Math.round((Math.random() - 0.5) * 30);
                    cy += Math.round((Math.random() - 0.5) * 20);
                    cx = Math.max(50, Math.min(350, cx));
                    cy = Math.max(200, Math.min(700, cy));
                    moves2.push([cx, cy, Math.round(25 + Math.random() * 15)]);
                }
                const tx = 93, ty = 425;
                for (let i = 0; i < 80; i++) {
                    const p = i / 80;
                    cx = Math.round(cx + (tx - cx) * p * 0.15 + (Math.random() - 0.5) * 10);
                    cy = Math.round(cy + (ty - cy) * p * 0.15 + (Math.random() - 0.5) * 8);
                    moves2.push([cx, cy, Math.round(20 + Math.random() * 15)]);
                }
                for (let i = 0; i < 60; i++) {
                    cx = Math.round(93 + (Math.random() - 0.5) * 15);
                    cy = Math.round(425 + (Math.random() - 0.5) * 10);
                    moves2.push([cx, cy, Math.round(30 + Math.random() * 20)]);
                }
                moves2.forEach(([x, y, dt]) => {
                    t += dt;
                    setTimeout(() => {
                        const e = makeMouseEvent('mousemove', x, y);
                        global.dispatchEvent(e);
                        document.dispatchEvent(e);
                    }, t);
                });
            }
        })();

        // 根据真实 pre-activity 时长动态计算滑块触发延迟
        // 最少等 4s（让 fireyejs 初始化），最多等 pre-activity 时长 + 1s
        let slideDelay = 6000;
        try {
            const preFile2 = path.join(__dirname, '../data/pre_activity.json');
            if (fs.existsSync(preFile2)) {
                const preRaw = JSON.parse(fs.readFileSync(preFile2, 'utf8'));
                if (Array.isArray(preRaw) && preRaw.length >= 10) {
                    const duration = preRaw[preRaw.length - 1].t;
                    slideDelay = Math.max(4000, Math.min(duration + 1000, 30000));
                    console.log('[PRE] 滑块触发延迟:', slideDelay, 'ms (pre-activity时长:', duration, 'ms)');
                }
            }
        } catch(e) {}

        // 等待 pre-activity 回放完成再尝试滑块
        let slideAttempts = 0;
        setTimeout(function trySlideOnce() {
            slideAttempts++;
            console.log('[TRY SLIDE] attempt', slideAttempts);

            // wrapperId 从 1 开始 (__awscnc_wrapper_id__ 计数器)
            const wid = global.__awscnc_wrapper_id__ || 1;
            console.log('[TRY SLIDE] cache keys:', Object.keys(document._mockDivCache));

            if (!simulateSlide(wid)) {
                if (slideAttempts < 10) setTimeout(trySlideOnce, 1000);
                else console.log('[SLIDE] Giving up after', slideAttempts, 'attempts');
            }
        }, slideDelay);

    }, {timeout: 8000});
    loadLocal('nc.js');
    setTimeout(() => process.exit(0), 40000);
}, 100);
