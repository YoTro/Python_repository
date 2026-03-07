// patch_fy.js - 系统性防护版：对所有单字母寄存器的属性访问用 ?? 守护
const fs = require('fs');
const path = require('path');
let code = fs.readFileSync(path.join(__dirname, 'fireyejs.js'), 'utf8');

// 1. 移除 console.log(d[x]) 噪音
code = code.replace('console.log(d[x]),', '');
console.log('[1] console.log removed');

// 2. d[1][d[8]] 调用安全化
code = code.replace(
    'd[1][d[8]](d[4],d[i])',
    '(typeof d[1][d[8]]==="function"?d[1][d[8]](d[4],d[i]):null)'
);
console.log('[2] d[1][d[8]] call guard applied');

// 3. 注入 __safeNull 和 __safeMethod（动态方法调用安全包装）
const safePrefix = `
var __safeNull=(function(){
  var sn=new Proxy(function safeNull(){return sn;},{
    get:function(t,p){
      if(p==='length')return 0;
      if(p==='toString'||p==='toJSON')return function(){return '';};
      if(p==='valueOf')return function(){return 0;};
      if(typeof p==='symbol')return undefined;
      return sn;
    },
    apply:function(){return sn;},
    construct:function(){return sn;}
  });
  return sn;
})();
function __safeMethod(obj,method){
  if(obj==null)return __safeNull;
  var fn=obj[method];
  if(typeof fn==='function')return fn.bind(obj);
  return __safeNull;
}
`;
code = code.replace('!function(){', '!function(){' + safePrefix);
console.log('[3] __safeNull injected');

// 3b. 特别处理 d[1][d[N]]( 函数调用（在通用 guard 之前）
// d[1] 是 WebGL context 或 Math 对象，d[N] 是动态方法名
// 替换为 __safeMethod(d[1],d[N])( 确保方法不存在时不崩溃
const d1CallNums = [0,1,2,3,4,5,6,7,8,9];
let d1Cnt = 0;
for (const n of d1CallNums) {
    const from = `d[1][d[${n}]](`;
    const to   = `__safeMethod(d[1],d[${n}])(`;
    while(code.includes(from)){ code=code.replace(from,to); d1Cnt++; }
}
console.log('[3b] d[1][d[N]]( call guards:', d1Cnt);

// 3c. v[C[X]]( 函数调用安全化（v 是 VM 局部变量，可能未初始化）
// 包括 v[C[H]]( v[C[E]]( v[C[p]]( v[C[t]]( 等
const vCallRegs = ['E','l','m','x','S','\\$','L','k','p','t','i','G','F','a','b','X','I','H','g','_','T','c','w','h','u','V','K','Y','o'];
let vCnt = 0;
for (const reg of vCallRegs) {
    const letter = reg.replace('\\$','$');
    const from = `v[C[${letter}]](`;
    const to   = `__safeMethod(v,C[${letter}])(`;
    while(code.includes(from)){ code=code.replace(from,to); vCnt++; }
}
// 也处理数字索引
for (const n of [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,45,70,72,73,74]) {
    const from = `v[C[${n}]](`;
    const to   = `__safeMethod(v,C[${n}])(`;
    while(code.includes(from)){ code=code.replace(from,to); vCnt++; }
}
console.log('[3c] v[C[X]]( call guards:', vCnt);

// 4. 系统性替换：所有 d[REGISTER][C[ 和 d[REGISTER][d[ 模式
// fireyejs 中单字母寄存器常量：E,l,m,x,S,$,L,k,p,t,i,G,F,a,b,X,I,H,g,_,T,c,w,h,u,V,K,Y,o
// 使用 ?? 而非 ||，只在 null/undefined 时才替换（不影响 0/false/"" 等有效值）
// 也处理数字寄存器 1,2,3,4,5,6,8,9 等小数字
const regs = [
    'E','l','m','x','S','\\$','L','k','p','t','i','G','F','a','b','X','I','H','g','_','T','c','w','h','u','V','K','Y','o',
    '0','1','2','3','4','5','6','7','8','9'
];

let totalCnt = 0;
for (const reg of regs) {
    // 匹配 d[reg][C[ 和 d[reg][d[，用 ?? __safeNull 守护
    // 注意：$需要转义，其他字母不需要
    const patternC = `d[${reg.replace('\\$','$')}][C[`;
    const patternD = `d[${reg.replace('\\$','$')}][d[`;
    let cnt = 0;
    while(code.includes(patternC)){
        code = code.replace(patternC, `(d[${reg.replace('\\$','$')}]??__safeNull)[C[`);
        cnt++;
    }
    while(code.includes(patternD)){
        code = code.replace(patternD, `(d[${reg.replace('\\$','$')}]??__safeNull)[d[`);
        cnt++;
    }
    if (cnt > 0) {
        console.log(`[4] d[${reg.replace('\\$','$')}] guards: ${cnt}`);
        totalCnt += cnt;
    }
}
console.log('[4] Total guards:', totalCnt);

// 4b. 系统性替换：所有 d[REGISTER][localVar] 模式（非 C[, 非 d[）
// 这些模式用本地变量名作为属性键访问 d 寄存器，同样需要 null guard
const localVarGuardRe = /d\[([a-zA-Z0-9$_]+)\]\[(?!C\[|d\[|\d)([a-zA-Z_$][a-zA-Z0-9_$]*)\]/g;
let localVarCnt = 0;
code = code.replace(localVarGuardRe, (match, reg, varName) => {
    localVarCnt++;
    return `(d[${reg}]??__safeNull)[${varName}]`;
});
console.log('[4b] d[X][localvar] guards:', localVarCnt);

// 4c. 处理 r=d[$],B=d[5],r[B]=value 这类 VM 行为追踪赋值
// r 被赋为 d[$]（可能 undefined），然后 r[B]=value 崩溃
// 只替换 r=d[$], 后跟逗号的情况（排除 r=d[$]?...布尔判断）
let rBCnt = 0;
while (code.includes('r=d[$],')) {
    code = code.replace('r=d[$],', 'r=d[$]??__safeNull,');
    rBCnt++;
}
for (const n of [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18]) {
    const from = `r=d[${n}],`;
    const to = `r=d[${n}]??__safeNull,`;
    while (code.includes(from)) { code = code.replace(from, to); rBCnt++; }
}
console.log('[4c] r=d[X], guards:', rBCnt);

// 4d. 处理 (r=d[X])[ 内联赋值后立即做属性访问的模式
// (r=d[1])[P]=value → r=d[1]时可能undefined, r[P]就崩溃
// 替换为 (r=d[X]??__safeNull)[
let rdCnt = 0;
const rdPatterns = ['d[$]','d[1]','d[x]','d[k]','d[2]','d[3]','d[4]','d[5]','d[6]','d[7]','d[8]','d[9]',
    'd[E]','d[l]','d[m]','d[S]','d[L]','d[G]','d[b]','d[t]','d[I]'];
for (const pat of rdPatterns) {
    const from = `(r=${pat})[`;
    const to = `(r=${pat}??__safeNull)[`;
    while (code.includes(from)) { code = code.replace(from, to); rdCnt++; }
    // 也处理已被守护的版本 (r=(d[X]??__safeNull)[d[Y]])[
    // 这在步骤4之后会出现
}
// 处理 (r=(d[7]??__safeNull)[d[4]])[ 这种两层访问
const from2 = '(r=(d[7]??__safeNull)[d[4]])[';
const to2   = '(r=((d[7]??__safeNull)[d[4]])??__safeNull)[';
while (code.includes(from2)) { code = code.replace(from2, to2); rdCnt++; }
console.log('[4d] (r=d[X])[ guards:', rdCnt);
const vGuardFrom = 'z=Z=s,Z=v[C[p]](z)';
const vGuardTo   = 'z=Z=s,Z=(typeof v==="object"&&v!==null&&typeof v[C[p]]==="function"?v[C[p]](z):null)';
if (code.includes(vGuardFrom)) {
    code = code.replace(vGuardFrom, vGuardTo);
    console.log('[5] v[C[p]] call guard applied');
} else {
    console.log('[5] v[C[p]] pattern not found');
}

// 6. xf 函数中的 z[d[2]](d[1]) 安全化
// z 可能是非函数对象（如数字、undefined）
const zGuardFrom = 'z[d[2]](d[1])';
const zGuardTo   = '(typeof z!=="undefined"&&z!==null&&typeof z[d[2]]==="function"?z[d[2]](d[1]):null)';
if (code.includes(zGuardFrom)) {
    code = code.replace(zGuardFrom, zGuardTo);
    console.log('[6] z[d[2]] call guard applied');
} else {
    console.log('[6] z[d[2]] pattern not found');
}

// 7. xf 函数中的 window[d[0]] 安全化
const wdGuardFrom = 'window[d[0]]';
const wdGuardTo   = '(typeof window!=="undefined"&&window!=null?window[d[0]]:undefined)';
if (code.includes(wdGuardFrom)) {
    code = code.replace(wdGuardFrom, wdGuardTo);
    console.log('[7] window[d[0]] guard applied');
} else {
    console.log('[7] window[d[0]] pattern not found');
}

// 6. 写出
fs.writeFileSync(path.join(__dirname, 'fireyejs_debug.js'), code);
console.log('[DONE] fireyejs_debug.js written:', code.length, 'bytes');
