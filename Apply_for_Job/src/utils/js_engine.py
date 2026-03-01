# -*- coding: utf-8 -*-
import os
import execjs

class JSEngine:
    def __init__(self):
        # 强制指定 Node 运行环境
        os.environ["EXECJS_RUNTIME"] = "Node"
        self.node_env = '''
            const jsdom = require("jsdom");
            const { JSDOM } = jsdom;
            const dom = new JSDOM(`<!DOCTYPE html><script>`);
            window = dom.window;
            document = window.document;
            XMLHttpRequest = window.XMLHttpRequest;
            global.navigator = { userAgent: 'node.js' };
            var res;
        '''

    def get_timestamp_1258(self, script_content: str, url: str) -> str:
        """
        解析51job详情页的动态加密参数
        """
        target_url_placeholder = "_0x48a0dc(_0x319bfa)"
        target_code_placeholder = "_0x3baf44[_0x3e621b]=_0x30f62c;"
        
        insert_code = 'res=_0x30f62c;'
        insert_url = f"_0x48a0dc('{url}')"
        
        # 动态打补丁
        content = script_content.replace(target_code_placeholder, insert_code + target_code_placeholder)
        content = content.replace(target_url_placeholder, insert_url)
        
        try:
            ctx = execjs.compile(self.node_env + content)
            return ctx.eval('res')
        except Exception as e:
            print(f"JS 执行失败: {e}")
            return ""
