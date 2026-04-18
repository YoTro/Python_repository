"""
adapters/lagou.py - 拉勾网 platform adapter

Target URL: https://www.lagou.com/im/
"""
from __future__ import annotations

import logging
import random
import time

from ..base import PlatformAdapter

logger = logging.getLogger(__name__)

_JS_LIST = """
var items = [];
var els = document.querySelectorAll(
  '.chat-list li, .conversation-list li, .contact-list li, ' +
  '[class*="chat-list"] [class*="item"], [class*="im-list"] [class*="item"]'
);
Array.from(els).forEach(function(el, i) {
  var nameEl = el.querySelector('[class*="name"], [class*="title"]');
  var jobEl  = el.querySelector('[class*="job"], [class*="position"]');
  var msgEl  = el.querySelector('[class*="last"], [class*="preview"], [class*="desc"]');
  var badge  = el.querySelector('[class*="badge"], [class*="unread"], [class*="count"]');
  items.push({
    index:   i,
    name:    nameEl ? nameEl.innerText.trim() : el.innerText.trim().split('\\n')[0],
    job:     jobEl  ? jobEl.innerText.trim()  : '',
    preview: msgEl  ? msgEl.innerText.trim()  : '',
    unread:  !!(badge && badge.innerText.trim() !== '' && badge.innerText.trim() !== '0')
  });
});
return items;
"""

_JS_HEADER = """
var h = {job_title: '', company: '', hr_name: ''};
// Lagou shows job info in the chat header panel
var jEl = document.querySelector(
  '[class*="position-name"], [class*="job-name"], [class*="chat-job"], h3, h2'
);
var cEl = document.querySelector(
  '[class*="company-name"], [class*="com-name"], [class*="corp-name"]'
);
var hEl = document.querySelector(
  '[class*="hr-name"], [class*="user-name"], [class*="contact-name"], ' +
  '[class*="chat-header"] [class*="name"]'
);
if (jEl) h.job_title = jEl.innerText.trim();
if (cEl) h.company   = cEl.innerText.trim();
if (hEl) h.hr_name   = hEl.innerText.trim();
return h;
"""

_JS_MESSAGES = """
var msgs = [];
var items = document.querySelectorAll(
  '[class*="chat-content"] [class*="item"], [class*="message-list"] [class*="item"], ' +
  '[class*="im-msg"] [class*="item"], [class*="msg-list"] li'
);
if (items.length === 0) {
  var list = document.querySelector('[class*="chat-content"], [class*="message-wrap"]');
  if (list) items = list.querySelectorAll('li, [class*="item"], [class*="row"]');
}
Array.from(items).forEach(function(el) {
  var cls = (el.getAttribute('class') || '') + ' ' +
    (el.parentElement ? el.parentElement.getAttribute('class') || '' : '');
  var isMine = /\\bright\\b|\\bself\\b|\\bme\\b|\\bsend\\b|\\bright-msg\\b/i.test(cls);
  var textEl = el.querySelector('p, [class*="text"], [class*="content"]');
  var text = textEl ? textEl.innerText.trim() : el.innerText.trim().split('\\n').slice(-1)[0];
  if (text) msgs.push({role: isMine ? 'me' : 'hr', text: text});
});
return msgs;
"""


class LagouAdapter(PlatformAdapter):
    """拉勾网 (https://www.lagou.com) chat adapter."""

    PLATFORM_NAME = "拉勾"
    CHAT_URL      = "https://www.lagou.com/im/"

    _SIDEBAR_SELECTORS = [
        'css:.chat-list li',
        'css:.conversation-list li',
        'css:[class*="im-list"] [class*="item"]',
        'css:[class*="chat-list"] [class*="item"]',
    ]

    def list_conversations(self) -> list[dict]:
        items = self._tab.run_js(_JS_LIST)
        return items or []

    def open_conversation(self, index: int) -> dict:
        items = None
        for sel in self._SIDEBAR_SELECTORS:
            found = self._tab.eles(sel)
            if found:
                items = found
                break

        if not items or index >= len(items):
            raise IndexError(
                f"Conversation index {index} out of range "
                f"(found {len(items) if items else 0})"
            )

        items[index].click()
        time.sleep(2)

        header = self._tab.run_js(_JS_HEADER) or {}
        return {
            'job_title': header.get('job_title', ''),
            'company':   header.get('company',   ''),
            'hr_name':   header.get('hr_name',   ''),
        }

    def read_messages(self) -> list[dict]:
        msgs = self._tab.run_js(_JS_MESSAGES)
        return msgs or []

    def send_message(self, text: str) -> bool:
        input_el = self._tab.ele(
            'css:div[contenteditable="true"], css:textarea[class*="input"], '
            'css:[class*="chat-input"] textarea, css:[class*="im-input"]',
            timeout=5,
        )
        if not input_el:
            logger.error("[%s] Cannot find input element", self.PLATFORM_NAME)
            return False

        try:
            input_el.click()
            time.sleep(0.3)
            input_el.input(text)
            time.sleep(0.5)

            # Try send button
            send_btn = self._tab.ele(
                'css:[class*="send-btn"], css:button[class*="send"]', timeout=2
            )
            if not send_btn:
                for btn in self._tab.eles('tag:button'):
                    if btn.text and '发送' in btn.text:
                        send_btn = btn
                        break

            if send_btn:
                send_btn.click()
            else:
                input_el.input('\n')

        except Exception as e:
            logger.exception("[%s] Error sending: %s", self.PLATFORM_NAME, e)
            return False

        time.sleep(random.uniform(1.5, 3.0))
        return True
