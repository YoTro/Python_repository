"""
adapters/liepin.py - 猎聘网 platform adapter

Target URL: https://www.liepin.com/chat/
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
  '.conversation-list .item, .chat-list .item, .contact-list .item, ' +
  '[class*="session-list"] [class*="item"], [class*="chat-session"] [class*="item"]'
);
Array.from(els).forEach(function(el, i) {
  var nameEl = el.querySelector('[class*="name"]');
  var jobEl  = el.querySelector('[class*="job"], [class*="position"], [class*="title"]');
  var msgEl  = el.querySelector('[class*="last-msg"], [class*="content"], [class*="desc"]');
  var badge  = el.querySelector('[class*="badge"], [class*="unread"], [class*="dot"]');
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
var jEl = document.querySelector(
  '[class*="job-name"], [class*="position-name"], [class*="chat-title"]'
);
var cEl = document.querySelector(
  '[class*="company-name"], [class*="corp"], [class*="enterprise-name"]'
);
var hEl = document.querySelector(
  '[class*="hr-name"], [class*="recruiter-name"], [class*="sender-name"], ' +
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
  '[class*="message-item"], [class*="msg-item"], [class*="chat-item"], ' +
  '[class*="dialog-item"]'
);
if (items.length === 0) {
  var list = document.querySelector('[class*="message-list"], [class*="dialog-list"]');
  if (list) items = list.querySelectorAll('li, [class*="item"]');
}
Array.from(items).forEach(function(el) {
  var cls = (el.getAttribute('class') || '');
  var isMine = /\\bright\\b|\\bself\\b|\\bmy-\\b|\\bsend\\b/i.test(cls);
  var textEl = el.querySelector('p, [class*="text"], [class*="content"], [class*="bubble"]');
  var text = textEl ? textEl.innerText.trim() : '';
  if (!text) {
    var lines = el.innerText.trim().split('\\n').map(function(l){return l.trim();}).filter(Boolean);
    text = lines.slice(-1)[0] || '';
  }
  if (text) msgs.push({role: isMine ? 'me' : 'hr', text: text});
});
return msgs;
"""


class LiepinAdapter(PlatformAdapter):
    """猎聘 (https://www.liepin.com) chat adapter."""

    PLATFORM_NAME = "猎聘"
    CHAT_URL      = "https://www.liepin.com/chat/"

    _SIDEBAR_SELECTORS = [
        'css:.conversation-list .item',
        'css:.chat-list .item',
        'css:[class*="session-list"] [class*="item"]',
        'css:[class*="chat-session"] [class*="item"]',
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
            'css:[class*="chat-input"] textarea',
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
