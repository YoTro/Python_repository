"""
adapters/zhipin.py - BOSS直聘 platform adapter

Verified selectors (live DOM probe 2026-04-18):
  Sidebar item   : .friend-content
  HR name        : .name-text
  Unread badge   : .notice-badge
  Job title      : .top-info-content .name
  Salary         : .top-info-content .salary
  Company        : .user-info-wrap lines[1]
  HR title       : .user-info-wrap lines[2]
  Messages       : .message-item  (.item-friend = HR, .item-myself = me)
  Message text   : .text  (skip items without .text = system cards)
  Input          : div.chat-input  (contenteditable)
  Send button    : .btn-send
"""
from __future__ import annotations

import logging
import random
import time

from ..base import PlatformAdapter

logger = logging.getLogger(__name__)

# ── JavaScript helpers ────────────────────────────────────────────────

_JS_LIST = """
var items = [];
var els = document.querySelectorAll('.friend-content');
Array.from(els).forEach(function(el, i) {
  var nameEl  = el.querySelector('.name-text');
  var jobEl   = el.querySelector('.job-text');
  var lastEl  = el.querySelector('.last-msg-text');
  var timeEl  = el.querySelector('.time');
  var badge   = el.querySelector('.notice-badge');
  items.push({
    index:   i,
    name:    nameEl ? nameEl.innerText.trim() : '',
    job:     jobEl  ? jobEl.innerText.trim()  : '',
    preview: lastEl ? lastEl.innerText.trim() : '',
    time:    timeEl ? timeEl.innerText.trim() : '',
    unread:  !!(badge && badge.innerText.trim() !== '' && badge.innerText.trim() !== '0')
  });
});
return items;
"""

_JS_HEADER = """
var h = {job_title: '', company: '', hr_name: '', salary: ''};

// Job title: .position-name (confirmed via live DOM probe)
var posEl = document.querySelector('.position-name');
if (posEl) h.job_title = posEl.innerText.trim();

// Salary: inside .chat-position-content
var salEl = document.querySelector('.chat-position-content .salary');
if (salEl) h.salary = salEl.innerText.trim();

// HR name + company: .user-info-wrap  lines = [HR_name, company, HR_title, ...]
var infoEl = document.querySelector('.user-info-wrap');
if (infoEl) {
  var lines = infoEl.innerText.trim().split('\\n')
    .map(function(l){ return l.trim(); }).filter(Boolean);
  h.hr_name = lines[0] || '';
  h.company = lines[1] || '';
}
return h;
"""

_JS_MESSAGES = """
var msgs = [];
var items = document.querySelectorAll('.message-item');
Array.from(items).forEach(function(el) {
  var cls    = el.getAttribute('class') || '';
  var isMine = cls.indexOf('item-myself') !== -1;
  var isHR   = cls.indexOf('item-friend') !== -1;
  if (!isMine && !isHR) return;   // skip system messages

  var textEl = el.querySelector('.text');
  if (!textEl) return;            // skip non-text cards (image, system card)
  
  // Confirmed: use .innerText for message text
  // For item-myself, innerText may include read-receipt lines ("已读" / "未读")
  // before the actual message — strip them so sent_texts matching works correctly.
  var text = textEl.innerText.trim();
  if (isMine) {
    text = text.split('\\n').filter(function(l) {
      var t = l.trim();
      return t && t !== '已读' && t !== '未读';
    }).join('\\n').trim();
  }
  if (text) msgs.push({role: isMine ? 'me' : 'hr', text: text});
});
return msgs;
"""

_JS_MSG_COUNT = """
var count = 0;
document.querySelectorAll('.message-item').forEach(function(el) {
  var cls = el.getAttribute('class') || '';
  if ((cls.indexOf('item-myself') !== -1 || cls.indexOf('item-friend') !== -1)
      && el.querySelector('.text')) count++;
});
return count;
"""


class ZhipinAdapter(PlatformAdapter):
    """BOSS直聘 (https://www.zhipin.com) chat adapter."""

    PLATFORM_NAME = "BOSS直聘"
    CHAT_URL      = "https://www.zhipin.com/web/geek/chat"

    def list_conversations(self) -> list[dict]:
        items = self._tab.run_js(_JS_LIST)
        return items or []

    def open_conversation(self, index: int) -> dict:
        items = self._tab.eles('css:.friend-content')
        if not items or index >= len(items):
            raise IndexError(
                f"Conversation index {index} out of range "
                f"(found {len(items) if items else 0})"
            )
        items[index].click()
        time.sleep(2)
        self._load_full_history()

        header = self._tab.run_js(_JS_HEADER) or {}
        return {
            'job_title': header.get('job_title', ''),
            'company':   header.get('company',   ''),
            'hr_name':   header.get('hr_name',   ''),
            'salary':    header.get('salary',    ''),
        }

    def _load_full_history(self, max_scrolls: int = 10) -> None:
        """
        Scroll the chat record to the top repeatedly until no new messages
        appear, ensuring the full conversation history is loaded.
        BOSS直聘 lazy-loads older messages on upward scroll.
        """
        for _ in range(max_scrolls):
            prev = self.message_count()
            # Scroll the message container to the very top
            self._tab.run_js("""
            var el = document.querySelector('.chat-record');
            if (el) el.scrollTop = 0;
            """)
            time.sleep(1.2)
            # Also click "load more" button if present
            self._tab.run_js("""
            var btn = document.querySelector('.loading-more, .load-more-btn');
            if (btn && btn.click) btn.click();
            """)
            time.sleep(0.5)
            if self.message_count() == prev:
                break  # no new messages loaded, history is complete
        logger.debug("[%s] Full history loaded: %d messages", self.PLATFORM_NAME, self.message_count())

    def read_messages(self) -> list[dict]:
        msgs = self._tab.run_js(_JS_MESSAGES)
        return msgs or []

    def message_count(self) -> int:
        count = self._tab.run_js(_JS_MSG_COUNT)
        return count if isinstance(count, int) else 0

    def send_message(self, text: str) -> bool:
        input_el = self._tab.ele('css:div.chat-input', timeout=5)
        if not input_el:
            logger.error("[%s] Cannot find div.chat-input", self.PLATFORM_NAME)
            return False

        try:
            input_el.click()
            time.sleep(0.3)
            input_el.input(text)
            time.sleep(0.5)

            send_btn = self._tab.ele('css:.btn-send', timeout=2)
            if send_btn:
                send_btn.click()
            else:
                input_el.input('\n')

        except Exception as e:
            logger.exception("[%s] Error sending: %s", self.PLATFORM_NAME, e)
            return False

        time.sleep(random.uniform(1.5, 3.0))
        return True
