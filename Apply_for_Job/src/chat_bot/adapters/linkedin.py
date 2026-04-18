"""
adapters/linkedin.py - LinkedIn Messaging adapter

Target URL: https://www.linkedin.com/messaging/

Notes:
  - LinkedIn uses React; class names contain random hashes (e.g. "t-14 t-black--light")
    so we target stable data-* attributes and ARIA roles where possible.
  - Only works for accepted connection messages or InMail threads.
  - Job title / company are inferred from the contact's profile headline
    shown in the conversation header (not always the specific role discussed).
"""
from __future__ import annotations

import logging
import random
import time

from ..base import PlatformAdapter

logger = logging.getLogger(__name__)

_JS_LIST = """
var items = [];
// LinkedIn conversation list items
var els = document.querySelectorAll(
  'li.msg-conversation-listitem, ' +
  '[class*="msg-conversation-listitem"], ' +
  '[data-control-name="overlay.open_conv"]'
);
Array.from(els).forEach(function(el, i) {
  var nameEl = el.querySelector('[class*="participant-name"], strong, h3');
  var msgEl  = el.querySelector('[class*="msg-conversation-card__message-snippet"],' +
                                '[class*="last-msg"], p');
  var badge  = el.querySelector('[class*="notification-badge"], [class*="unread"]');
  var hasBadge = !!(badge && getComputedStyle(badge).display !== 'none');
  items.push({
    index:   i,
    name:    nameEl ? nameEl.innerText.trim() : '',
    job:     '',   // LinkedIn sidebar doesn't show job title
    preview: msgEl ? msgEl.innerText.trim() : '',
    unread:  hasBadge
  });
});
return items;
"""

_JS_HEADER = """
var h = {job_title: '', company: '', hr_name: ''};
// Open conversation header: shows name + headline
var nameEl = document.querySelector(
  '.msg-thread__link-to-profile, [class*="participant-metadata"] [class*="name"], ' +
  '[class*="msg-entity-lockup__entity-title"]'
);
var headlineEl = document.querySelector(
  '[class*="participant-metadata"] [class*="subline"], ' +
  '[class*="msg-entity-lockup__entity-subtitle"]'
);
if (nameEl)     h.hr_name   = nameEl.innerText.trim();
if (headlineEl) {
  // Headline is often "Title at Company" — split on " at " or " @ "
  var headline = headlineEl.innerText.trim();
  var parts = headline.split(/ at | @ | · /);
  h.job_title = parts[0] ? parts[0].trim() : headline;
  h.company   = parts[1] ? parts[1].trim() : '';
}
return h;
"""

_JS_MESSAGES = """
var msgs = [];
var items = document.querySelectorAll(
  '.msg-s-event-listitem, [class*="msg-s-event-listitem"]'
);
Array.from(items).forEach(function(el) {
  // Skip system events (e.g. "connected", date headers)
  if (el.querySelector('[class*="system-message"], [class*="date-divider"]')) return;
  // LinkedIn marks own messages with data-is-auto-send or a specific subclass
  var cls = el.getAttribute('class') || '';
  // "other-participant" = HR (the other person), absence = me
  var isHR = /other-participant/i.test(cls);
  var textEl = el.querySelector('[class*="msg-s-event__content"] p, ' +
                                '.msg-s-event__content p, ' +
                                '[class*="msg-content"] p, p');
  var text = textEl ? textEl.innerText.trim() : '';
  if (text) msgs.push({role: isHR ? 'hr' : 'me', text: text});
});
return msgs;
"""


class LinkedInAdapter(PlatformAdapter):
    """LinkedIn Messaging adapter (https://www.linkedin.com/messaging/)."""

    PLATFORM_NAME = "LinkedIn"
    CHAT_URL      = "https://www.linkedin.com/messaging/"

    _SIDEBAR_SELECTORS = [
        'css:li.msg-conversation-listitem',
        'css:[class*="msg-conversation-listitem"]',
        'css:[data-control-name="overlay.open_conv"]',
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
        # LinkedIn uses a contenteditable div with a specific class
        input_el = self._tab.ele(
            'css:div.msg-form__contenteditable, '
            'css:[class*="msg-form__contenteditable"], '
            'css:div[contenteditable="true"]',
            timeout=5,
        )
        if not input_el:
            logger.error("[%s] Cannot find message input", self.PLATFORM_NAME)
            return False

        try:
            input_el.click()
            time.sleep(0.3)
            input_el.input(text)
            time.sleep(0.5)

            # LinkedIn send button is type="submit" inside the form
            send_btn = self._tab.ele(
                'css:button.msg-form__send-button, '
                'css:button[class*="send-button"], '
                'css:button[type="submit"]',
                timeout=2,
            )
            if send_btn:
                send_btn.click()
            else:
                # Ctrl+Enter or Enter as fallback
                input_el.input('\n')

        except Exception as e:
            logger.exception("[%s] Error sending: %s", self.PLATFORM_NAME, e)
            return False

        time.sleep(random.uniform(2.0, 4.0))  # LinkedIn rate-limits more aggressively
        return True
