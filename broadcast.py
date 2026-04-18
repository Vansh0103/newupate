from __future__ import annotations

import json
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from telebot import TeleBot, types
from telebot.apihelper import ApiTelegramException


# ══════════════════════════════════════════════════════════════════════════════
# Constants
# ══════════════════════════════════════════════════════════════════════════════

SEND_DELAY        = 0.05   # seconds between messages (avoid 30 msg/s limit)
RETRY_ATTEMPTS    = 3      # retries per user on failure
RETRY_DELAY       = 1.0    # base seconds between retries (doubles each time)
PROGRESS_INTERVAL = 50     # send progress update every N users

ALL_CONTENT_TYPES = [
    "text", "photo", "video", "document", "animation",
    "audio", "voice", "sticker", "video_note",
    "poll", "location", "contact",
]

BROADCAST_TYPES: Dict[str, str] = {
    "text":       "📝 Text",
    "photo":      "🖼 Photo",
    "video":      "🎬 Video",
    "document":   "📄 Document",
    "animation":  "🎞 Animation (GIF)",
    "audio":      "🎵 Audio",
    "voice":      "🎤 Voice",
    "sticker":    "🙂 Sticker",
    "video_note": "🎥 Video Note",
    "poll":       "📊 Poll",
    "location":   "📍 Location",
    "contact":    "👤 Contact",
    "copy":       "📤 Forward / Copy",
}


# ══════════════════════════════════════════════════════════════════════════════
# BroadcastSystem
# ══════════════════════════════════════════════════════════════════════════════

class BroadcastSystem:
    """
    Drop-in broadcast module for pyTelegramBotAPI (telebot).

    Parameters
    ──────────
    bot              : TeleBot instance.
    is_admin         : callable(user_id) → bool
    get_all_users    : callable() → list of dicts or objects with 'user_id'.
    safe_send        : (unused internally but kept for API compatibility)
    log_admin_action : optional callable(admin_id, action, details)
    """

    def __init__(
        self,
        bot: TeleBot,
        is_admin: Callable[[int], bool],
        get_all_users: Callable[[], List[Any]],
        safe_send: Optional[Callable[..., Any]] = None,
        log_admin_action: Optional[Callable[..., Any]] = None,
    ) -> None:
        self.bot              = bot
        self.is_admin         = is_admin
        self.get_all_users    = get_all_users
        self.safe_send        = safe_send
        self.log_admin_action = log_admin_action

        # user_id → { "step": str, "data": dict }
        self._states: Dict[int, Dict[str, Any]] = {}

    # ──────────────────────────────────────────────────────────────
    # State machine
    # ──────────────────────────────────────────────────────────────

    def _set(self, uid: int, step: str, data: Optional[Dict] = None) -> None:
        self._states[uid] = {"step": step, "data": data or {}}

    def _get(self, uid: int) -> Optional[Dict[str, Any]]:
        return self._states.get(uid)

    def _clear(self, uid: int) -> None:
        self._states.pop(uid, None)

    def _update_step(self, uid: int, step: str) -> None:
        state = self._states.get(uid)
        if state:
            state["step"] = step

    # ──────────────────────────────────────────────────────────────
    # Telegram helpers
    # ──────────────────────────────────────────────────────────────

    def _send(
        self,
        chat_id: int,
        text: str,
        reply_markup: Any = None,
        parse_mode: str = "HTML",
    ) -> Optional[types.Message]:
        try:
            return self.bot.send_message(
                chat_id,
                text,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
                disable_web_page_preview=True,
            )
        except Exception as exc:
            print(f"[Broadcast] _send({chat_id}): {exc}")
            return None

    def _answer(
        self,
        call: Any,
        text: str = "",
        show_alert: bool = False,
    ) -> None:
        try:
            self.bot.answer_callback_query(call.id, text=text, show_alert=show_alert)
        except Exception:
            pass

    def _edit_or_send(
        self,
        chat_id: int,
        message_id: Optional[int],
        text: str,
        reply_markup: Any = None,
    ) -> None:
        """Try to edit an existing message; fall back to a new message."""
        if message_id:
            try:
                self.bot.edit_message_text(
                    text,
                    chat_id,
                    message_id,
                    parse_mode="HTML",
                    reply_markup=reply_markup,
                    disable_web_page_preview=True,
                )
                return
            except Exception:
                pass
        self._send(chat_id, text, reply_markup=reply_markup)

    # ──────────────────────────────────────────────────────────────
    # Keyboard builders
    # ──────────────────────────────────────────────────────────────

    def _main_menu(self) -> types.InlineKeyboardMarkup:
        kb = types.InlineKeyboardMarkup(row_width=2)
        pairs = [
            ("📝 Text",              "text"),
            ("🖼 Photo",             "photo"),
            ("🎬 Video",             "video"),
            ("📄 Document",          "document"),
            ("🎞 Animation",         "animation"),
            ("🎵 Audio",             "audio"),
            ("🎤 Voice",             "voice"),
            ("🙂 Sticker",           "sticker"),
            ("🎥 Video Note",        "video_note"),
            ("📊 Poll",              "poll"),
            ("📍 Location",          "location"),
            ("👤 Contact",           "contact"),
        ]
        buttons = [
            types.InlineKeyboardButton(label, callback_data=f"ab_type_{btype}")
            for label, btype in pairs
        ]
        kb.add(*buttons)
        kb.add(
            types.InlineKeyboardButton(
                "📤 Forward / Copy Existing", callback_data="ab_type_copy"
            )
        )
        kb.add(types.InlineKeyboardButton("❌ Cancel", callback_data="ab_cancel"))
        return kb

    def _parse_mode_menu(self) -> types.InlineKeyboardMarkup:
        kb = types.InlineKeyboardMarkup(row_width=2)
        kb.add(
            types.InlineKeyboardButton("HTML", callback_data="ab_pm_HTML"),
            types.InlineKeyboardButton("MarkdownV2", callback_data="ab_pm_MarkdownV2"),
            types.InlineKeyboardButton("None (plain)", callback_data="ab_pm_"),
        )
        kb.add(types.InlineKeyboardButton("❌ Cancel", callback_data="ab_cancel"))
        return kb

    def _buttons_menu(self) -> types.InlineKeyboardMarkup:
        kb = types.InlineKeyboardMarkup(row_width=2)
        kb.add(
            types.InlineKeyboardButton("➕ Add Buttons", callback_data="ab_btn_yes"),
            types.InlineKeyboardButton("⏭ Skip",        callback_data="ab_btn_no"),
        )
        kb.add(types.InlineKeyboardButton("❌ Cancel", callback_data="ab_cancel"))
        return kb

    def _preview_menu(self) -> types.InlineKeyboardMarkup:
        kb = types.InlineKeyboardMarkup(row_width=2)
        kb.add(
            types.InlineKeyboardButton("✅ Send Now",     callback_data="ab_send"),
            types.InlineKeyboardButton("✏️ Edit Buttons", callback_data="ab_edit_btn"),
        )
        kb.add(
            types.InlineKeyboardButton("🔁 Restart",     callback_data="ab_restart"),
            types.InlineKeyboardButton("❌ Cancel",       callback_data="ab_cancel"),
        )
        return kb

    # ──────────────────────────────────────────────────────────────
    # Button JSON parser
    # ──────────────────────────────────────────────────────────────

    def _parse_buttons(
        self, raw: str
    ) -> Tuple[Optional[types.InlineKeyboardMarkup], Optional[str]]:
        """
        Parse a JSON array-of-rows into an InlineKeyboardMarkup.

        Returns (markup, error_string).  error_string is None on success.

        Accepted format
        ───────────────
        [
          [{"text": "Label", "url": "https://..."}],
          [{"text": "CB",    "callback_data": "data"}]
        ]

        Sending "[]" or "" removes all buttons.
        """
        raw = (raw or "").strip()
        if not raw or raw == "[]":
            return None, None

        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            return None, f"JSON parse error: {exc}"

        if not isinstance(parsed, list):
            return None, "Top-level value must be a JSON array [ … ]."

        markup = types.InlineKeyboardMarkup()
        for row_idx, row in enumerate(parsed, 1):
            if not isinstance(row, list):
                return None, f"Row {row_idx} is not a list."
            btn_row: List[types.InlineKeyboardButton] = []
            for btn_idx, item in enumerate(row, 1):
                if not isinstance(item, dict):
                    return None, f"Button {btn_idx} in row {row_idx} is not an object."
                text = str(item.get("text", "")).strip()
                if not text:
                    return None, f"Button {btn_idx} in row {row_idx} is missing 'text'."
                url  = item.get("url")
                cdata = item.get("callback_data")
                if url:
                    btn_row.append(types.InlineKeyboardButton(text, url=str(url)))
                elif cdata:
                    btn_row.append(
                        types.InlineKeyboardButton(text, callback_data=str(cdata))
                    )
                else:
                    return (
                        None,
                        f"Button '{text}' (row {row_idx}) needs 'url' or 'callback_data'.",
                    )
            if btn_row:
                markup.row(*btn_row)

        return markup, None

    # ──────────────────────────────────────────────────────────────
    # User collection
    # ──────────────────────────────────────────────────────────────

    def _collect_users(self) -> List[int]:
        result: List[int] = []
        try:
            users = self.get_all_users() or []
        except Exception as exc:
            print(f"[Broadcast] get_all_users error: {exc}")
            return result

        for u in users:
            try:
                if isinstance(u, dict):
                    uid = int(u["user_id"])
                else:
                    uid = int(u.user_id)
                result.append(uid)
            except Exception:
                continue
        return result

    # ──────────────────────────────────────────────────────────────
    # Preview
    # ──────────────────────────────────────────────────────────────

    def _send_preview(self, chat_id: int, data: Dict[str, Any]) -> None:
        btype       = data.get("broadcast_type", "?")
        total_users = len(self._collect_users())
        has_buttons = bool((data.get("buttons_json") or "").strip())
        parse_mode  = data.get("parse_mode") or "HTML"

        body = data.get("text") or data.get("caption") or "(no text / media only)"
        if len(body) > 600:
            body = body[:600] + "\n…(truncated)"

        info = (
            f"🚀 <b>Broadcast Preview</b>\n\n"
            f"<b>Type:</b> {BROADCAST_TYPES.get(btype, btype)}\n"
            f"<b>Parse mode:</b> {parse_mode or 'None'}\n"
            f"<b>Inline buttons:</b> {'Yes ✅' if has_buttons else 'No'}\n"
            f"<b>Recipients:</b> {total_users} users\n\n"
            f"<b>Content preview:</b>\n<pre>{self._escape(body)}</pre>"
        )
        self._send(chat_id, info, reply_markup=self._preview_menu())

        # ── show a sample of the actual message ──────────────────
        markup, _ = self._parse_buttons(data.get("buttons_json", ""))
        try:
            self._deliver(chat_id, data, markup, parse_mode)
        except Exception as exc:
            self._send(chat_id, f"⚠️ Sample preview failed:\n<code>{exc}</code>")

    @staticmethod
    def _escape(text: str) -> str:
        """Escape < > & for HTML so raw content is shown safely."""
        return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    # ──────────────────────────────────────────────────────────────
    # Single-message delivery (shared by preview + broadcast)
    # ──────────────────────────────────────────────────────────────

    def _deliver(
        self,
        chat_id: int,
        data: Dict[str, Any],
        markup: Optional[types.InlineKeyboardMarkup],
        parse_mode: str,
    ) -> None:
        """Send one message of any type to chat_id. Raises on failure."""
        btype = data.get("broadcast_type")
        fid   = data.get("file_id", "")
        cap   = data.get("caption", "")
        pm    = parse_mode or None           # None = no parse_mode

        if btype == "text":
            self.bot.send_message(
                chat_id,
                data.get("text", ""),
                parse_mode=pm,
                reply_markup=markup,
                disable_web_page_preview=False,
            )

        elif btype == "photo":
            self.bot.send_photo(
                chat_id, fid,
                caption=cap, parse_mode=pm, reply_markup=markup,
            )

        elif btype == "video":
            self.bot.send_video(
                chat_id, fid,
                caption=cap, parse_mode=pm, reply_markup=markup,
            )

        elif btype == "document":
            self.bot.send_document(
                chat_id, fid,
                caption=cap, parse_mode=pm, reply_markup=markup,
            )

        elif btype == "animation":
            self.bot.send_animation(
                chat_id, fid,
                caption=cap, parse_mode=pm, reply_markup=markup,
            )

        elif btype == "audio":
            self.bot.send_audio(
                chat_id, fid,
                caption=cap, parse_mode=pm, reply_markup=markup,
            )

        elif btype == "voice":
            self.bot.send_voice(
                chat_id, fid,
                caption=cap, parse_mode=pm, reply_markup=markup,
            )

        elif btype == "sticker":
            # Stickers don't support captions or reply_markup in send_sticker;
            # if buttons are requested, send sticker first then buttons as text.
            self.bot.send_sticker(chat_id, fid)
            if markup:
                self.bot.send_message(
                    chat_id,
                    "⬆️ See buttons above",
                    reply_markup=markup,
                )

        elif btype == "video_note":
            self.bot.send_video_note(chat_id, fid)
            if markup:
                self.bot.send_message(
                    chat_id, "⬆️ See buttons above", reply_markup=markup
                )

        elif btype == "poll":
            poll = data.get("poll_data", {})
            self.bot.send_poll(
                chat_id,
                question=poll.get("question", "?"),
                options=poll.get("options", []),
                is_anonymous=poll.get("is_anonymous", True),
                type=poll.get("type", "regular"),
                allows_multiple_answers=poll.get("allows_multiple_answers", False),
                reply_markup=markup,
            )

        elif btype == "location":
            loc = data.get("location_data", {})
            self.bot.send_location(
                chat_id,
                latitude=loc["latitude"],
                longitude=loc["longitude"],
                reply_markup=markup,
            )

        elif btype == "contact":
            c = data.get("contact_data", {})
            self.bot.send_contact(
                chat_id,
                phone_number=c["phone_number"],
                first_name=c["first_name"],
                last_name=c.get("last_name", ""),
                reply_markup=markup,
            )

        elif btype == "copy":
            self.bot.copy_message(
                chat_id=chat_id,
                from_chat_id=data["source_chat_id"],
                message_id=data["source_message_id"],
                reply_markup=markup,
                caption=cap or None,
                parse_mode=pm if cap else None,
            )

        else:
            raise ValueError(f"Unknown broadcast type: {btype!r}")

    # ──────────────────────────────────────────────────────────────
    # Broadcast engine
    # ──────────────────────────────────────────────────────────────

    def _send_to_one(self, uid: int, data: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Attempt to deliver to one user.
        Returns (success, reason).
        Handles RetryAfter (flood control) and retries.
        """
        markup, err = self._parse_buttons(data.get("buttons_json", ""))
        if err:
            markup = None  # send without buttons rather than crashing

        parse_mode = data.get("parse_mode") or "HTML"

        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                self._deliver(uid, data, markup, parse_mode)
                return True, ""

            except ApiTelegramException as exc:
                code = exc.error_code
                description = str(exc.description or "")

                # User blocked / deactivated → no point retrying
                if code in (403, 400) or "chat not found" in description.lower():
                    return False, f"blocked/not_found ({code})"

                # Flood control → sleep exact seconds Telegram asks
                if code == 429 or "retry after" in description.lower():
                    retry_secs = getattr(exc, "result_json", {})
                    if isinstance(retry_secs, dict):
                        retry_secs = retry_secs.get("parameters", {}).get("retry_after", 5)
                    else:
                        retry_secs = 5
                    print(f"[Broadcast] flood control – sleeping {retry_secs}s")
                    time.sleep(int(retry_secs) + 1)
                    continue

                # Generic error – back-off and retry
                wait = RETRY_DELAY * (2 ** (attempt - 1))
                print(f"[Broadcast] uid={uid} attempt={attempt} err={exc} – retry in {wait}s")
                time.sleep(wait)

            except Exception as exc:
                wait = RETRY_DELAY * (2 ** (attempt - 1))
                print(f"[Broadcast] uid={uid} attempt={attempt} err={exc} – retry in {wait}s")
                time.sleep(wait)

        return False, "max_retries_exceeded"

    def _execute_broadcast(
        self, admin_id: int, chat_id: int, data: Dict[str, Any]
    ) -> Dict[str, Any]:
        user_ids   = self._collect_users()
        total      = len(user_ids)
        sent       = 0
        failed     = 0
        blocked    = 0
        start_time = time.time()

        progress_msg: Optional[types.Message] = self._send(
            chat_id,
            f"🚀 Starting broadcast to <b>{total}</b> users…",
        )
        progress_msg_id = progress_msg.message_id if progress_msg else None

        for idx, uid in enumerate(user_ids, 1):
            ok, reason = self._send_to_one(uid, data)
            if ok:
                sent += 1
            else:
                failed += 1
                if "blocked" in reason or "not_found" in reason:
                    blocked += 1

            # Live progress update
            if idx % PROGRESS_INTERVAL == 0 or idx == total:
                elapsed = round(time.time() - start_time, 1)
                pct     = round((idx / total) * 100)
                bar     = ("█" * (pct // 10)).ljust(10, "░")
                progress_text = (
                    f"📡 <b>Broadcasting…</b>\n\n"
                    f"[{bar}] {pct}%\n\n"
                    f"✅ Sent:    <b>{sent}</b>\n"
                    f"❌ Failed:  <b>{failed}</b>\n"
                    f"🚫 Blocked: <b>{blocked}</b>\n"
                    f"📊 Total:   <b>{idx}/{total}</b>\n"
                    f"⏱ Elapsed: <b>{elapsed}s</b>"
                )
                self._edit_or_send(chat_id, progress_msg_id, progress_text)

            time.sleep(SEND_DELAY)

        duration = round(time.time() - start_time, 2)

        # Log action
        if self.log_admin_action:
            try:
                self.log_admin_action(
                    admin_id,
                    "broadcast",
                    (
                        f"type={data.get('broadcast_type')} "
                        f"total={total} sent={sent} failed={failed} "
                        f"blocked={blocked} duration={duration}s"
                    ),
                )
            except Exception:
                pass

        return {
            "total": total,
            "sent": sent,
            "failed": failed,
            "blocked": blocked,
            "duration": duration,
        }

    # ──────────────────────────────────────────────────────────────
    # Primary content handler
    # ──────────────────────────────────────────────────────────────

    def _handle_primary_content(
        self,
        uid: int,
        chat_id: int,
        message: Any,
        btype: str,
        data: Dict[str, Any],
    ) -> None:
        ct = message.content_type

        # ── text ─────────────────────────────────────────────────
        if btype == "text":
            if ct != "text":
                self._send(chat_id, "❌ Please send a <b>text</b> message.")
                return
            data["text"] = message.text or ""

        # ── photo ────────────────────────────────────────────────
        elif btype == "photo":
            if ct != "photo":
                self._send(chat_id, "❌ Please send a <b>photo</b>.")
                return
            data["file_id"] = message.photo[-1].file_id   # highest resolution
            data["caption"] = message.caption or ""

        # ── video ────────────────────────────────────────────────
        elif btype == "video":
            if ct != "video":
                self._send(chat_id, "❌ Please send a <b>video</b>.")
                return
            data["file_id"] = message.video.file_id
            data["caption"] = message.caption or ""

        # ── document ─────────────────────────────────────────────
        elif btype == "document":
            if ct != "document":
                self._send(chat_id, "❌ Please send a <b>document / file</b>.")
                return
            data["file_id"] = message.document.file_id
            data["caption"] = message.caption or ""

        # ── animation / GIF ──────────────────────────────────────
        elif btype == "animation":
            # Telegram delivers GIFs as document; also check animation field
            if ct == "animation" or (ct == "document" and message.document and message.document.mime_type == "video/mp4"):
                fid = (
                    message.animation.file_id
                    if ct == "animation"
                    else message.document.file_id
                )
                data["file_id"] = fid
                data["caption"] = message.caption or ""
            else:
                self._send(chat_id, "❌ Please send a <b>GIF / animation</b>.")
                return

        # ── audio ────────────────────────────────────────────────
        elif btype == "audio":
            if ct != "audio":
                self._send(chat_id, "❌ Please send an <b>audio</b> file.")
                return
            data["file_id"] = message.audio.file_id
            data["caption"] = message.caption or ""

        # ── voice ────────────────────────────────────────────────
        elif btype == "voice":
            if ct != "voice":
                self._send(chat_id, "❌ Please send a <b>voice</b> message.")
                return
            data["file_id"] = message.voice.file_id
            data["caption"] = message.caption or ""

        # ── sticker ──────────────────────────────────────────────
        elif btype == "sticker":
            if ct != "sticker":
                self._send(chat_id, "❌ Please send a <b>sticker</b>.")
                return
            data["file_id"] = message.sticker.file_id

        # ── video note (round video) ──────────────────────────────
        elif btype == "video_note":
            if ct != "video_note":
                self._send(chat_id, "❌ Please send a <b>round video note</b>.")
                return
            data["file_id"] = message.video_note.file_id

        # ── poll ─────────────────────────────────────────────────
        elif btype == "poll":
            if ct != "poll":
                self._send(chat_id, "❌ Please <b>forward a poll</b> to me.")
                return
            p = message.poll
            data["poll_data"] = {
                "question":               p.question,
                "options":                [o.text for o in p.options],
                "is_anonymous":           p.is_anonymous,
                "type":                   p.type,
                "allows_multiple_answers": p.allows_multiple_answers,
            }

        # ── location ─────────────────────────────────────────────
        elif btype == "location":
            if ct != "location":
                self._send(chat_id, "❌ Please send a <b>location</b>.")
                return
            data["location_data"] = {
                "latitude":  message.location.latitude,
                "longitude": message.location.longitude,
            }

        # ── contact ──────────────────────────────────────────────
        elif btype == "contact":
            if ct != "contact":
                self._send(chat_id, "❌ Please send a <b>contact</b>.")
                return
            data["contact_data"] = {
                "phone_number": message.contact.phone_number,
                "first_name":   message.contact.first_name,
                "last_name":    message.contact.last_name or "",
            }

        # ── forward / copy ────────────────────────────────────────
        elif btype == "copy":
            data["source_chat_id"]    = message.chat.id
            data["source_message_id"] = message.message_id
            data["caption"]           = ""   # keep original caption

        else:
            self._send(chat_id, "❌ Unknown broadcast type. Use /advbrod to restart.")
            self._clear(uid)
            return

        # Advance state → ask parse mode (for text-capable types)
        text_types = {"text", "photo", "video", "document", "animation",
                      "audio", "voice", "copy"}
        if btype in text_types:
            self._set(uid, "await_parse_mode", data)
            self._send(
                chat_id,
                f"✅ <b>{BROADCAST_TYPES.get(btype, btype)}</b> received.\n\n"
                "Choose <b>parse mode</b> for text/caption:",
                reply_markup=self._parse_mode_menu(),
            )
        else:
            # Non-text types → skip parse mode, go straight to buttons
            data["parse_mode"] = ""
            self._set(uid, "await_buttons_choice", data)
            self._send(
                chat_id,
                f"✅ <b>{BROADCAST_TYPES.get(btype, btype)}</b> received.\n\n"
                "Do you want to add <b>inline buttons</b>?",
                reply_markup=self._buttons_menu(),
            )

    # ══════════════════════════════════════════════════════════════
    # Handler registration  (call once after bot is created)
    # ══════════════════════════════════════════════════════════════

    def register_handlers(self) -> None:

        # ── /advbrod  ─────────────────────────────────────────────
        @self.bot.message_handler(commands=["advbrod"])
        def cmd_advbrod(message: Any) -> None:
            uid = message.from_user.id
            if not self.is_admin(uid):
                self._send(message.chat.id, "❌ Access denied.")
                return
            self._clear(uid)
            self._send(
                message.chat.id,
                (
                    "🚀 <b>Advanced Broadcast Panel</b>\n\n"
                    "Choose the broadcast type:\n\n"
                    "• Text, Photo, Video, Document, Animation\n"
                    "• Audio, Voice, Sticker, Video Note\n"
                    "• Poll, Location, Contact\n"
                    "• Forward / Copy any existing message\n\n"
                    "✨ Inline buttons and HTML / MarkdownV2 are supported."
                ),
                reply_markup=self._main_menu(),
            )

        # ── All ab_ callbacks ─────────────────────────────────────
        @self.bot.callback_query_handler(
            func=lambda c: isinstance(c.data, str) and c.data.startswith("ab_")
        )
        def cb_advbrod(call: Any) -> None:
            uid = call.from_user.id
            if not self.is_admin(uid):
                self._answer(call, "❌ Access denied.", show_alert=True)
                return

            self._answer(call)       # dismiss loading spinner immediately
            cdata   = call.data
            chat_id = call.message.chat.id

            # ── Cancel ───────────────────────────────────────────
            if cdata == "ab_cancel":
                self._clear(uid)
                self._send(chat_id, "❌ Broadcast cancelled.")
                return

            # ── Restart ──────────────────────────────────────────
            if cdata == "ab_restart":
                self._clear(uid)
                self._send(
                    chat_id,
                    "🔁 Restarted. Choose broadcast type:",
                    reply_markup=self._main_menu(),
                )
                return

            # ── Type selection ────────────────────────────────────
            if cdata.startswith("ab_type_"):
                btype = cdata[len("ab_type_"):]
                if btype not in BROADCAST_TYPES:
                    self._send(chat_id, "❌ Unknown type.")
                    return
                self._set(uid, "await_primary_content", {"broadcast_type": btype})

                prompts = {
                    "text":       "📝 Send the broadcast <b>text</b> now.\n\nHTML is supported by default.",
                    "photo":      "🖼 Send the <b>photo</b> now.\n\nCaption is optional.",
                    "video":      "🎬 Send the <b>video</b> now.\n\nCaption is optional.",
                    "document":   "📄 Send the <b>document</b> now.\n\nCaption is optional.",
                    "animation":  "🎞 Send the <b>GIF / animation</b> now.\n\nCaption is optional.",
                    "audio":      "🎵 Send the <b>audio file</b> now.\n\nCaption is optional.",
                    "voice":      "🎤 Send the <b>voice message</b> now.",
                    "sticker":    "🙂 Send the <b>sticker</b> now.",
                    "video_note": "🎥 Send the <b>round video note</b> now.",
                    "poll":       "📊 <b>Forward a poll</b> to me — I'll broadcast it.",
                    "location":   "📍 Send a <b>location</b> now.",
                    "contact":    "👤 Send a <b>contact</b> now.",
                    "copy":       (
                        "📤 <b>Forward the source message</b> to me.\n\n"
                        "It will be copied to all users exactly as-is\n"
                        "(media, formatting, buttons)."
                    ),
                }
                self._send(chat_id, prompts.get(btype, "Send content now."))
                return

            # ── Parse-mode selection ──────────────────────────────
            if cdata.startswith("ab_pm_"):
                pm    = cdata[len("ab_pm_"):]   # "HTML" | "MarkdownV2" | ""
                state = self._get(uid)
                if not state or state["step"] != "await_parse_mode":
                    self._send(chat_id, "❌ Unexpected state. Use /advbrod.")
                    return
                state["data"]["parse_mode"] = pm
                self._update_step(uid, "await_buttons_choice")
                self._send(
                    chat_id,
                    f"✅ Parse mode set to <b>{pm or 'None'}</b>.\n\n"
                    "Do you want to add <b>inline buttons</b>?",
                    reply_markup=self._buttons_menu(),
                )
                return

            # ── Add buttons ───────────────────────────────────────
            if cdata == "ab_btn_yes":
                state = self._get(uid)
                if not state:
                    self._send(chat_id, "❌ No active broadcast. Use /advbrod.")
                    return
                self._update_step(uid, "await_buttons_json")
                self._send(
                    chat_id,
                    (
                        "➕ <b>Send buttons JSON</b>\n\n"
                        "Format — array of rows, each row is array of buttons:\n"
                        "<pre>[\n"
                        '  [{"text": "Join Channel", "url": "https://t.me/example"}],\n'
                        '  [{"text": "Website",      "url": "https://example.com"},\n'
                        '   {"text": "Support",      "url": "https://t.me/support"}]\n'
                        "]</pre>\n\n"
                        "For callback buttons use <code>\"callback_data\"</code> "
                        "instead of <code>\"url\"</code>.\n\n"
                        "Send <code>[]</code> to clear all buttons."
                    ),
                )
                return

            # ── Skip buttons ──────────────────────────────────────
            if cdata == "ab_btn_no":
                state = self._get(uid)
                if not state:
                    self._send(chat_id, "❌ No active broadcast. Use /advbrod.")
                    return
                state["data"]["buttons_json"] = ""
                self._update_step(uid, "ready_preview")
                self._send_preview(chat_id, state["data"])
                return

            # ── Edit buttons ──────────────────────────────────────
            if cdata == "ab_edit_btn":
                state = self._get(uid)
                if not state:
                    self._send(chat_id, "❌ No active broadcast. Use /advbrod.")
                    return
                self._update_step(uid, "await_buttons_json")
                self._send(
                    chat_id,
                    "✏️ <b>Send new buttons JSON.</b>\n\nSend <code>[]</code> to remove all.",
                )
                return

            # ── Confirm & send ────────────────────────────────────
            if cdata == "ab_send":
                state = self._get(uid)
                if not state:
                    self._send(chat_id, "❌ No active broadcast. Use /advbrod.")
                    return
                data_copy = dict(state["data"])
                self._clear(uid)

                result = self._execute_broadcast(uid, chat_id, data_copy)

                self._send(
                    chat_id,
                    (
                        f"✅ <b>Broadcast Complete!</b>\n\n"
                        f"📊 Total users:  <b>{result['total']}</b>\n"
                        f"✅ Sent:          <b>{result['sent']}</b>\n"
                        f"❌ Failed:        <b>{result['failed']}</b>\n"
                        f"🚫 Blocked/gone:  <b>{result['blocked']}</b>\n"
                        f"⏱ Duration:      <b>{result['duration']}s</b>"
                    ),
                )
                return

        # ── Content handler (state-driven) ────────────────────────
        @self.bot.message_handler(
            func=lambda m: (
                m.from_user is not None
                and self.is_admin(m.from_user.id)
                and self._get(m.from_user.id) is not None
            ),
            content_types=ALL_CONTENT_TYPES,
        )
        def state_handler(message: Any) -> None:
            uid   = message.from_user.id
            state = self._get(uid)
            if not state:
                return

            step    = state["step"]
            data    = state["data"]
            chat_id = message.chat.id
            btype   = data.get("broadcast_type", "")

            # ── Waiting for primary content ───────────────────────
            if step == "await_primary_content":
                self._handle_primary_content(uid, chat_id, message, btype, data)
                return

            # ── Waiting for buttons JSON ──────────────────────────
            if step == "await_buttons_json":
                if message.content_type != "text":
                    self._send(chat_id, "❌ Please send the buttons as <b>text JSON</b>.")
                    return
                raw = (message.text or "").strip()
                _, err = self._parse_buttons(raw)
                if err:
                    self._send(
                        chat_id,
                        f"❌ <b>Invalid JSON:</b>\n<code>{self._escape(err)}</code>\n\n"
                        "Please fix and try again.",
                    )
                    return
                data["buttons_json"] = "" if raw == "[]" else raw
                self._update_step(uid, "ready_preview")
                self._send(chat_id, "✅ Buttons saved.")
                self._send_preview(chat_id, data)
                return

            # ── Any other state: ignore content ───────────────────
            self._send(
                chat_id,
                "⚠️ Please use the buttons above, or use /advbrod to restart.",
            )
