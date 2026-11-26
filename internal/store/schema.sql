CREATE TABLE IF NOT EXISTS contacts (
	jid TEXT PRIMARY KEY,
	name TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS messages (
	chat TEXT NOT NULL,
	id TEXT NOT NULL,
	-- Empty when from_me = 1.
	sender TEXT NOT NULL DEFAULT '',
	from_me INTEGER NOT NULL,
	-- UTC, 2006-01-02T15:04:05Z, so text order is time order.
	at TEXT NOT NULL,
	-- text, image, audio, video, document, sticker, reaction, other
	kind TEXT NOT NULL,
	text TEXT NOT NULL DEFAULT '',
	-- Relative to the data folder, always media/<user>/<id>.<ext>.
	media TEXT,
	media_mime TEXT,
	-- expired (only the phone has it), phone_refused, unavailable, unreadable, lost
	media_error TEXT,
	reply_to TEXT,
	-- prochat or whatsapp
	source TEXT NOT NULL,
	raw TEXT,
	-- The paired number that archived or sent it; empty for rows from before it was kept, until a number pairs.
	account TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (chat, id)
);

CREATE INDEX IF NOT EXISTS messages_chat_at ON messages (chat, at);
CREATE INDEX IF NOT EXISTS messages_at ON messages (at);
CREATE INDEX IF NOT EXISTS messages_unowned ON messages (chat) WHERE account = '';
-- Media still without a file: the downloader reads only these, every 30 s.
CREATE INDEX IF NOT EXISTS messages_media_pending ON messages (at)
	WHERE source = 'whatsapp' AND media IS NULL AND media_mime IS NOT NULL;

-- One row per request to the phone; kept after done_at so the daily cap counts them.
CREATE TABLE IF NOT EXISTS media_requests (
	chat TEXT NOT NULL,
	id TEXT NOT NULL,
	requested_at TEXT NOT NULL,
	done_at TEXT
);

CREATE INDEX IF NOT EXISTS media_requests_message ON media_requests (chat, id);
CREATE INDEX IF NOT EXISTS media_requests_requested_at ON media_requests (requested_at);

-- Drafts the owner approves in the Hangar before anything is sent; the copy of an attachment lives
-- in outbox/<id>/ until the draft reaches a final state.
CREATE TABLE IF NOT EXISTS outbox (
	id TEXT PRIMARY KEY,
	-- sha256 of origin, chat, text and the attachment's sha256: a pending twin is returned, not repeated.
	dedup TEXT NOT NULL,
	origin TEXT NOT NULL,
	chat TEXT NOT NULL,
	-- Where the message goes: the chat as WhatsApp knows it (raw.chat of the chat's newest message).
	address TEXT NOT NULL,
	-- contact or group
	chat_kind TEXT NOT NULL,
	chat_number TEXT NOT NULL,
	chat_name TEXT NOT NULL DEFAULT '',
	-- contact, push, group or empty
	chat_name_source TEXT NOT NULL DEFAULT '',
	-- JSON array of the chat's last messages when the draft was made.
	recent TEXT NOT NULL,
	text TEXT NOT NULL,
	reply_to TEXT,
	quoted_text TEXT,
	quoted_raw TEXT,
	file_name TEXT,
	file_size INTEGER,
	file_sha256 TEXT,
	file_source TEXT,
	file_outside_cwd INTEGER,
	created_at TEXT NOT NULL,
	expires_at TEXT NOT NULL,
	-- pending, sending, sent, declined, expired, failed
	state TEXT NOT NULL,
	via TEXT,
	answered_at TEXT,
	message_id TEXT,
	error TEXT,
	wa_code INTEGER,
	-- 1 when the owner changed the text before sending.
	edited INTEGER NOT NULL DEFAULT 0,
	-- The text approved for sending, written when the draft leaves pending; empty before.
	sent_text TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS outbox_state ON outbox (state, created_at);
-- The send caps count what left pending to be sent, by when it was approved.
CREATE INDEX IF NOT EXISTS outbox_sends ON outbox (answered_at) WHERE state IN ('sending', 'sent', 'failed');

-- Contact and group pictures, fetched after a draft is made for the chat. account is the number paired
-- at the time: a hidden picture depends on who asks.
CREATE TABLE IF NOT EXISTS avatars (
	account TEXT NOT NULL,
	jid TEXT NOT NULL,
	picture_id TEXT NOT NULL DEFAULT '',
	-- ok, none, hidden
	state TEXT NOT NULL,
	image BLOB,
	fetched_at TEXT NOT NULL,
	PRIMARY KEY (account, jid)
);

-- One row per picture query in the last 24 h, for the daily cap.
CREATE TABLE IF NOT EXISTS avatar_fetches (
	at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS settings (
	key TEXT PRIMARY KEY,
	value TEXT NOT NULL
);

CREATE VIEW IF NOT EXISTS chats AS
SELECT m.chat, coalesce(c.name, '') AS name, count(*) AS messages, min(m.at) AS first_at, max(m.at) AS last_at
FROM messages m LEFT JOIN contacts c ON c.jid = m.chat
GROUP BY m.chat;
