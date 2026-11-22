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
	PRIMARY KEY (chat, id)
);

CREATE INDEX IF NOT EXISTS messages_chat_at ON messages (chat, at);
CREATE INDEX IF NOT EXISTS messages_at ON messages (at);
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

CREATE VIEW IF NOT EXISTS chats AS
SELECT m.chat, coalesce(c.name, '') AS name, count(*) AS messages, min(m.at) AS first_at, max(m.at) AS last_at
FROM messages m LEFT JOIN contacts c ON c.jid = m.chat
GROUP BY m.chat;
