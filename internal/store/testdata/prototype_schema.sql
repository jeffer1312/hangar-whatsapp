CREATE TABLE IF NOT EXISTS contatos (
	jid TEXT PRIMARY KEY,
	nome TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS mensagens (
	conversa TEXT NOT NULL,
	id TEXT NOT NULL,
	-- Vazio quando de_mim = 1.
	remetente TEXT NOT NULL DEFAULT '',
	de_mim INTEGER NOT NULL,
	-- ISO 8601 em UTC.
	horario TEXT NOT NULL,
	-- texto, imagem, audio, video, documento, figurinha, outro
	tipo TEXT NOT NULL,
	texto TEXT NOT NULL DEFAULT '',
	-- Caminho relativo à pasta dados/.
	midia TEXT,
	midia_mime TEXT,
	midia_erro TEXT,
	responde_a TEXT,
	-- prochat ou whatsapp
	origem TEXT NOT NULL,
	bruto TEXT,
	PRIMARY KEY (conversa, id)
);

CREATE INDEX IF NOT EXISTS mensagens_conversa_horario ON mensagens (conversa, horario);

-- Mídia que só o celular ainda tem e alguém pediu; o programa pede ao celular e apaga a linha.
CREATE TABLE IF NOT EXISTS midia_pedida (
	conversa TEXT NOT NULL,
	id TEXT NOT NULL,
	pedida_em TEXT NOT NULL DEFAULT (datetime('now')),
	PRIMARY KEY (conversa, id)
);

CREATE VIEW IF NOT EXISTS conversas AS
SELECT m.conversa, coalesce(c.nome, '') AS nome, count(*) AS mensagens,
	min(m.horario) AS primeira, max(m.horario) AS ultima
FROM mensagens m LEFT JOIN contatos c ON c.jid = m.conversa
GROUP BY m.conversa;
