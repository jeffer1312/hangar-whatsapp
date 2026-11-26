# API do hangar-whatsapp (C1, C2, C5, C7)

Contrato com o Hangar. Forma completa no plano `docs/2026-11-22-whatsapp-somente-leitura.md` do Hangar; aqui
ficam os detalhes que o cliente precisa para falar com o binário.

## C1. `control.json` e `control.key`

- `control.json`: `{"version":1,"api":3,"port":<u16>,"pid":<u32>}`, reescrito a cada subida (tmp + rename)
  antes de a porta aceitar pedidos. Porta escolhida pelo sistema em `127.0.0.1`.
- `control.key`: 32 bytes aleatórios em hex (64 caracteres e `\n`), criado na primeira subida. **A chave do
  HMAC são os 32 bytes decodificados**, não o texto hex. Modo 0600, pasta `--data` 0700.
- Um segundo processo na mesma pasta não sobe: o `session.db` fica travado pelo primeiro.

## Assinatura

Pedido, cabeçalho `X-Hangar-Signature: <ts>.<hex>`:

```
hex = HMAC-SHA256(key, METHOD + "\n" + URI + "\n" + TS + "\n" + hex(sha256(body)))
```

- `URI` é o caminho mais `?` e a query crua, exatamente como vai na linha do pedido (`/v1/messages?chat=5564…&limit=2`);
  sem query, só o caminho.
- `TS` em segundos Unix, aceito em ±30 s. Corpo vazio usa `sha256("")`.

Resposta, cabeçalho `X-Hangar-Signature: <hex>`, em **toda** resposta, erros incluídos:

```
hex = HMAC-SHA256(key, TS_DO_PEDIDO + "\n" + STATUS + "\n" + hex(sha256(body)))
```

`STATUS` em decimal. No 401 de pedido sem `ts` legível e no 413 (corpo acima de 64 KB), `TS_DO_PEDIDO` é
vazio.

A assinatura dá autenticidade, não sigilo. Um processo que ocupe a porta antiga não consegue forjar
resposta, porque não tem a chave. Mas ele lê em texto claro a query e o corpo do pedido: conversa, busca,
telefone do pareamento. O tráfego é só em `127.0.0.1`, então o risco existe só em máquina com vários
usuários.

Na query, `+` de fuso vai como `%2B` (senão vira espaço e dá 400); assine a URI já codificada, a mesma que vai
no pedido.

## Rotas

| Rota | Sucesso | Erros próprios |
|---|---|---|
| `GET /v1/status` | 200 | |
| `POST /v1/pair` `{"phone"?}` | 202, corpo vazio | 400 telefone inválido; 409 `bad_request` já pareado; 409 `outdated` |
| `POST /v1/logout` | 204 | 409 `not_paired` |
| `POST /v1/wipe` `{"confirm":true}` | 200 `{"logout":"done"\|"not_done"\|"not_paired"}` | 400 sem `confirm`; 500 `internal` se algo não pôde ser apagado (o serviço reabre e segue respondendo) |
| `GET /v1/chats?q&limit` | 200 `{"chats":[…]}` | |
| `GET /v1/messages?chat&since&until&before_id&limit` | 200 | 400 sem `chat`, `before_id` inexistente |
| `GET /v1/search?q&chat&limit` | 200 `{"messages":[…]}` | 400 sem `q` |
| `POST /v1/media/request` `{"chat","id"?,"since"?}` | 200 `{"requested","remaining_today","capped"}` | 409 `not_paired`/`outdated`; 429 `rate_limited` |

Parâmetros:
- `wipe.logout`: `done` (o celular foi avisado), `not_done` (o logout falhou, por exemplo sem rede: o aparelho
  pode continuar listado no celular e é preciso desvincular lá), `not_paired` (não havia aparelho). Os dados
  são apagados nos três casos.
- `chat` aceita o JID ou só o número com DDI (`5564999999999` vira `5564999999999@s.whatsapp.net`).
- `since`/`until`: ISO 8601 com fuso, ou data `AAAA-MM-DD` (UTC; em `until` vale até 23:59:59).
- `limit`: padrão 50, acima de 200 vira 200, menor que 1 é 400.
- `media/request`: no máximo 20 por chamada; `remaining_today` conta os pedidos das últimas 24 h (janela
  móvel, teto 200). 429 só quando a janela já está cheia e havia mídia para pedir. Cada pedido chega ao
  celular uma vez: sem resposta, ele fecha e a mídia continua `expired`, para pedir de novo.

As rotas `/v1/outbox*` (envio por rascunho aprovado) estão na seção C5; a edição antes do envio e a foto da
conversa, na C7.

Erro: `{"detail":{"code","msg"}}`. Além dos códigos do contrato (`unauthorized` 401, `bad_request` 400,
`not_paired` 409, `busy` 503, `outdated` 409, `rate_limited` 429): `internal` 500 para falha inesperada e
`bad_request` 404 para rota desconhecida.

## Estado

`state`:
- `unpaired`: sem aparelho;
- `pairing`: QR e, se pedido com `phone`, código;
- `connected`: aparelho vinculado (a rede reconecta sozinha);
- `logged_out`: o celular desvinculou ou houve `logout`; dados mantidos. Fica só na memória: depois de
  reiniciar o serviço, volta como `unpaired`;
- `outdated`: o WhatsApp recusou a versão.

`device` (número mascarado, `+5564••••2222`) e `device_name` (como o celular lista o aparelho, `Google
Chrome (Linux)`) só vêm em `connected`; nos outros estados são null.

`error.code` depois de um pareamento que não fechou (volta a `unpaired`): `pair_timeout` (QR esgotado),
`connect_failed`, `pair_code_failed`, `pair_failed`. Um novo `pair` recria o cliente.

`pair_code_expires_at` é 160 s depois do pedido (o socket de login fecha quando os QR acabam).

`send`: `{"last_minute","today","per_minute":3,"per_day":20}`, envios aprovados no último minuto e nas
últimas 24 h (janela móvel), contando os que falharam. `reachout_locked_until`: até quando o WhatsApp recusa
mensagem a contato novo (UTC), ou null sem trava.

`media_error` de uma mensagem: `expired` (só o celular tem; pode ser pedida), `phone_refused`, `unavailable`
(o ProChat não tinha), `unreadable`, `lost`. `totals.media_phone_only` conta `expired`; `media_lost`, os
outros.

## Processo

- `--version` imprime `hangar-whatsapp <versão> (api <n>)`; `api 2` tem o envio por rascunho (C5); `api 3`, a
  edição do texto antes do envio, a foto da conversa e o tipo das recentes (C7). O formato não muda: o Hangar
  instalado lê essa linha exata.
- `--schema-version` imprime só a versão do esquema do `messages.db` que este binário grava (o
  `user_version`, hoje `2`), seguida de `\n`, e sai com 0 sem abrir a pasta `--data`. Binário anterior a essa
  flag sai com código diferente de 0.
- `--data-schema-version --data <pasta>` imprime só o `user_version` do `<pasta>/messages.db` (`0` quando o arquivo
  não existe), seguido de `\n`, e sai com 0. Só lê: não migra, não cria a pasta nem o arquivo e não deixa
  `-wal`/`-shm` novos, e roda com o serviço de pé (outra versão com o banco aberto). Arquivo que não abre sai
  com 1 e o motivo no stderr; sem `--data` sai com 2.
- Mensagem só sai por rascunho que o dono aprovou (`POST /v1/outbox/{id}/send`), com "digitando", upload do
  anexo e a mensagem. O whatsmeow manda sozinho os recibos de entrega (os mesmos que o celular já manda) e
  nenhum recibo de leitura; o serviço não marca como lida nem aparece "online". Fora isso, o login, o logout
  (`logout`, `wipe`), as reconexões e o download de mídia da CDN do WhatsApp, as outras chamadas de saída são o
  pedido de reenvio de mídia ao celular (`POST /v1/media/request`) e a consulta da foto de uma conversa
  (`GetProfilePictureInfo`, só leitura, com o download da prévia em `*.whatsapp.net`), feita depois de um
  rascunho novo (C7).
- O log sai em stderr, no formato texto do `slog`. Ele traz os avisos e erros do whatsmeow (`módulo=whatsapp`
  ou `módulo=sessão`) e os eventos de conexão: conectado, queda, sessão aberta em outro cliente,
  suspensão, conexão recusada e mensagem não decifrada.
- Depois do login, o whatsmeow avisa o servidor de que o aparelho está ativo; aparelho passivo não recebe
  mensagens. Quando esse aviso falha, o serviço reconecta sozinho, e o novo login repete o aviso. Na
  primeira falha reconecta na hora; nas seguintes espera 1 min, depois 2, 4..., até 30 min, e volta a
  reconectar na hora depois de 1 h sem falha. A detecção
  depende do texto que o whatsmeow registra (`Failed to send post-connect passive IQ`). Se uma versão do
  whatsmeow mudar esse texto, a reconexão deixa de disparar, mas o aviso continua no log. O `state` segue
  `connected` nesse meio-tempo.
- Sai com código 3 quando o `messages.db` é de um esquema mais novo (`schema_newer`): só um binário mais
  novo abre esses dados.
- Antes de subir o `user_version` do `messages.db` de `v` para `--schema-version` (com `0 < v`), grava a cópia
  `<data>/messages.db.schema<v>.bak` (`VACUUM INTO`, por um nome temporário e rename: o `.bak` presente está
  sempre inteiro). Há uma por `v`, sobrescrita na migração seguinte do mesmo `v`; é ela que o Hangar repõe se o
  binário novo não ficar de pé. O `wipe` apaga as cópias com o resto dos dados. Banco novo (`v = 0`) e coluna acrescentada sem subir o `user_version` (como na
  v0.3.0) não fazem cópia.
- Sai com código 4 (`backup_failed`) quando essa cópia não pôde ser gravada: nada migrou e o `messages.db` ficou
  como estava.

## C5. Envio por rascunho aprovado (api 2)

Quem pede cria um rascunho; a mensagem só sai quando o dono responde `send`. Mesma assinatura e mesmo formato
de erro das outras rotas. Horários em UTC, `2006-01-02T15:04:05Z`.

| Rota | Sucesso | Erros próprios |
|---|---|---|
| `POST /v1/outbox` `{"chat","text","reply_to"?,"file"?,"origin","origin_cwd"?}` | 201 rascunho; 200 com o rascunho pendente igual, sem criar outro | 400 `bad_request`, `file_refused`; 409 `not_paired`, `outdated`, `unknown_chat`, `new_contact`, `unsupported_chat`; 413 `file_too_large`; 429 `too_many_drafts` |
| `GET /v1/outbox?state=pending` | 200 `{"drafts":[…]}`, os 100 mais novos | 400 sem `state` válido |
| `GET /v1/outbox/{id}` | 200 rascunho | 404 `not_found` |
| `POST /v1/outbox/{id}/send` `{"via":"desktop"\|"mobile"}` | 202 rascunho em `sending` | 400 `via` inválido; 404 `not_found`; 409 `expired`, `answered`, `not_paired`, `outdated`, `new_contact`, `reachout_locked`; 429 `rate_limited` |
| `POST /v1/outbox/{id}/send-edited` `{"via","text"}` (api 3, C7) | 202 rascunho em `sending` | os do `send`; 400 `bad_request` também para `text` ausente ou inválido |
| `POST /v1/outbox/{id}/decline` | 200 rascunho em `declined` | 404 `not_found`; 409 `expired`, `answered` |

Rascunho:

```json
{"id","origin","chat","chat_kind":"contact|group","chat_number","chat_name","chat_name_source":"contact|push|group|",
 "recent":[{"from_me","sender","at","text","kind","file_name"}],"text","reply_to","quoted_text",
 "file":{"name","size","sha256","source","outside_cwd"}|null,
 "created_at","expires_at","state","via","answered_at","message_id","error","wa_code",
 "chat_avatar","edited","sent_text"}
```

- `state`: `pending`, `sending`, `sent`, `declined`, `expired`, `failed`. Só `pending` aceita `send` e `decline`;
  depois disso o rascunho só muda sozinho (`sending` vira `sent` ou `failed`).
- `chat`: JID ou número com DDI. `unknown_chat`: a conversa não está no arquivo. `new_contact`: a pessoa nunca
  escreveu para o número pareado agora (nenhuma mensagem com `from_me = 0` desse número). `unsupported_chat`: lista
  de transmissão e canal. Grupo é aceito.
- Número de cada mensagem: cada mensagem do arquivo guarda o número pareado que a arquivou ou enviou. Trocar de
  número, com ou sem volta, não aproveita conversa do outro. O `send` confere de novo a conversa com o número pareado
  na hora: se ele nunca recebeu mensagem da pessoa, responde `new_contact` e o rascunho fica pendente, mesmo que o
  rascunho tenha sido feito com outro número. As mensagens de antes da 0.2 ficam com o número pareado na primeira
  subida da 0.2; despareado nessa hora, com o primeiro número pareado depois. A leitura não muda.
- `chat_number`: os dígitos do contato; para grupo (e conversa só por `@lid`), o JID inteiro. `chat_name_source`:
  `contact` (agenda do celular), `push` (nome que a própria pessoa escolheu, ou o da empresa), `group`, ou vazio
  quando o nome só existe no arquivo, sem origem conhecida.
- `recent`: as 3 últimas mensagens da conversa, da mais antiga para a mais nova; `sender` é o nome no arquivo
  ou o número, vazio nas da própria conta.
- `text`: até 4096 caracteres; vazio só com `file`. Com `file`, o texto vai como legenda do documento, numa
  mensagem só. `reply_to`: id de mensagem da mesma conversa (senão 400); `quoted_text` é o texto dela.
- `origin` é `servidor::sessao` de quem pediu, como o Hangar declarou; `via`, quem aprovou. Os dois servem só ao
  registro.
- Repetição: já existindo um rascunho pendente com a mesma `origin`, conversa, texto e sha256 do anexo, a
  criação devolve ele (200).
- Expira 10 min depois de `created_at`, conferido a cada pedido e a cada 30 s.
- Tetos: 3 rascunhos pendentes por `origin` e 10 no total (`too_many_drafts`); 3 envios por minuto e 20 em
  24 h, contando `sending`, `sent` e `failed` pela hora da aprovação (`rate_limited`). A conferência dos tetos
  e a passagem para `sending` são uma transação só: duas aprovações ao mesmo tempo não passam juntas.

Anexo (`file`):

- Caminho absoluto (senão 400 `bad_request`), arquivo comum de até 100 MB (`file_too_large`).
- `file_refused` para: a pasta `--data`; qualquer parte do caminho começando por ponto (cobre `~/.hangar`,
  `~/.ssh`, `~/.gnupg`, `~/.config`, `~/.claude*` e `.env*`), exceto `~/.hangar/worktrees/`, onde o Hangar abre
  sessões no código do usuário e a regra vale só do que vem abaixo dela (`.git`, `.env`); `*.key` e `*.pem`;
  `/proc`, `/sys` e `/dev`;
  link simbólico no último trecho; pasta, FIFO e afins; arquivo que não existe. As regras valem para o caminho
  pedido e para o caminho com as pastas resolvidas.
- O arquivo é aberto sem seguir link (`O_NOFOLLOW`; no Windows, aberto como ponto de nova análise, recusado se for
  link ou junção, e reaberto conferindo que é o mesmo arquivo) e copiado do
  descritor já conferido para `outbox/<id>/<nome>` (pasta 0700), com sha256. A cópia é o que sai: mudar o original
  depois não muda o envio. Ela some quando o rascunho chega a estado final; no envio feito, vai para `media/`.
- `source` é o caminho com as pastas resolvidas. `outside_cwd` é true quando o arquivo não está dentro de
  `origin_cwd` ou quando `origin_cwd` não veio.

Envio (depois do 202, fora do pedido HTTP; `sending` vira `sent` ou `failed` em no máximo 10 min depois da
aprovação, o teto que cobre o upload de 100 MB):

1. "digitando" na conversa, espera de `min(1 s + 50 ms por caractere, 6 s)`, "parou de digitar";
2. upload do anexo, quando há;
3. a mensagem: texto simples; com `reply_to`, texto com citação (id, autor e conteúdo da mensagem citada); com
   anexo, documento com nome, tipo, legenda e citação.

O endereço é o da mensagem mais recente da conversa como o WhatsApp a conhece, não a chave normalizada do
arquivo. No sucesso, `message_id` recebe o id da mensagem e ela entra em `/v1/messages` com `from_me` true.
Qualquer falha deixa `failed`, sem reenvio. `wa_code` é o código que o WhatsApp devolveu, quando houve.
`error`:
- `reachout_locked`: o WhatsApp recusou com 463 ou 475 (trava de contato novo), ou a trava chegou durante o
  "digitando" e a mensagem não saiu;
- `unconfirmed`: o envio foi cortado depois de a mensagem talvez ter saído (sem resposta do WhatsApp em 75 s,
  serviço parando, rede caindo); a pessoa pode ter recebido;
- `interrupted`: o serviço parou no meio, visto na subida seguinte;
- senão, o texto do erro.

Trava: quando o WhatsApp avisa a trava de mensagens a contatos novos, o serviço grava o fim dela
(`reachout_locked_until` no status; aviso sem fim vale 24 h) e recusa `send` com `reachout_locked` até lá. Um 463
ou 475 também trava por 24 h (ou até o fim já gravado, se for mais tarde): insistir depois dele leva a ban. A
trava é conferida de novo logo antes da mensagem sair.

Na subida, todo `sending` vira `failed` com `error = "interrupted"` (sem reenvio) e as cópias sem rascunho
pendente são apagadas. O `wipe` apaga a tabela `outbox` com o `messages.db`, e também `outbox/`: depois dele,
`GET /v1/outbox/{id}` de um rascunho antigo responde 404 `not_found`.

## C7. Edição antes do envio e foto da conversa (api 3)

Mesma assinatura e formato de erro. Um binário `api 2` não tem a rota `send-edited`: responde 404 `bad_request`
(rota desconhecida) e nada sai. O Hangar lê `api` do `control.json` antes de oferecer a edição.

### Envio com texto editado

`POST /v1/outbox/{id}/send-edited` `{"via":"desktop"\|"mobile","text":"..."}`:

- `text` é obrigatório. Ele é normalizado (`\r\n` e `\r` viram `\n`) e validado como na criação: até 4096
  caracteres, vazio (só espaços) só quando o rascunho tem anexo. Inválido: 400 `bad_request`, e o rascunho continua
  `pending`.
- Igual ao texto do rascunho (os dois normalizados): é um envio normal, `edited = false` e `sent_text` é o texto
  do rascunho.
- Diferente: `edited = true`, e o que sai é o texto normalizado (com anexo, como legenda; vazio ou só espaços,
  documento sem legenda e `sent_text` vazio). `text` do rascunho não muda: continua o que a sessão pediu.
- Mesmos erros e tetos do `send`: 404 `not_found`; 409 `expired`, `answered`, `not_paired`, `outdated`,
  `new_contact`, `reachout_locked`; 429 `rate_limited`; 400 `via` inválido.
- 202 com o rascunho em `sending`; o cliente confere `sent_text`.

Exemplo: rascunho com `"text":"chego às 15h"`; o dono manda `{"via":"mobile","text":"chego às 16h\r\nabraço"}`;
a resposta traz `"state":"sending","edited":true,"sent_text":"chego às 16h\nabraço","text":"chego às 15h"`.

### Campos novos do rascunho

- `sent_text`: o texto aprovado, o mesmo que o envio usa, gravado na transação que passa de `pending` a `sending`
  (nas duas rotas). Vazio enquanto `pending` e nos rascunhos que nunca foram aprovados (`declined`, `expired`).
- `edited`: `true` quando saiu pelo `send-edited` com texto diferente.
- `chat_avatar`: a prévia da foto da conversa em JPEG, em base64, ou `null` (sem foto, escondida, ainda não buscada,
  ou nenhum número pareado). Lida do cache a cada leitura do rascunho, para o número pareado agora.
- `recent[].kind`: `text`, `image`, `audio`, `video`, `document`, `sticker` ou `other` (de `messages.kind`). As
  recentes são as 3 últimas sem contar reações. Rascunho criado por um binário `api 2` não traz `kind`: tratar
  ausente como texto.
- `recent[].file_name`: nome do documento (`fileName` da mensagem original, até 120 caracteres); vazio nos outros
  tipos. Como `text`, é escrito por terceiro.

### Foto da conversa

- Buscada em segundo plano depois do 201 de um rascunho novo, só com aparelho `connected`, uma busca por conversa de
  cada vez, com prazo de 30 s. Não varre a agenda.
- Cache por número pareado e conversa (a foto escondida depende de quem pergunta; grupo usa o JID do grupo). Vale
  24 h; depois, a consulta leva o id da foto guardada e só baixa se ela mudou. "Sem foto" e "escondida" também valem
  24 h. Erro de rede ou do WhatsApp não grava nada: o próximo rascunho tenta de novo.
- Teto de 30 consultas em 24 h (janela móvel), contando as que falharam.
- Download só por `https` de host `*.whatsapp.net` (redirecionamento incluído), até 24 KB, começando por `FF D8 FF`
  e com até 640×640; fora disso, fica como sem foto.
- O `wipe` apaga o cache com o `messages.db` e cancela a busca em curso, que não grava nada depois.

### Dados

`outbox` ganha `edited` e `sent_text` por `ALTER TABLE` na subida; `avatars` e `avatar_fetches` são tabelas novas.
O `user_version` continua 2: um binário 0.2 ainda abre o arquivo e grava rascunhos nele. A cada subida, rascunho
aprovado sem `sent_text` (pela 0.2, que não o grava) recebe o próprio `text`, que foi o que saiu.
