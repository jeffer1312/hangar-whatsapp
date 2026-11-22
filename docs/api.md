# API do hangar-whatsapp (C1, C2)

Contrato com o Hangar. Forma completa no plano `docs/2026-11-22-whatsapp-somente-leitura.md` do Hangar; aqui
ficam os detalhes que o cliente precisa para falar com o binário.

## C1. `control.json` e `control.key`

- `control.json`: `{"version":1,"api":1,"port":<u16>,"pid":<u32>}`, reescrito a cada subida (tmp + rename)
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

`media_error` de uma mensagem: `expired` (só o celular tem; pode ser pedida), `phone_refused`, `unavailable`
(o ProChat não tinha), `unreadable`, `lost`. `totals.media_phone_only` conta `expired`; `media_lost`, os
outros.

## Processo

- `--version` imprime `hangar-whatsapp <versão> (api <n>)`.
- Sai com código 3 quando o `messages.db` é de um esquema mais novo (`schema_newer`): só um binário mais
  novo abre esses dados.
