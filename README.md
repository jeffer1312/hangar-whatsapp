# hangar-whatsapp

Arquivador **só de leitura** do seu WhatsApp para o [Hangar](https://hangar.dev.br). Ele entra como aparelho
conectado (igual ao WhatsApp Web), guarda mensagens, histórico e mídia numa pasta local e deixa as sessões do
Hangar lerem e buscarem esse arquivo. Não envia mensagem, não marca como lida, não mostra "online".

> **Risco.** É um cliente não oficial ([whatsmeow](https://github.com/tulir/whatsmeow)). O WhatsApp pode
> banir o número que usar cliente não oficial. Só ler é o uso de menor risco, mas não é risco zero.
> Os dados ficam só na sua máquina, em `--data`. O `session.db` dá acesso à conta: não compartilhe.

O Hangar instala, atualiza e supervisiona este binário pela página Configurações > WhatsApp. Rodar à mão só
faz sentido para depurar.

## Uso

```sh
hangar-whatsapp [--data ~/.hangar/whatsapp]   # serviço: API em 127.0.0.1, porta em control.json
hangar-whatsapp --version
hangar-whatsapp --data ~/.hangar/whatsapp --import ~/Projetos/whatsapp-arquivo/dados
```

`--import` leva a pasta do protótipo `whatsapp-arquivo` (mensagens, mídia e sessão, sem parear de novo). Só
roda com o destino sem `messages.db` e sem `session.db` e com o serviço parado. No fim renomeia o `sessao.db`
de origem para `sessao.db.migrated`, para as mesmas chaves não rodarem em dois lugares.

## Pasta de dados

| Arquivo | O que é |
|---|---|
| `messages.db` | SQLite: `messages`, `contacts`, `media_requests` e a visão `chats` |
| `session.db` | chaves do aparelho (whatsmeow); fica travado enquanto o serviço roda |
| `media/` | arquivos de mídia, `media/<número>/<id>.<ext>` |
| `control.json`, `control.key` | porta, pid e chave da API (ver [docs/api.md](docs/api.md)) |

Mídia antiga que o servidor do WhatsApp já apagou só o celular tem. Ela é pedida sob demanda
(`POST /v1/media/request`), no máximo 20 por pedido e 200 em 24 h, porque cada uma gera notificação no celular.

## Desenvolvimento

```sh
go test ./...
go vet ./...
```

Os testes usam um dublê do whatsmeow (`internal/wa/watest`) e bancos temporários; nenhum pareia um número.

## Licença

[MPL-2.0](LICENSE), a mesma do whatsmeow.
