# hangar-whatsapp

Arquivador do seu WhatsApp para o [Hangar](https://hangar.dev.br): **lê tudo e envia só rascunho aprovado**. Ele
entra como aparelho conectado (igual ao WhatsApp Web), guarda mensagens, histórico e mídia numa pasta local e
deixa as sessões do Hangar lerem e buscarem esse arquivo. Uma sessão pode criar um rascunho (texto e, se quiser,
um arquivo); a mensagem só sai quando você toca em "Enviar" no cartão do Hangar. Não marca como lida, não mostra
"online". Pelo whatsmeow só saem, além do que você aprovou, os recibos de entrega automáticos, os mesmos que o
celular já manda, e nunca recibo de leitura.

> **Risco.** É um cliente não oficial ([whatsmeow](https://github.com/tulir/whatsmeow)). O WhatsApp pode
> banir o número que usar cliente não oficial. Ler é o uso de menor risco, mas não é risco zero; enviar aumenta
> o risco, e o maior é escrever para quem nunca falou com a conta. Por isso o envio é manual e tem freio:
> 3 por minuto e 20 em 24 h, rascunho que expira em 10 min, nada para contato novo, e nenhum reenvio depois de
> falha ou de trava do WhatsApp.
> Os dados ficam só na sua máquina, em `--data`. O `session.db` dá acesso à conta: não compartilhe.

O Hangar instala, atualiza e supervisiona este binário pela página Configurações > WhatsApp. Rodar à mão só
faz sentido para depurar.

Cada release traz um `checksums.txt` (sha256). Ele confere que o download não se corrompeu, mas vem da
mesma release: não protege contra uma release comprometida.

## Uso

```sh
hangar-whatsapp [--data ~/.hangar/whatsapp]   # serviço: API em 127.0.0.1, porta em control.json
hangar-whatsapp --version
hangar-whatsapp --schema-version                # versão do esquema do messages.db, sem abrir os dados
hangar-whatsapp --data-schema-version --data ~/.hangar/whatsapp   # versão do esquema do messages.db da pasta, só lendo
hangar-whatsapp --data ~/.hangar/whatsapp --import ~/Projetos/whatsapp-arquivo/dados
```

`--import` leva a pasta do protótipo `whatsapp-arquivo` (mensagens, mídia e sessão, sem parear de novo). Só
roda com o destino sem `messages.db` e sem `session.db` e com o serviço parado. No fim renomeia o `sessao.db`
de origem para `sessao.db.migrated`, para as mesmas chaves não rodarem em dois lugares.

O log vai para stderr (num serviço systemd, `journalctl --user -u hangar-whatsapp`), com os avisos do
whatsmeow e os eventos de conexão. Se o servidor não puser o aparelho em modo ativo depois do login, o serviço
reconecta sozinho; essa detecção depende do texto do aviso do whatsmeow (ver [docs/api.md](docs/api.md)).

## Pasta de dados

| Arquivo | O que é |
|---|---|
| `messages.db` | SQLite: `messages`, `contacts`, `media_requests`, `outbox` (rascunhos e o que saiu), `settings` e a visão `chats` |
| `messages.db.schema<v>.bak` | cópia do `messages.db` feita antes de migrar do esquema `v` (ver [docs/api.md](docs/api.md)); sai junto no wipe |
| `session.db` | chaves do aparelho (whatsmeow); fica travado enquanto o serviço roda |
| `media/` | arquivos de mídia, `media/<número>/<id>.<ext>` |
| `outbox/` | cópia do anexo de cada rascunho aberto, `outbox/<id>/<nome>` |
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
