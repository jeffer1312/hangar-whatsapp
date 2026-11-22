// hangar-whatsapp links as a WhatsApp companion device and only archives: messages, history and
// media go to --data, readable through a signed API on loopback. It never sends a message.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/jeffer1312/hangar-whatsapp/internal/api"
	"github.com/jeffer1312/hangar-whatsapp/internal/control"
	"github.com/jeffer1312/hangar-whatsapp/internal/store"
	"github.com/jeffer1312/hangar-whatsapp/internal/wa"
)

var version = "dev"

// exitSchemaNewer tells the supervisor that only a newer binary can open this data.
const exitSchemaNewer = 3

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func defaultData() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(".hangar", "whatsapp")
	}
	return filepath.Join(home, ".hangar", "whatsapp")
}

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("hangar-whatsapp", flag.ContinueOnError)
	fs.SetOutput(stderr)
	data := fs.String("data", defaultData(), "pasta dos dados (messages.db, session.db, media/, control.json, control.key)")
	from := fs.String("import", "", "importa a pasta dados/ do whatsapp-arquivo para --data, que precisa estar vazia, e sai")
	showVersion := fs.Bool("version", false, "mostra a versão e sai")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *showVersion {
		fmt.Fprintf(stdout, "hangar-whatsapp %s (api %d)\n", version, wa.API)
		return 0
	}
	if *from != "" {
		if err := importPrototype(*from, *data); err != nil {
			fmt.Fprintln(stderr, "erro:", err)
			return 1
		}
		fmt.Fprintf(stdout, "importado de %s para %s; o sessao.db de origem virou sessao.db.migrated\n", *from, *data)
		return 0
	}
	log := slog.New(slog.NewTextHandler(stderr, nil))
	if err := serve(*data, log); err != nil {
		log.Error("encerrado com erro", "erro", err)
		if errors.Is(err, store.ErrSchemaNewer) {
			return exitSchemaNewer
		}
		return 1
	}
	return 0
}

func importPrototype(from, data string) error {
	c, err := control.ReadControl(data)
	if err != nil {
		return err
	}
	if c.PID != 0 && control.PIDAlive(c.PID) {
		return fmt.Errorf("o hangar-whatsapp está rodando nesta pasta (pid %d): pare o serviço antes de importar", c.PID)
	}
	return store.Import(from, data)
}

func serve(data string, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	key, err := control.LoadOrCreateKey(data)
	if err != nil {
		return err
	}
	svc, err := wa.Open(ctx, wa.Options{Dir: data, Version: version, Log: log})
	if err != nil {
		return err
	}
	defer svc.Close()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	port := uint16(ln.Addr().(*net.TCPAddr).Port)
	// Written before serving: whoever reads it finds a port that already answers.
	if err := control.WriteControl(data, control.Control{Version: 1, API: wa.API, Port: port, PID: os.Getpid()}); err != nil {
		ln.Close()
		return err
	}
	srv := &http.Server{Handler: api.New(svc, key, time.Now, log),
		ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	log.Info("pronto", "versão", version, "porta", port, "pasta", data)
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdown)
}
