package wa

import (
	"fmt"
	"log/slog"
	"strings"

	waLog "go.mau.fi/whatsmeow/util/log"
)

// passiveIQFailed is whatsmeow's warning when, after login, the server was not told this device is
// active; a passive device gets no messages. If whatsmeow rewords it, the reconnection stops firing
// but the warning still reaches the log.
const passiveIQFailed = "Failed to send post-connect passive IQ"

// slogWA sends whatsmeow's warnings and errors to the service log; below that it is too chatty.
type slogWA struct {
	log           *slog.Logger
	module        string
	passiveFailed func()
}

func waLogger(log *slog.Logger, module string, passiveFailed func()) waLog.Logger {
	return slogWA{log, module, passiveFailed}
}

func (l slogWA) Warnf(msg string, args ...any) {
	l.log.Warn(fmt.Sprintf(msg, args...), "módulo", l.module)
	if l.passiveFailed != nil && strings.HasPrefix(msg, passiveIQFailed) {
		l.passiveFailed()
	}
}
func (l slogWA) Errorf(msg string, args ...any) {
	l.log.Error(fmt.Sprintf(msg, args...), "módulo", l.module)
}
func (l slogWA) Infof(string, ...any)  {}
func (l slogWA) Debugf(string, ...any) {}
func (l slogWA) Sub(module string) waLog.Logger {
	return slogWA{l.log, l.module + "/" + module, l.passiveFailed}
}
