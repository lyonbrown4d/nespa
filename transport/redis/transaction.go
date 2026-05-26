package redis

import (
	"context"
	"strings"

	"github.com/lyonbrown4d/nespa/cache"
)

func handleMulti(state *session, args []respArg) respValue {
	if len(args) != 1 {
		return syntaxError()
	}
	if state.inTransaction {
		return errorString("ERR MULTI calls can not be nested")
	}
	state.inTransaction = true
	state.queued = nil
	return simpleString("OK")
}

func handleDiscard(state *session, args []respArg) respValue {
	if len(args) != 1 {
		return syntaxError()
	}
	if !state.inTransaction {
		return errorString("ERR DISCARD without MULTI")
	}
	state.inTransaction = false
	state.queued = nil
	return simpleString("OK")
}

func (s *Server) handleExec(ctx context.Context, state *session, args []respArg) respValue {
	if len(args) != 1 {
		return syntaxError()
	}
	if !state.inTransaction {
		return errorString("ERR EXEC without MULTI")
	}

	queued := state.queued
	state.inTransaction = false
	state.queued = nil

	responses := make([]respValue, 0, len(queued))
	err := state.activeService(s).Transaction(ctx, func(txCtx context.Context, txService cache.Service) error {
		state.txService = txService
		defer func() {
			state.txService = nil
		}()
		for index := range queued {
			responses = append(responses, s.handleCommand(txCtx, state, []respArg(queued[index])))
		}
		return nil
	})
	if err != nil {
		return serviceError(err)
	}
	return arrayValue(responses...)
}

func (s *Server) handleQueuedCommand(
	ctx context.Context,
	state *session,
	command string,
	args []respArg,
) respValue {
	switch command {
	case "EXEC":
		return s.handleExec(ctx, state, args)
	case "DISCARD":
		return handleDiscard(state, args)
	case "MULTI":
		return errorString("ERR MULTI calls can not be nested")
	}

	if _, ok := s.commandHandlers()[command]; !ok {
		return errorString("ERR unknown command '" + strings.ToLower(command) + "'")
	}
	state.queued = append(state.queued, cloneQueuedCommand(args))
	return simpleString("QUEUED")
}

func cloneQueuedCommand(args []respArg) queuedCommand {
	out := make(queuedCommand, 0, len(args))
	for index := range args {
		out = append(out, respArg(append([]byte(nil), args[index]...)))
	}
	return out
}
