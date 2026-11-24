package gateway

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/ZAUakaAlexey/threadroute/internal/gwapi"
	"github.com/ZAUakaAlexey/threadroute/internal/opapi"
)

// Stand settings (decision 0014). The gateway holds the router settings,
// with the stand block and its change log, in one atomic pointer
// (Gateway.settings) that is swapped in one step. Three writers swap it: the
// poll of the router every opapi.SettingsPollInterval, the answer of an
// applied change, and the current settings that come with a conflict.
//
// # Order
//
// Every swap, every push to the clients and the registration of a new
// client run under Gateway.pushMu, so:
//
//   - a new client gets the block in its Welcome or as a later "settings"
//     message, never neither;
//   - a client that asked for a change gets the "settings" message of the
//     new block before its Result, because both go through the same queue
//     of that client and the push happens before the Result is queued.
//
// # Old answers
//
// A read of the router reserves a number before it calls the router; an
// answer of a change reserves one when the answer arrives. A swap with a
// number below the last swapped one is dropped. So a poll that started
// before a change and ends after it cannot put the older set back. A poll
// that started after the change was answered sees the new set anyway. A
// dropped answer costs nothing: the next poll comes within
// opapi.SettingsPollInterval.

// SetSettings stores the router settings, as a new read of the router.
func (g *Gateway) SetSettings(s opapi.Settings) { g.applySettings(s, g.settingsSeq.Add(1)) }

// RefreshSettings reads the router settings with the change log and stores
// them.
func (g *Gateway) RefreshSettings(ctx context.Context) (opapi.Settings, error) {
	seq := g.settingsSeq.Add(1)
	s, err := g.api.SettingsWithChanges(ctx)
	if err != nil {
		return opapi.Settings{}, err
	}
	g.applySettings(s, seq)
	return s, nil
}

// applySettings swaps the settings and pushes a new or changed stand block
// to every client.
func (g *Gateway) applySettings(s opapi.Settings, seq uint64) {
	g.pushMu.Lock()
	defer g.pushMu.Unlock()
	if seq < g.appliedSeq {
		return
	}
	g.appliedSeq = seq
	old := g.settings.Load()
	g.settings.Store(&s)
	g.noteSettings(old, &s)
	var before *opapi.StandState
	if old != nil {
		before = old.Stand
	}
	// Every client gets a block that appeared or changed, also one whose
	// Welcome had none (the router started to offer stand settings later).
	if s.Stand == nil || (before != nil && !standChanged(before, s.Stand)) {
		return
	}
	st := s.Stand
	g.fanOut(func(*client) bool { return true }, func(a gwapi.Audience) gwapi.ServerMessage {
		return gwapi.SettingsUpdate{Settings: *settingsBlock(st, a.MayChangeSettings)}
	})
}

// noteSettings logs a change of the base settings or of the state of the
// stand block, and sets the version metric. The caller holds pushMu.
func (g *Gateway) noteSettings(old, cur *opapi.Settings) {
	var oldBase opapi.Settings
	var oldStand *opapi.StandState
	if old != nil {
		oldBase, oldStand = *old, old.Stand
		oldBase.Stand = nil
	}
	base := *cur
	base.Stand = nil
	if old == nil || oldBase != base {
		g.logger.Info("router settings read", "speed_factor", cur.SpeedFactor,
			"accept_timeout", cur.AcceptTimeout().String())
	}
	st := cur.Stand
	if st == nil {
		g.m.standVersion.Set(-1)
		if old == nil || oldStand != nil {
			g.logger.Info("the router offers no stand settings")
		}
		return
	}
	g.m.standVersion.Set(float64(st.Version))
	if oldStand == nil || oldStand.Version != st.Version {
		g.logger.Info("stand settings read", "version", st.Version)
	}
}

// standChanged reports whether a client that holds before must get after:
// a new version, or another current set or other defaults.
func standChanged(before, after *opapi.StandState) bool {
	return before.Version != after.Version || !before.Current.Equal(after.Current) ||
		!before.Defaults.Equal(after.Defaults)
}

// settingsBlock returns the protocol block of a stand state for a client
// with or without the right to change settings. Without the right, every
// "by" label is left out (decision 0014): it may hold a site user ID.
func settingsBlock(st *opapi.StandState, mayChange bool) *gwapi.Settings {
	changes := make([]opapi.SettingsChange, len(st.Changes))
	for i, ch := range st.Changes {
		if !mayChange {
			ch.By = ""
		}
		changes[i] = ch
	}
	return &gwapi.Settings{
		Version: st.Version, Current: st.Current, Defaults: st.Defaults, Bounds: opapi.Bounds(),
		Changes: changes, MayChange: mayChange,
	}
}

// fanOut queues one message for every client that want selects. build
// makes the message for an audience; each audience is encoded once. The
// caller holds pushMu.
func (g *Gateway) fanOut(want func(c *client) bool, build func(a gwapi.Audience) gwapi.ServerMessage) {
	encoded := map[gwapi.Audience]outItem{}
	g.hub.each(func(c *client) {
		if !want(c) {
			return
		}
		it, ok := encoded[c.aud]
		if !ok {
			m := build(c.aud)
			b, err := gwapi.EncodeServer(m, c.aud)
			if err != nil {
				g.logger.Error("server message failed validation", "type", m.ServerType(), "error", err)
				c.closeWith(gwapi.CloseInternal, reasonInternal)
				return
			}
			it = outItem{b: b, typ: m.ServerType()}
			encoded[c.aud] = it
		}
		c.enqueue(it)
	})
}

// register adds a new client to the hub and returns what its Welcome
// carries: the status, the stand block (nil when there is none) and, for
// the supervisor view, the trend history. Every later push reaches the
// client, and nothing before it is lost.
func (g *Gateway) register(c *client) (gwapi.Status, *opapi.StandState, []gwapi.TrendPoint) {
	g.pushMu.Lock()
	defer g.pushMu.Unlock()
	status := g.hub.add(c)
	var st *opapi.StandState
	if s := g.settings.Load(); s != nil {
		st = s.Stand
	}
	var points []gwapi.TrendPoint
	if c.aud.View == gwapi.ViewSupervisor {
		points = append([]gwapi.TrendPoint{}, g.trend...)
	}
	return status, st, points
}

// Texts of the answers to change_settings. The gateway writes its own text
// instead of passing on the router's.
const (
	settingsNotAllowedText = "this connection may not change settings"
	settingsOffText        = "the router does not offer stand settings"
	settingsInvalidText    = "the settings are not valid"
	settingsTooSoonText    = "one change per 10 seconds"
)

// changeSettingsType is the request type of change_settings in the metrics.
const changeSettingsType = "change_settings"

// handleChangeSettings answers a change of the stand settings (decision
// 0014). The right and the rate limit are checked at once; the rest runs on
// its own goroutine, which queues the push of a new block before the Result.
func (c *client) handleChangeSettings(ctx context.Context, m gwapi.ChangeSettings) {
	if !c.aud.MayChangeSettings {
		c.settingsAnswer(settingsRejected(m.RequestID, gwapi.CodeNotAllowed, settingsNotAllowedText))
		return
	}
	if !c.lim.allow(c.g.clock.Now()) {
		c.settingsAnswer(settingsRejected(m.RequestID, gwapi.CodeRateLimited, rateLimitedMessage))
		return
	}
	c.wg.Go(func() { c.settingsAnswer(c.g.changeSettings(ctx, c, m)) })
}

// settingsAnswer counts and sends the Result of a change_settings.
func (c *client) settingsAnswer(res gwapi.Result) {
	label := string(res.Outcome)
	if res.Outcome == gwapi.OutcomeRejected {
		label = string(res.Code)
	}
	c.g.m.actions.WithLabelValues(changeSettingsType, label).Inc()
	c.g.m.settingsChanges.WithLabelValues(label).Inc()
	c.send(res)
}

func settingsRejected(requestID string, code gwapi.ErrorCode, text string) gwapi.Result {
	return gwapi.Result{RequestID: requestID, Outcome: gwapi.OutcomeRejected, Code: code, Message: text}
}

func settingsInvalid(requestID string, fields []opapi.FieldError) gwapi.Result {
	r := settingsRejected(requestID, gwapi.CodeInvalidRequest, settingsInvalidText)
	r.Fields = fields
	return r
}

func settingsConflict(requestID string, base, current int64) gwapi.Result {
	return settingsRejected(requestID, gwapi.CodeSettingsConflict,
		fmt.Sprintf("base version %d is not the current version %d", base, current))
}

// changeSettings checks a change in the order of the router (opapi
// PatternPutSettings: the bounds, the version, the sim part) as far as the
// held block allows, so that a refused change costs no router call, then
// asks the router.
func (g *Gateway) changeSettings(ctx context.Context, c *client, m gwapi.ChangeSettings) gwapi.Result {
	s := g.settings.Load()
	if s == nil || s.Stand == nil {
		return settingsRejected(m.RequestID, gwapi.CodeSettingsOff, settingsOffText)
	}
	st := s.Stand
	if fes := m.Settings.FieldErrors(); len(fes) > 0 {
		return settingsInvalid(m.RequestID, fes)
	}
	// Versions only grow, so an older base is a conflict for sure. A newer
	// base than the held one means that the gateway has not read the
	// router since; the router decides then.
	// The client holds the current block already: it came with the
	// Welcome or with the push of its swap.
	if m.BaseVersion < st.Version {
		return settingsConflict(m.RequestID, m.BaseVersion, st.Version)
	}
	if m.BaseVersion == st.Version {
		if fe, bad := simPartError(st.Current, m.Settings); bad {
			return settingsInvalid(m.RequestID, []opapi.FieldError{fe})
		}
	}
	by := c.ident.by()
	if err := opapi.CheckBy(by); err != nil || by == opapi.BySimulator {
		g.logger.Error("no valid by label for a connection that may change settings", "local", c.ident.Local)
		return settingsRejected(m.RequestID, gwapi.CodeInternal, codeTexts[gwapi.CodeInternal])
	}
	resp, err := g.api.PutSettings(ctx, opapi.SettingsChangeRequest{Settings: m.Settings, BaseVersion: m.BaseVersion, By: by})
	// The number is taken when the answer is here, see "Old answers".
	seq := g.settingsSeq.Add(1)
	if err == nil {
		g.applySettings(resp.Settings, seq)
		if resp.Result == opapi.ResultAlreadyDone {
			return gwapi.Result{RequestID: m.RequestID, Outcome: gwapi.OutcomeAlreadyDone}
		}
		return gwapi.Result{RequestID: m.RequestID, Outcome: gwapi.OutcomeDone}
	}
	se, ok := errors.AsType[*opapi.SettingsError](err)
	if !ok {
		g.logger.Warn("stand settings change failed", "error", err)
		return settingsRejected(m.RequestID, gwapi.CodeUnavailable, codeTexts[gwapi.CodeUnavailable])
	}
	switch se.Code {
	case opapi.CodeInvalidRequest:
		for _, f := range se.Fields {
			if f.Field != opapi.FieldSim && !slices.Contains(opapi.SettingsFields, f.Field) {
				// base_version or by: the gateway built them, so this is
				// its own bug, not the user's.
				g.logger.Error("the router refused a field of the gateway", "field", f.Field)
				return settingsRejected(m.RequestID, gwapi.CodeInternal, codeTexts[gwapi.CodeInternal])
			}
		}
		return settingsInvalid(m.RequestID, se.Fields)
	case opapi.CodeSettingsConflict:
		g.applySettings(*se.Settings, seq)
		return settingsConflict(m.RequestID, m.BaseVersion, se.Settings.Stand.Version)
	case opapi.CodeSettingsTooSoon:
		r := settingsRejected(m.RequestID, gwapi.CodeSettingsTooSoon, settingsTooSoonText)
		r.RetryAfterMS = se.RetryAfterMS
		return r
	case opapi.CodeSettingsOff:
		return settingsRejected(m.RequestID, gwapi.CodeSettingsOff, settingsOffText)
	case opapi.CodeUnavailable:
		return settingsRejected(m.RequestID, gwapi.CodeUnavailable, codeTexts[gwapi.CodeUnavailable])
	}
	g.logger.Warn("stand settings change failed", "code", se.Code)
	return settingsRejected(m.RequestID, gwapi.CodeInternal, codeTexts[gwapi.CodeInternal])
}

// simPartError checks the rule of the sim part against the current set:
// present when the current set has one, absent when it has none.
func simPartError(current, next opapi.StandSettings) (opapi.FieldError, bool) {
	switch {
	case current.Sim != nil && next.Sim == nil:
		return opapi.FieldError{Field: opapi.FieldSim, Reason: "is missing: the current set has a sim part"}, true
	case current.Sim == nil && next.Sim != nil:
		return opapi.FieldError{Field: opapi.FieldSim, Reason: "must be absent: no simulator has reported yet"}, true
	}
	return opapi.FieldError{}, false
}
