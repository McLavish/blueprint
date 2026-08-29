package rpcpolicy

import (
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Live profile reload (feature F9).
//
// A SIGHUP alone is not enough here: the generated container's PID 1 is
// `sh ./run.sh` with the Go binary backgrounded, so there is no reliable way to
// signal the process from `docker kill -s HUP`. The primary trigger is
// therefore the interceptor watching its own config file; SIGHUP stays as a
// secondary path for local debugging.

// reloadPollInterval is how often the watcher stats the config file. The arm
// step waits up to 2 s for the reload event, so 200 ms leaves ample margin.
const reloadPollInterval = 200 * time.Millisecond

// Stages a test hook observes inside the reload transaction.
const (
	reloadStageRead   = "read"
	reloadStageStored = "stored"
)

func (s *runtimeState) startWatching() {
	s.watchers.Add(1)
	go s.watchFile()
	// Register the signal handler SYNCHRONOUSLY: doing it inside the goroutine
	// leaves a window in which SIGHUP still has its default action, which is to
	// terminate the process -- exactly the opposite of what F9 wants.
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP)
	s.watchers.Add(1)
	go s.watchSignal(ch)
}

// watchFile polls mtime first and only hashes when it moved: at 200 ms a stat
// per tick is free, a sha256 of the file is not.
//
// The baseline starts EMPTY rather than being seeded with a stat at startup, so
// a file rewritten in the window between the initial read and the first tick is
// still noticed. The first tick therefore always calls reload(), which
// short-circuits when the bytes hash to what is already loaded.
func (s *runtimeState) watchFile() {
	defer s.watchers.Done()
	t := time.NewTicker(reloadPollInterval)
	defer t.Stop()
	var lastMod time.Time
	var lastSize int64 = -1
	for {
		select {
		case <-s.stopReload:
			return
		case <-t.C:
			fi, err := os.Stat(s.configPath)
			if err != nil {
				// A vanished or unreadable config file is not "no change": the
				// old registry stays live, and the log says so ONCE per run of
				// failures rather than five times a second forever.
				s.noteReadFailure(err)
				continue
			}
			if fi.ModTime().Equal(lastMod) && fi.Size() == lastSize {
				continue
			}
			lastMod, lastSize = fi.ModTime(), fi.Size()
			s.reload()
		}
	}
}

func (s *runtimeState) watchSignal(ch chan os.Signal) {
	defer s.watchers.Done()
	defer signal.Stop(ch)
	for {
		select {
		case <-s.stopReload:
			return
		case <-ch:
			s.reload()
		}
	}
}

// reload re-reads the config and swaps the registry. Profiles whose serialized
// block is unchanged keep their objects, so breaker and budget state survives a
// reload that only touched a different profile. An unloadable file keeps the
// OLD registry and records the failure -- the opposite of startup, where an
// unloadable file panics, because a live recording must not be destroyed by a
// bad edit.
func (s *runtimeState) reload() {
	// The WHOLE transaction -- read, hash compare, parse, build, store, event --
	// runs under one mutex. The file poller and the SIGHUP handler are two
	// independent triggers; interleaved, the one that read the OLDER bytes could
	// store last and leave a stale registry installed with the newer file's
	// event already written.
	s.reloadMu.Lock()
	defer s.reloadMu.Unlock()

	data, err := os.ReadFile(s.configPath)
	if err != nil {
		s.noteReadFailureLocked(err)
		return
	}
	s.readFailed = false
	if s.reloadHook != nil {
		s.reloadHook(reloadStageRead)
	}
	sha := sha256Hex(data)
	if prev := s.registry.Load(); prev != nil && prev.sha == sha {
		// Content-identical rewrite (a `cp` of the same bytes): nothing to do,
		// and no event, so the harness's "reload happened" assertion stays
		// meaningful.
		return
	}
	cfg, err := ParseConfig(data, s.configPath)
	if err != nil {
		s.writeReloadEvent(eventPolicyReloadFailed, sha, err.Error())
		return
	}
	prev := s.registry.Load()
	reg, err := buildRegistry(cfg, sha, prev, s.clock)
	if err != nil {
		s.writeReloadEvent(eventPolicyReloadFailed, sha, err.Error())
		return
	}
	note := ""
	if prev != nil && serverBlockChanged(prev.server, cfg.Server) {
		note = "server: is not hot-reloaded; the running station is unchanged"
	}
	s.registry.Store(reg)
	s.writeReloadEvent(eventPolicyReload, sha, note)
	if s.reloadHook != nil {
		s.reloadHook(reloadStageStored)
	}
}

// noteReadFailure records a stat/read failure of the watched file from OUTSIDE
// the reload transaction (the poller's stat).
func (s *runtimeState) noteReadFailure(err error) {
	s.reloadMu.Lock()
	defer s.reloadMu.Unlock()
	s.noteReadFailureLocked(err)
}

// noteReadFailureLocked emits ONE policy_reload_failed event per run of failures
// to read the watched file. Deduplicated until the next successful read, so a
// file deleted mid-run costs one line rather than five a second, and keeps the
// old registry either way. Callers hold reloadMu.
func (s *runtimeState) noteReadFailureLocked(err error) {
	if s.readFailed {
		return
	}
	s.readFailed = true
	s.writeReloadEvent(eventPolicyReloadFailed, "", "cannot read "+s.configPath+": "+err.Error())
}

// writeReloadEvent records the instant and flushes: the arm step tails the log
// for this line, so it must not sit in the buffer.
func (s *runtimeState) writeReloadEvent(name, sha, note string) {
	_ = s.log.Write(&EventRecord{
		Kind:    "event",
		Name:    name,
		Epoch:   epochOf(time.Now()),
		SHA256:  sha,
		Service: s.service,
		Note:    note,
	})
	_ = s.log.Flush()
}
