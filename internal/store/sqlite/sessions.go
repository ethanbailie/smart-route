package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ethan/smart-route/internal/domain"
	"github.com/ethan/smart-route/internal/store"
)

func (s *DB) CreateSession(ctx context.Context, v domain.Session) (domain.Session, error) {
	if v.ID == "" || v.Pool == "" || v.IdleTTL < 0 || v.MaxLifetime < 0 {
		return v, store.ErrConflict
	}
	if v.State == "" {
		v.State = domain.SessionPending
	}
	if v.State != domain.SessionPending {
		return v, store.ErrConflict
	}
	if v.RecoveryPolicy == "" {
		v.RecoveryPolicy = domain.RecoveryNone
	}
	if v.CheckpointMode == "" {
		v.CheckpointMode = domain.CheckpointExplicit
	}
	if v.Epoch == 0 {
		v.Epoch = 1
	}
	if v.RecoveryState == "" {
		v.RecoveryState = domain.RecoveryIdle
	}
	if v.RecoveryPolicy != domain.RecoveryNone && v.RecoveryPolicy != domain.RecoveryCheckpoint && v.RecoveryPolicy != domain.RecoveryRebuild {
		return v, store.ErrConflict
	}
	if v.CheckpointMode != domain.CheckpointExplicit && v.CheckpointMode != domain.CheckpointAfterSuccess {
		return v, store.ErrConflict
	}
	if v.RecoveryPolicy == domain.RecoveryRebuild {
		if len(v.RebuildPlan) == 0 {
			return v, store.ErrConflict
		}
		for _, step := range v.RebuildPlan {
			if step.Kind == "" || step.IdempotencyKey == "" {
				return v, store.ErrConflict
			}
		}
	}
	n := time.Now().UTC()
	if v.CreatedAt.IsZero() {
		v.CreatedAt = n
	}
	if v.LastActivity.IsZero() {
		v.LastActivity = v.CreatedAt
	}
	if v.IdleTTL > 0 {
		v.IdleExpiresAt = v.LastActivity.Add(v.IdleTTL)
	}
	c, _ := enc(v.Capabilities)
	labels, _ := enc(v.Labels)
	rebuild, _ := enc(v.RebuildPlan)
	_, err := s.db.ExecContext(ctx, `INSERT INTO sessions(id,pool,capabilities_json,preferred_provider,labels_json,state,idle_ttl_ns,max_lifetime_ns,created_at,last_activity,idle_expires_at,recovery_policy,checkpoint_mode,rebuild_plan_json,epoch,recovery_state) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, v.ID, v.Pool, c, v.PreferredProvider, labels, v.State, int64(v.IdleTTL), int64(v.MaxLifetime), v.CreatedAt, v.LastActivity, nullTime(v.IdleExpiresAt), v.RecoveryPolicy, v.CheckpointMode, rebuild, v.Epoch, v.RecoveryState)
	return v, mapErr(err)
}
func scanSession(row *sql.Row) (domain.Session, error) {
	var v domain.Session
	var caps, labels string
	var sid, wid, state string
	var idle, max int64
	var expiry, closed sql.NullTime
	var failure sql.NullString
	var recoveryAfter sql.NullTime
	var rebuild string
	err := row.Scan(&v.ID, &v.Pool, &caps, &v.PreferredProvider, &labels, &sid, &wid, &state, &idle, &max, &v.CreatedAt, &v.LastActivity, &expiry, &closed, &failure, &v.RecoveryPolicy, &v.CheckpointMode, &rebuild, &v.Epoch, &v.RecoveryState, &v.RecoveryAttempts, &recoveryAfter, &v.RecoveryError, &v.LatestCheckpointID, &v.RestoreAcknowledged)
	if err != nil {
		return v, mapErr(err)
	}
	v.SandboxID = domain.SandboxID(sid)
	v.WorkerID = domain.WorkerID(wid)
	v.State = domain.SessionState(state)
	v.IdleTTL = time.Duration(idle)
	v.MaxLifetime = time.Duration(max)
	if expiry.Valid {
		v.IdleExpiresAt = expiry.Time
	}
	if closed.Valid {
		v.ClosedAt = closed.Time
	}
	if err = dec(caps, &v.Capabilities); err != nil {
		return v, err
	}
	if err = dec(labels, &v.Labels); err != nil {
		return v, err
	}
	if failure.Valid {
		v.Failure = &domain.Failure{}
		err = dec(failure.String, v.Failure)
	}
	if err == nil {
		err = dec(rebuild, &v.RebuildPlan)
	}
	if recoveryAfter.Valid {
		v.RecoveryAfter = recoveryAfter.Time
	}
	return v, err
}

const sessionColumns = `id,pool,capabilities_json,preferred_provider,labels_json,COALESCE(sandbox_id,''),COALESCE(worker_id,''),state,idle_ttl_ns,max_lifetime_ns,created_at,last_activity,idle_expires_at,closed_at,failure_json,recovery_policy,checkpoint_mode,rebuild_plan_json,epoch,recovery_state,recovery_attempts,recovery_after,recovery_error,latest_checkpoint_id,restore_acknowledged`

func (s *DB) GetSession(ctx context.Context, id domain.SessionID) (domain.Session, error) {
	return scanSession(s.db.QueryRowContext(ctx, `SELECT `+sessionColumns+` FROM sessions WHERE id=?`, id))
}
func (s *DB) ListSessions(ctx context.Context, states ...domain.SessionState) ([]domain.Session, error) {
	q := `SELECT id FROM sessions`
	args := make([]any, len(states))
	if len(states) > 0 {
		p := make([]string, len(states))
		for i, v := range states {
			p[i] = "?"
			args[i] = v
		}
		q += ` WHERE state IN (` + strings.Join(p, ",") + `)`
	}
	q += ` ORDER BY created_at,id`
	ids, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	var keys []domain.SessionID
	for ids.Next() {
		var id domain.SessionID
		if err = ids.Scan(&id); err != nil {
			return nil, err
		}
		keys = append(keys, id)
	}
	if err = ids.Close(); err != nil {
		return nil, err
	}
	var out []domain.Session
	for _, id := range keys {
		v, e := s.GetSession(ctx, id)
		if e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, ids.Err()
}
func (s *DB) ListSessionJobs(ctx context.Context, id domain.SessionID) ([]domain.Job, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM jobs WHERE session_id=? ORDER BY created_at,id`, id)
	if err != nil {
		return nil, err
	}
	var keys []domain.JobID
	for rows.Next() {
		var jid domain.JobID
		if err = rows.Scan(&jid); err != nil {
			return nil, err
		}
		keys = append(keys, jid)
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	var out []domain.Job
	for _, jid := range keys {
		j, e := s.GetJob(ctx, jid)
		if e != nil {
			return nil, e
		}
		out = append(out, j)
	}
	return out, rows.Err()
}
func (s *DB) BindSession(ctx context.Context, id domain.SessionID, w domain.WorkerID, b domain.SandboxID, at time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var idle int64
	if err = tx.QueryRowContext(ctx, `SELECT idle_ttl_ns FROM sessions WHERE id=?`, id).Scan(&idle); err != nil {
		return mapErr(err)
	}
	var expiry any
	if idle > 0 {
		expiry = at.Add(time.Duration(idle)).UTC()
	}
	res, err := tx.ExecContext(ctx, `UPDATE sessions SET worker_id=?,sandbox_id=?,state='active',last_activity=?,idle_expires_at=? WHERE id=? AND state='pending' AND NOT EXISTS(SELECT 1 FROM sessions WHERE worker_id=? AND state='active')`, w, b, at, expiry, id, w)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return store.ErrConflict
	}
	var epoch uint64
	if err = tx.QueryRowContext(ctx, `SELECT epoch FROM sessions WHERE id=?`, id).Scan(&epoch); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE workers SET reserved_session_id=?,session_epoch=? WHERE id=? AND COALESCE(reserved_session_id,'') IN ('',?)`, id, epoch, w, id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE sandboxes SET reserved_session_id=?,updated_at=? WHERE id=? AND COALESCE(reserved_session_id,'') IN ('',?)`, id, at, b, id); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *DB) CloseSession(ctx context.Context, id domain.SessionID, at time.Time) error {
	return s.finishSession(ctx, id, domain.SessionClosed, "session_closed", at)
}
func (s *DB) finishSession(ctx context.Context, id domain.SessionID, state domain.SessionState, code string, at time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = finishSessionTx(ctx, tx, id, state, code, at); err != nil {
		return err
	}
	return tx.Commit()
}
func finishSessionTx(ctx context.Context, tx *sql.Tx, id domain.SessionID, state domain.SessionState, code string, at time.Time) error {
	var err error
	var w, b string
	var old string
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(worker_id,''),COALESCE(sandbox_id,''),state FROM sessions WHERE id=?`, id).Scan(&w, &b, &old); err != nil {
		return mapErr(err)
	}
	if domain.SessionState(old).Terminal() {
		return nil
	}
	f := domain.Failure{Code: code, Message: code, Class: domain.FailureNonRetryable}
	fj, _ := enc(f)
	var fail any
	if state == domain.SessionLost {
		fail = fj
	}
	if _, err = tx.ExecContext(ctx, `UPDATE sessions SET state=?,closed_at=?,failure_json=? WHERE id=?`, state, at, fail, id); err != nil {
		return err
	}
	jobState := domain.JobCanceled
	if state == domain.SessionLost {
		jobState = domain.JobSessionLost
	}
	rows, err := tx.QueryContext(ctx, `SELECT id FROM jobs WHERE session_id=? AND state IN ('waiting','queued','leased','running') ORDER BY id`, id)
	if err != nil {
		return err
	}
	var changed []domain.JobID
	for rows.Next() {
		var jid domain.JobID
		if err = rows.Scan(&jid); err != nil {
			rows.Close()
			return err
		}
		changed = append(changed, jid)
	}
	if err = rows.Close(); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE jobs SET state=?,updated_at=? WHERE session_id=? AND state IN ('waiting','queued','leased','running')`, jobState, at, id); err != nil {
		return err
	}
	for _, jid := range changed {
		if err = appendTerminalEvent(ctx, tx, jid, jobState, code, at); err != nil {
			return err
		}
	}
	attemptState := domain.AttemptCanceled
	if state == domain.SessionLost {
		attemptState = domain.AttemptLost
	}
	if _, err = tx.ExecContext(ctx, `UPDATE job_attempts SET state=?,failure_json=?,ended_at=? WHERE job_id IN (SELECT id FROM jobs WHERE session_id=?) AND state IN ('leased','running')`, attemptState, fj, at, id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM leases WHERE attempt_id IN (SELECT a.id FROM job_attempts a JOIN jobs j ON j.id=a.job_id WHERE j.session_id=?)`, id); err != nil {
		return err
	}
	if w != "" {
		_, err = tx.ExecContext(ctx, `UPDATE workers SET reserved_session_id='' WHERE id=? AND reserved_session_id=?`, w, id)
		if err != nil {
			return err
		}
	}
	if b != "" {
		_, err = tx.ExecContext(ctx, `UPDATE sandboxes SET reserved_session_id='',state=CASE WHEN state IN ('ready','running') THEN 'draining' ELSE state END,drain_at=CASE WHEN state IN ('ready','running') THEN ? ELSE drain_at END WHERE id=? AND reserved_session_id=?`, at, b, id)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *DB) ExpireSessions(ctx context.Context, at time.Time) ([]domain.SessionID, error) {
	items, err := s.ListSessions(ctx, domain.SessionPending, domain.SessionActive)
	if err != nil {
		return nil, err
	}
	var ids []domain.SessionID
	for _, v := range items {
		maxed := v.MaxLifetime > 0 && !at.Before(v.CreatedAt.Add(v.MaxLifetime))
		idle := !v.IdleExpiresAt.IsZero() && !at.Before(v.IdleExpiresAt)
		if idle && !maxed {
			var active int
			if err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM job_attempts a JOIN jobs j ON j.id=a.job_id WHERE j.session_id=? AND a.state IN ('leased','running')`, v.ID).Scan(&active); err != nil {
				return nil, err
			}
			idle = active == 0
		}
		if idle || maxed {
			ids = append(ids, v.ID)
		}
	}
	for _, id := range ids {
		if err = s.CloseSession(ctx, id, at); err != nil {
			return nil, err
		}
	}
	return ids, nil
}
func (s *DB) validateDependencies(ctx context.Context, j domain.Job) error {
	seen := map[domain.JobID]bool{}
	for _, id := range j.DependsOn {
		if id == j.ID || seen[id] {
			return fmt.Errorf("%w: dependency cycle or duplicate", store.ErrConflict)
		}
		seen[id] = true
		var session sql.NullString
		if err := s.db.QueryRowContext(ctx, `SELECT session_id FROM jobs WHERE id=?`, id).Scan(&session); errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: dependency not found", store.ErrNotFound)
		} else if err != nil {
			return err
		}
		if domain.SessionID(session.String) != j.SessionID {
			return fmt.Errorf("%w: dependencies must share session", store.ErrConflict)
		}
	}
	if j.SessionID != "" {
		var state string
		if err := s.db.QueryRowContext(ctx, `SELECT state FROM sessions WHERE id=?`, j.SessionID).Scan(&state); err != nil {
			return mapErr(err)
		}
		if domain.SessionState(state).Terminal() || state == string(domain.SessionDraining) {
			return store.ErrConflict
		}
	}
	return nil
}
func propagateDependencyFailure(ctx context.Context, tx *sql.Tx, root domain.JobID, at time.Time) error {
	rows, err := tx.QueryContext(ctx, `WITH RECURSIVE downstream(id) AS (
	 SELECT j.id FROM jobs j,json_each(j.depends_on_json) d WHERE d.value=? AND j.state IN ('waiting','queued')
	 UNION SELECT j.id FROM jobs j,json_each(j.depends_on_json) d JOIN downstream x ON d.value=x.id WHERE j.state IN ('waiting','queued')
	) SELECT id FROM downstream ORDER BY id`, root)
	if err != nil {
		return err
	}
	var ids []domain.JobID
	for rows.Next() {
		var id domain.JobID
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	if err = rows.Close(); err != nil {
		return err
	}
	for _, id := range ids {
		if _, err = tx.ExecContext(ctx, `UPDATE jobs SET state='dependency_failed',updated_at=? WHERE id=? AND state IN ('waiting','queued')`, at, id); err != nil {
			return err
		}
		if err = appendTerminalEvent(ctx, tx, id, domain.JobDependencyFailed, "dependency_failed", at); err != nil {
			return err
		}
	}
	return nil
}
func appendTerminalEvent(ctx context.Context, tx *sql.Tx, id domain.JobID, state domain.JobState, reason string, at time.Time) error {
	var seq uint64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence),0)+1 FROM job_events WHERE job_id=?`, id).Scan(&seq); err != nil {
		return err
	}
	data, _ := enc(map[string]string{"job_state": string(state), "reason": reason})
	_, err := tx.ExecContext(ctx, `INSERT INTO job_events(id,sequence,type,job_id,occurred_at,data_json) VALUES(?,?,?,?,?,?)`, fmt.Sprintf("%s-%s-%d", reason, id, at.UnixNano()), seq, domain.EventJobTransition, id, at, data)
	return err
}

func (s *DB) RequestRecovery(ctx context.Context, id domain.SessionID, at time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE sessions SET state='recovering',recovery_state='pending',epoch=epoch+1,worker_id='',sandbox_id='',restore_acknowledged=0,recovery_after=?,recovery_error='' WHERE id=? AND state IN ('active','recovering') AND recovery_policy!='none'`, at.UTC(), id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return store.ErrConflict
	}
	var epoch uint64
	if err = tx.QueryRowContext(ctx, `SELECT epoch FROM sessions WHERE id=?`, id).Scan(&epoch); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO session_recovery_events(id,session_id,epoch,stage,message,occurred_at)VALUES(?,?,?,?,?,?)`, fmt.Sprintf("recovery-requested-%s-%d", id, epoch), id, epoch, "requested", "", at.UTC()); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *DB) ClaimRecovery(ctx context.Context, id domain.SessionID, at time.Time) (domain.Session, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Session{}, err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE sessions SET recovery_state='provisioning',recovery_attempts=recovery_attempts+1 WHERE id=? AND state='recovering' AND recovery_state IN ('pending','failed') AND (recovery_after IS NULL OR recovery_after<=?)`, id, at.UTC())
	if err != nil {
		return domain.Session{}, err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return domain.Session{}, store.ErrConflict
	}
	v, err := scanSession(tx.QueryRowContext(ctx, `SELECT `+sessionColumns+` FROM sessions WHERE id=?`, id))
	if err != nil {
		return domain.Session{}, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO session_recovery_events(id,session_id,epoch,stage,message,occurred_at)VALUES(?,?,?,?,?,?)`, fmt.Sprintf("recovery-claimed-%s-%d-%d", id, v.Epoch, v.RecoveryAttempts), id, v.Epoch, "provisioning", "", at.UTC()); err != nil {
		return domain.Session{}, err
	}
	if err = tx.Commit(); err != nil {
		return domain.Session{}, err
	}
	return v, nil
}
func (s *DB) CompleteRecovery(ctx context.Context, id domain.SessionID, epoch uint64, w domain.WorkerID, b domain.SandboxID, at time.Time) error {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	res, e := tx.ExecContext(ctx, `UPDATE sessions SET state='active',recovery_state='idle',worker_id=?,sandbox_id=?,last_activity=?,recovery_after=NULL,recovery_error='' WHERE id=? AND epoch=? AND state='recovering' AND restore_acknowledged=1`, w, b, at.UTC(), id, epoch)
	if e != nil {
		return e
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return store.ErrConflict
	}
	_, e = tx.ExecContext(ctx, `UPDATE workers SET reserved_session_id=?,session_epoch=? WHERE id=?`, id, epoch, w)
	if e != nil {
		return e
	}
	if _, e = tx.ExecContext(ctx, `INSERT OR IGNORE INTO session_recovery_events(id,session_id,epoch,stage,message,occurred_at)VALUES(?,?,?,?,?,?)`, fmt.Sprintf("recovery-complete-%s-%d", id, epoch), id, epoch, "completed", "", at.UTC()); e != nil {
		return e
	}
	return tx.Commit()
}
func (s *DB) FailRecovery(ctx context.Context, id domain.SessionID, epoch uint64, msg string, retry, timeNow time.Time) error {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	res, e := tx.ExecContext(ctx, `UPDATE sessions SET recovery_state='failed',recovery_error=?,recovery_after=? WHERE id=? AND epoch=? AND state='recovering'`, msg, retry.UTC(), id, epoch)
	if e != nil {
		return e
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return store.ErrConflict
	}
	if _, e = tx.ExecContext(ctx, `UPDATE workers SET reserved_session_id='',session_epoch=0 WHERE reserved_session_id=? AND session_epoch=?`, id, epoch); e != nil {
		return e
	}
	_, e = tx.ExecContext(ctx, `INSERT OR IGNORE INTO session_recovery_events(id,session_id,epoch,stage,message,occurred_at)VALUES(?,?,?,?,?,?)`, fmt.Sprintf("recovery-failed-%s-%d-%d", id, epoch, timeNow.UnixNano()), id, epoch, "retry_wait", msg, timeNow.UTC())
	if e != nil {
		return e
	}
	return tx.Commit()
}
func (s *DB) AcknowledgeRecovery(ctx context.Context, id domain.SessionID, epoch uint64, w domain.WorkerID, at time.Time) error {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	res, e := tx.ExecContext(ctx, `UPDATE sessions SET restore_acknowledged=1,recovery_state='restoring' WHERE id=? AND epoch=? AND state='recovering' AND EXISTS(SELECT 1 FROM workers WHERE id=? AND reserved_session_id=? AND session_epoch=?)`, id, epoch, w, id, epoch)
	if e != nil {
		return e
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return store.ErrConflict
	}
	if _, e = tx.ExecContext(ctx, `INSERT OR IGNORE INTO session_recovery_events(id,session_id,epoch,stage,message,occurred_at)VALUES(?,?,?,?,?,?)`, fmt.Sprintf("recovery-ack-%s-%d", id, epoch), id, epoch, "validated", "", at.UTC()); e != nil {
		return e
	}
	return tx.Commit()
}
func (s *DB) TerminalRecovery(ctx context.Context, id domain.SessionID, epoch uint64, msg string, at time.Time) error {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var currentEpoch uint64
	var state domain.SessionState
	if e = tx.QueryRowContext(ctx, `SELECT epoch,state FROM sessions WHERE id=?`, id).Scan(&currentEpoch, &state); e != nil {
		return mapErr(e)
	}
	if currentEpoch != epoch || state != domain.SessionRecovering {
		return store.ErrConflict
	}
	if e = finishSessionTx(ctx, tx, id, domain.SessionLost, "recovery_failed", at); e != nil {
		return e
	}
	f, _ := enc(domain.Failure{Code: "recovery_failed", Message: msg, Class: domain.FailureNonRetryable})
	if _, e = tx.ExecContext(ctx, `UPDATE sessions SET recovery_state='failed',recovery_error=?,failure_json=? WHERE id=?`, msg, f, id); e != nil {
		return e
	}
	if _, e = tx.ExecContext(ctx, `UPDATE workers SET reserved_session_id='',session_epoch=0 WHERE reserved_session_id=? AND session_epoch=?`, id, epoch); e != nil {
		return e
	}
	if _, e = tx.ExecContext(ctx, `UPDATE sandboxes SET reserved_session_id='',state=CASE WHEN state IN ('ready','running') THEN 'draining' ELSE state END,drain_at=CASE WHEN state IN ('ready','running') THEN ? ELSE drain_at END WHERE reserved_session_id=?`, at.UTC(), id); e != nil {
		return e
	}
	if _, e = tx.ExecContext(ctx, `INSERT OR IGNORE INTO session_recovery_events(id,session_id,epoch,stage,message,occurred_at)VALUES(?,?,?,?,?,?)`, fmt.Sprintf("recovery-terminal-%s-%d", id, epoch), id, epoch, "terminal_failed", msg, at.UTC()); e != nil {
		return e
	}
	return tx.Commit()
}
func (s *DB) AppendRecoveryEvent(ctx context.Context, e domain.RecoveryEvent) error {
	_, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO session_recovery_events(id,session_id,epoch,stage,message,occurred_at)VALUES(?,?,?,?,?,?)`, e.ID, e.SessionID, e.Epoch, e.Stage, e.Message, e.OccurredAt.UTC())
	return err
}
func (s *DB) ListRecoveryEvents(ctx context.Context, id domain.SessionID) ([]domain.RecoveryEvent, error) {
	rows, e := s.db.QueryContext(ctx, `SELECT id,session_id,epoch,stage,message,occurred_at FROM session_recovery_events WHERE session_id=? ORDER BY occurred_at,id`, id)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []domain.RecoveryEvent
	for rows.Next() {
		var v domain.RecoveryEvent
		if e = rows.Scan(&v.ID, &v.SessionID, &v.Epoch, &v.Stage, &v.Message, &v.OccurredAt); e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (s *DB) ListRecoveringSessions(ctx context.Context, at time.Time) ([]domain.Session, error) {
	all, e := s.ListSessions(ctx, domain.SessionRecovering)
	if e != nil {
		return nil, e
	}
	out := all[:0]
	for _, v := range all {
		if v.RecoveryAfter.IsZero() || !at.Before(v.RecoveryAfter) {
			out = append(out, v)
		}
	}
	return out, nil
}

func (s *DB) CreateCheckpoint(ctx context.Context, c domain.Checkpoint) error {
	if c.ID == "" || c.SessionID == "" || c.Epoch == 0 || c.Adapter == "" {
		return store.ErrConflict
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var epoch uint64
	if e = tx.QueryRowContext(ctx, `SELECT epoch FROM sessions WHERE id=?`, c.SessionID).Scan(&epoch); e != nil {
		return mapErr(e)
	}
	if epoch != c.Epoch {
		return store.ErrConflict
	}
	if c.Sequence == 0 {
		if e = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence),0)+1 FROM session_checkpoints WHERE session_id=?`, c.SessionID).Scan(&c.Sequence); e != nil {
			return e
		}
	}
	if c.State == "" {
		c.State = "creating"
	}
	_, e = tx.ExecContext(ctx, `INSERT INTO session_checkpoints(id,session_id,epoch,sequence,adapter,location,checksum,size,state,created_at,expires_at,error) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, c.ID, c.SessionID, c.Epoch, c.Sequence, c.Adapter, c.Location, c.Checksum, c.Size, c.State, c.CreatedAt.UTC(), nullTime(c.ExpiresAt), c.Error)
	if e != nil {
		return mapErr(e)
	}
	return tx.Commit()
}
func (s *DB) CompleteCheckpoint(ctx context.Context, id string, sid domain.SessionID, epoch uint64, location, checksum string, size int64, at time.Time) error {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	res, e := tx.ExecContext(ctx, `UPDATE session_checkpoints SET state='ready',location=?,checksum=?,size=? WHERE id=? AND session_id=? AND epoch=? AND state='creating'`, location, checksum, size, id, sid, epoch)
	if e != nil {
		return e
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return store.ErrConflict
	}
	res, e = tx.ExecContext(ctx, `UPDATE sessions SET latest_checkpoint_id=? WHERE id=? AND epoch=?`, id, sid, epoch)
	if e != nil {
		return e
	}
	n, _ = res.RowsAffected()
	if n != 1 {
		return store.ErrConflict
	}
	_ = at
	return tx.Commit()
}
func (s *DB) ListCheckpoints(ctx context.Context, sid domain.SessionID) ([]domain.Checkpoint, error) {
	return s.listCheckpoints(ctx, ` WHERE session_id=? ORDER BY sequence DESC`, sid)
}
func (s *DB) ListAllCheckpoints(ctx context.Context) ([]domain.Checkpoint, error) {
	return s.listCheckpoints(ctx, ` ORDER BY session_id,sequence DESC`)
}
func (s *DB) listCheckpoints(ctx context.Context, suffix string, args ...any) ([]domain.Checkpoint, error) {
	rows, e := s.db.QueryContext(ctx, `SELECT id,session_id,epoch,sequence,adapter,location,checksum,size,state,created_at,expires_at,error FROM session_checkpoints`+suffix, args...)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []domain.Checkpoint
	for rows.Next() {
		var c domain.Checkpoint
		var exp sql.NullTime
		if e = rows.Scan(&c.ID, &c.SessionID, &c.Epoch, &c.Sequence, &c.Adapter, &c.Location, &c.Checksum, &c.Size, &c.State, &c.CreatedAt, &exp, &c.Error); e != nil {
			return nil, e
		}
		if exp.Valid {
			c.ExpiresAt = exp.Time
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
func (s *DB) MarkCheckpoint(ctx context.Context, id, state, message string) error {
	if state != "partial" && state != "corrupt" {
		return store.ErrConflict
	}
	res, e := s.db.ExecContext(ctx, `UPDATE session_checkpoints SET state=?,error=? WHERE id=?`, state, message, id)
	if e != nil {
		return e
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return store.ErrNotFound
	}
	return nil
}
func (s *DB) GarbageCollectCheckpoints(ctx context.Context, at time.Time, retainLatest int, deleteOnClose bool) ([]domain.Checkpoint, error) {
	if retainLatest < 0 {
		retainLatest = 0
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback()
	rows, e := tx.QueryContext(ctx, `SELECT c.id,c.session_id,c.epoch,c.sequence,c.adapter,c.location,c.checksum,c.size,c.state,c.created_at,c.expires_at,c.error,s.state
		FROM session_checkpoints c JOIN sessions s ON s.id=c.session_id
		ORDER BY c.session_id,c.sequence DESC,c.created_at DESC,c.id`)
	if e != nil {
		return nil, e
	}
	var out []domain.Checkpoint
	rank := map[domain.SessionID]int{}
	affected := map[domain.SessionID]bool{}
	for rows.Next() {
		var c domain.Checkpoint
		var exp sql.NullTime
		var sessionState domain.SessionState
		if e = rows.Scan(&c.ID, &c.SessionID, &c.Epoch, &c.Sequence, &c.Adapter, &c.Location, &c.Checksum, &c.Size, &c.State, &c.CreatedAt, &exp, &c.Error, &sessionState); e != nil {
			rows.Close()
			return nil, e
		}
		if exp.Valid {
			c.ExpiresAt = exp.Time
		}
		remove := c.State == "corrupt" || c.State == "partial"
		if deleteOnClose && sessionState.Terminal() {
			remove = true
		} else if c.State == "ready" {
			rank[c.SessionID]++
			if rank[c.SessionID] <= retainLatest {
				remove = false
			} else if exp.Valid && !at.Before(exp.Time) {
				remove = true
			}
		} else if exp.Valid && !at.Before(exp.Time) {
			remove = true
		}
		if remove {
			out = append(out, c)
			affected[c.SessionID] = true
		}
	}
	if e = rows.Close(); e != nil {
		return nil, e
	}
	for _, c := range out {
		if _, e = tx.ExecContext(ctx, `DELETE FROM session_checkpoints WHERE id=?`, c.ID); e != nil {
			return nil, e
		}
	}
	for id := range affected {
		if _, e = tx.ExecContext(ctx, `UPDATE sessions SET latest_checkpoint_id=COALESCE((SELECT id FROM session_checkpoints WHERE session_id=? AND state='ready' ORDER BY sequence DESC,created_at DESC,id LIMIT 1),'') WHERE id=?`, id, id); e != nil {
			return nil, e
		}
	}
	return out, tx.Commit()
}
