package sqlite

import (
	"context"
	"database/sql"

	"github.com/ethan/smart-route/internal/domain"
)

func (s *DB) ListWorkers(ctx context.Context) ([]domain.Worker, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM workers ORDER BY id`)
	if err != nil {
		return nil, err
	}
	workers := make([]domain.Worker, 0)
	for rows.Next() {
		var worker domain.Worker
		if err := rows.Scan(&worker.ID); err != nil {
			rows.Close()
			return nil, err
		}
		workers = append(workers, worker)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	for i := range workers {
		workers[i], err = s.GetWorker(ctx, workers[i].ID)
		if err != nil {
			return nil, err
		}
	}
	return workers, nil
}

func (s *DB) ListSandboxes(ctx context.Context) ([]domain.Sandbox, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,worker_id,capabilities_json,state,created_at,provider,external_id,updated_at,drain_at,reserved_session_id FROM sandboxes ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	sandboxes := make([]domain.Sandbox, 0)
	for rows.Next() {
		var sandbox domain.Sandbox
		var capabilities string
		var worker sql.NullString
		var updated, drain sql.NullTime
		if err := rows.Scan(&sandbox.ID, &worker, &capabilities, &sandbox.State, &sandbox.CreatedAt, &sandbox.Provider, &sandbox.ExternalID, &updated, &drain, &sandbox.ReservedSessionID); err != nil {
			return nil, err
		}
		sandbox.WorkerID = domain.WorkerID(worker.String)
		if updated.Valid {
			sandbox.UpdatedAt = updated.Time
		}
		if drain.Valid {
			sandbox.DrainAt = drain.Time
		}
		if err := dec(capabilities, &sandbox.Capabilities); err != nil {
			return nil, err
		}
		sandboxes = append(sandboxes, sandbox)
	}
	return sandboxes, rows.Err()
}
