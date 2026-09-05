ALTER TABLE workers ADD COLUMN instance_id TEXT NOT NULL DEFAULT '';
ALTER TABLE workers ADD COLUMN sandbox_id TEXT NOT NULL DEFAULT '';
ALTER TABLE workers ADD COLUMN sandbox_provider TEXT NOT NULL DEFAULT '';
ALTER TABLE workers ADD COLUMN max_concurrency INTEGER NOT NULL DEFAULT 1;
ALTER TABLE workers ADD COLUMN available_slots INTEGER NOT NULL DEFAULT 1;
CREATE UNIQUE INDEX workers_instance_idx ON workers(instance_id) WHERE instance_id<>'';
