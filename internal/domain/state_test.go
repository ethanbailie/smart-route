package domain

import "testing"

func TestCanTransition(t *testing.T) {
	states := []JobState{
		JobQueued,
		JobLeased,
		JobRunning,
		JobSucceeded,
		JobFailed,
		JobCanceled,
		JobTimedOut,
	}
	valid := map[[2]JobState]bool{
		{JobQueued, JobLeased}:     true,
		{JobQueued, JobCanceled}:   true,
		{JobQueued, JobTimedOut}:   true,
		{JobLeased, JobRunning}:    true,
		{JobLeased, JobQueued}:     true,
		{JobLeased, JobCanceled}:   true,
		{JobLeased, JobTimedOut}:   true,
		{JobRunning, JobSucceeded}: true,
		{JobRunning, JobFailed}:    true,
		{JobRunning, JobQueued}:    true,
		{JobRunning, JobCanceled}:  true,
		{JobRunning, JobTimedOut}:  true,
	}

	for _, from := range states {
		for _, to := range states {
			if got, want := CanTransition(from, to), valid[[2]JobState{from, to}]; got != want {
				t.Errorf("CanTransition(%q, %q) = %v, want %v", from, to, got, want)
			}
		}
	}
}
