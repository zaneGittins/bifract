package archives

import (
	"context"
	"log"
	"sync"
	"time"

	"bifract/pkg/fractals"
)

// Scheduler runs background scheduled archive creation for fractals.
type Scheduler struct {
	manager        *Manager
	fractalManager *fractals.Manager

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewScheduler creates a new archive scheduler.
func NewScheduler(manager *Manager, fractalManager *fractals.Manager) *Scheduler {
	return &Scheduler{
		manager:        manager,
		fractalManager: fractalManager,
		stopCh:         make(chan struct{}),
	}
}

// Start launches the background scheduler (checks every 5 minutes).
func (s *Scheduler) Start() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		log.Println("[Archives] Scheduler started (checking every 5 minutes)")

		for {
			select {
			case <-ticker.C:
				s.checkAndArchive()
			case <-s.stopCh:
				log.Println("[Archives] Scheduler stopped")
				return
			}
		}
	}()
}

// Stop gracefully shuts down the scheduler.
func (s *Scheduler) Stop() {
	close(s.stopCh)
	s.wg.Wait()
}

func (s *Scheduler) checkAndArchive() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	allFractals, err := s.fractalManager.ListFractals(ctx)
	if err != nil {
		log.Printf("[Archives] Scheduler: failed to list fractals: %v", err)
		return
	}

	now := time.Now()
	for _, fractal := range allFractals {
		if fractal.ArchiveSchedule == "" || fractal.ArchiveSchedule == "never" {
			continue
		}

		if !s.isDue(ctx, fractal, now) {
			continue
		}

		active, err := s.manager.archives.HasActiveOperation(ctx, fractal.ID)
		if err != nil {
			log.Printf("[Archives] Scheduler: failed to check active operation for %s: %v", fractal.Name, err)
			continue
		}
		if active {
			continue
		}

		log.Printf("[Archives] Scheduler: creating scheduled archive for fractal %s (schedule: %s)", fractal.Name, fractal.ArchiveSchedule)
		_, err = s.manager.CreateArchive(ctx, fractal.ID, "admin", fractal.RetentionDays, ArchiveTypeScheduled)
		if err != nil {
			log.Printf("[Archives] Scheduler: failed to create archive for %s: %v", fractal.Name, err)
			continue
		}

		// Enforce max_archives limit
		if fractal.MaxArchives != nil && *fractal.MaxArchives > 0 {
			if err := s.manager.EnforceMaxArchives(ctx, fractal.ID, *fractal.MaxArchives); err != nil {
				log.Printf("[Archives] Scheduler: failed to enforce max_archives for %s: %v", fractal.Name, err)
			}
		}
	}
}

// isDue checks whether a fractal is due for a scheduled archive.
func (s *Scheduler) isDue(ctx context.Context, fractal *fractals.Fractal, now time.Time) bool {
	interval := scheduleInterval(fractal.ArchiveSchedule)
	if interval == 0 {
		return false
	}

	archives, err := s.manager.ListArchives(ctx, fractal.ID)
	if err != nil {
		log.Printf("[Archives] Scheduler: failed to list archives for %s: %v", fractal.Name, err)
		return false
	}

	// Find the most recent completed archive (list is ordered by created_at DESC)
	for _, a := range archives {
		if a.Status == StatusCompleted {
			return now.After(a.CreatedAt.Add(interval))
		}
	}

	// No completed archives, so it's due
	return true
}

func scheduleInterval(schedule string) time.Duration {
	switch schedule {
	case "daily":
		return 24 * time.Hour
	case "weekly":
		return 7 * 24 * time.Hour
	case "monthly":
		return 30 * 24 * time.Hour
	default:
		return 0
	}
}
