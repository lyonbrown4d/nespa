package control

import (
	"context"
	"log/slog"
	"sync"

	"github.com/lyonbrown4d/nespa/controlapi"
)

func claimMigrationTaskBatch(
	ctx context.Context,
	logger *slog.Logger,
	svc *ServiceRuntime,
	cfg MigrationConfig,
) ([]controlapi.MigrationTaskBody, bool) {
	limit := migrationTaskConcurrency(cfg)
	tasks := make([]controlapi.MigrationTaskBody, 0, limit)
	for len(tasks) < limit {
		result, err := svc.claimMigrationTask(ctx)
		if err != nil {
			logger.Warn("control migration task claim failed", "error", err)
			return tasks, false
		}
		if !result.Claimed {
			return tasks, true
		}
		tasks = append(tasks, result.Task)
	}
	return tasks, true
}

func executeClaimedMigrationTaskBatch(
	ctx context.Context,
	logger *slog.Logger,
	svc *ServiceRuntime,
	cfg MigrationConfig,
	client migrationRangeClient,
	tasks []controlapi.MigrationTaskBody,
) {
	waves := migrationTaskWaves(tasks)
	for index := range waves {
		executeMigrationTaskWave(ctx, logger, svc, cfg, client, waves[index])
	}
}

func executeMigrationTaskWave(
	ctx context.Context,
	logger *slog.Logger,
	svc *ServiceRuntime,
	cfg MigrationConfig,
	client migrationRangeClient,
	tasks []controlapi.MigrationTaskBody,
) {
	var wg sync.WaitGroup
	wg.Add(len(tasks))
	for index := range tasks {
		task := tasks[index]
		go func() {
			defer wg.Done()
			executeClaimedMigrationTask(ctx, logger, svc, cfg, client, task)
		}()
	}
	wg.Wait()
}

func migrationTaskWaves(tasks []controlapi.MigrationTaskBody) [][]controlapi.MigrationTaskBody {
	waves := make([][]controlapi.MigrationTaskBody, 0, len(tasks))
	usedNodes := make([]map[string]struct{}, 0, len(tasks))
	for index := range tasks {
		waveIndex := migrationTaskWaveIndex(usedNodes, tasks[index])
		if waveIndex == len(waves) {
			waves = append(waves, nil)
			usedNodes = append(usedNodes, make(map[string]struct{}))
		}
		waves[waveIndex] = append(waves[waveIndex], tasks[index])
		recordMigrationTaskNodes(usedNodes[waveIndex], tasks[index])
	}
	return waves
}

func migrationTaskWaveIndex(usedNodes []map[string]struct{}, task controlapi.MigrationTaskBody) int {
	for index := range usedNodes {
		if !migrationTaskNodeConflict(usedNodes[index], task) {
			return index
		}
	}
	return len(usedNodes)
}

func migrationTaskNodeConflict(used map[string]struct{}, task controlapi.MigrationTaskBody) bool {
	for _, node := range migrationTaskNodeKeys(task) {
		if _, exists := used[node]; exists {
			return true
		}
	}
	return false
}

func recordMigrationTaskNodes(used map[string]struct{}, task controlapi.MigrationTaskBody) {
	for _, node := range migrationTaskNodeKeys(task) {
		used[node] = struct{}{}
	}
}

func migrationTaskNodeKeys(task controlapi.MigrationTaskBody) []string {
	keys := make([]string, 0, 2)
	keys = appendMigrationTaskNodeKey(keys, task.SourceNodeID, task.SourceAddr, "source")
	keys = appendMigrationTaskNodeKey(keys, task.TargetNodeID, task.TargetAddr, "target")
	return keys
}

func appendMigrationTaskNodeKey(keys []string, nodeID, addr, role string) []string {
	switch {
	case nodeID != "":
		return append(keys, "node:"+nodeID)
	case addr != "":
		return append(keys, "addr:"+addr)
	default:
		return append(keys, "unknown:"+role)
	}
}

func migrationTaskConcurrency(cfg MigrationConfig) int {
	if cfg.MaxParallelTasks <= 0 {
		return defaultMigrationMaxParallel
	}
	return cfg.MaxParallelTasks
}
