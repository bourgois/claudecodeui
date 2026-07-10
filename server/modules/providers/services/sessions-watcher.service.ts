import os from 'node:os';
import path from 'node:path';
import { promises as fsPromises } from 'node:fs';

import chokidar, { type FSWatcher } from 'chokidar';

import { sessionSynchronizerService } from '@/modules/providers/services/session-synchronizer.service.js';
import { sessionsService } from '@/modules/providers/services/sessions.service.js';
import { WS_OPEN_STATE, connectedClients } from '@/modules/websocket/index.js';
import type { LLMProvider } from '@/shared/types.js';
import { getProjectsWithSessions } from '@/modules/projects/index.js';

type WatcherEventType = 'add' | 'change';

const PROVIDER_WATCH_PATHS: Array<{ provider: LLMProvider; rootPath: string }> = [
  {
    provider: 'claude',
    rootPath: path.join(os.homedir(), '.claude', 'projects'),
  },
  {
    provider: 'cursor',
    rootPath: path.join(os.homedir(), '.cursor', 'projects'),
  },
  {
    provider: 'codex',
    rootPath: path.join(os.homedir(), '.codex', 'sessions'),
  },
  // {
  //   provider: 'gemini',
  //   rootPath: path.join(os.homedir(), '.gemini', 'sessions'),
  // },
  // Keep `sessions/` watcher disabled: Gemini also mirrors artifacts there,
  // which causes duplicate synchronization events.
  {
    provider: 'gemini',
    rootPath: path.join(os.homedir(), '.gemini', 'tmp'),
  },
  {
    provider: 'opencode',
    rootPath: path.join(os.homedir(), '.local', 'share', 'opencode'),
  },
];

// chokidar v4 REMOVED glob support in `ignored` — string globs like
// '**/tool-outputs/**' silently match nothing, so the ignore list must be a
// predicate (or RegExp). Returning true ignores the path; for a directory that
// also prunes the whole subtree so chokidar never descends into it — critical
// on macOS, where every watched file holds a kqueue FD.
const IGNORED_DIR_SEGMENTS = new Set([
  'node_modules',
  '.git',
  'dist',
  'build',
  // Gemini writes transient per-call tool outputs under tool-outputs/*.txt, and
  // Claude writes subagent/workflow artifacts under <session>/subagents/**.
  // Neither are session transcripts (the event filter already drops non-json),
  // but chokidar still *watches* each one — on macOS that's one FD per file, so
  // thousands of them leak FDs (observed ~14k) until `spawn`/`pty` calls fail
  // with EBADF, which silently breaks the terminal for every session. Never
  // descend into these directories.
  'tool-outputs',
  'subagents',
]);

const IGNORED_SUFFIXES = ['.tmp', '.swp', '.DS_Store'];

function isIgnoredWatchPath(targetPath: string): boolean {
  const segments = targetPath.split(path.sep);
  for (const segment of segments) {
    if (IGNORED_DIR_SEGMENTS.has(segment)) {
      return true;
    }
  }
  return IGNORED_SUFFIXES.some((suffix) => targetPath.endsWith(suffix));
}

const PROJECTS_UPDATE_DEBOUNCE_MS = 500;
const PROJECTS_UPDATE_MAX_WAIT_MS = 2_000;

const watchers: FSWatcher[] = [];

type PendingWatcherUpdate = {
  providers: Set<LLMProvider>;
  changeTypes: Set<WatcherEventType>;
  updatedSessionIds: Set<string>;
};

let pendingWatcherUpdate: PendingWatcherUpdate | null = null;
let pendingWatcherUpdateStartedAt: number | null = null;
let pendingWatcherFlushTimer: ReturnType<typeof setTimeout> | null = null;
let watcherRefreshInFlight = false;
let watcherRescheduleAfterRefresh = false;

/**
 * Filters watcher events to provider-specific session artifact file types.
 */
function isWatcherTargetFile(provider: LLMProvider, filePath: string): boolean {
  if (provider === 'opencode') {
    return path.basename(filePath) === 'opencode.db';
  }

  if (provider === 'gemini') {
    return filePath.endsWith('.json') || filePath.endsWith('.jsonl');
  }

  return filePath.endsWith('.jsonl');
}

function clearPendingWatcherFlushTimer(): void {
  if (pendingWatcherFlushTimer) {
    clearTimeout(pendingWatcherFlushTimer);
    pendingWatcherFlushTimer = null;
  }
}

function schedulePendingWatcherFlush(): void {
  if (!pendingWatcherUpdate) {
    return;
  }

  const now = Date.now();
  if (pendingWatcherUpdateStartedAt === null) {
    pendingWatcherUpdateStartedAt = now;
  }

  const elapsed = now - pendingWatcherUpdateStartedAt;
  const remainingMaxWait = Math.max(0, PROJECTS_UPDATE_MAX_WAIT_MS - elapsed);
  const delay = Math.min(PROJECTS_UPDATE_DEBOUNCE_MS, remainingMaxWait);

  clearPendingWatcherFlushTimer();
  pendingWatcherFlushTimer = setTimeout(() => {
    void flushPendingWatcherUpdate();
  }, delay);
}

function queuePendingWatcherUpdate(
  eventType: WatcherEventType,
  provider: LLMProvider,
  updatedSessionId: string | null
): void {
  if (!pendingWatcherUpdate) {
    pendingWatcherUpdate = {
      providers: new Set<LLMProvider>(),
      changeTypes: new Set<WatcherEventType>(),
      updatedSessionIds: new Set<string>(),
    };
  }

  pendingWatcherUpdate.providers.add(provider);
  pendingWatcherUpdate.changeTypes.add(eventType);
  if (updatedSessionId) {
    pendingWatcherUpdate.updatedSessionIds.add(updatedSessionId);
  }

  schedulePendingWatcherFlush();
}

async function flushPendingWatcherUpdate(): Promise<void> {
  clearPendingWatcherFlushTimer();

  if (!pendingWatcherUpdate) {
    return;
  }

  if (watcherRefreshInFlight) {
    watcherRescheduleAfterRefresh = true;
    return;
  }

  const queuedUpdate = pendingWatcherUpdate;
  pendingWatcherUpdate = null;
  pendingWatcherUpdateStartedAt = null;
  watcherRefreshInFlight = true;

  try {
    const updatedProjects = await getProjectsWithSessions({ skipSynchronization: true });
    const changeTypes = Array.from(queuedUpdate.changeTypes);
    const watchProviders = Array.from(queuedUpdate.providers);
    const updatedSessionIds = Array.from(queuedUpdate.updatedSessionIds);

    // Backward-compatible fields stay populated with the first queued values.
    const updateMessage = JSON.stringify({
      type: 'projects_updated',
      projects: updatedProjects,
      timestamp: new Date().toISOString(),
      changeType: changeTypes[0] ?? 'change',
      updatedSessionId: updatedSessionIds[0] ?? undefined,
      watchProvider: watchProviders[0] ?? undefined,
      changeTypes,
      updatedSessionIds,
      watchProviders,
      batched: true,
    });

    connectedClients.forEach(client => {
      if (client.readyState === WS_OPEN_STATE) {
        client.send(updateMessage);
      }
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error('Session watcher refresh failed while broadcasting projects_updated', { error: message });
  } finally {
    watcherRefreshInFlight = false;

    if (pendingWatcherUpdate || watcherRescheduleAfterRefresh) {
      watcherRescheduleAfterRefresh = false;
      schedulePendingWatcherFlush();
    }
  }
}

/**
 * Handles file watcher updates and triggers provider file-level synchronization.
 */
async function onUpdate(
  eventType: WatcherEventType,
  filePath: string,
  provider: LLMProvider
): Promise<void> {
  if (!isWatcherTargetFile(provider, filePath)) {
    return;
  }

  try {
    const result = await sessionSynchronizerService.synchronizeProviderFile(provider, filePath);
    if (!result.indexed) {
      return;
    }

    console.log(`Session synchronization triggered by ${eventType} event for provider "${provider}"`, {
      filePath,
      sessionId: result.sessionId,
    });
    queuePendingWatcherUpdate(eventType, provider, result.sessionId);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`Session watcher sync failed for provider "${provider}"`, {
      eventType,
      filePath,
      error: message,
    });
  }
}

/**
 * Starts provider filesystem watchers and performs initial DB synchronization.
 */
export async function initializeSessionsWatcher(): Promise<void> {
  console.log('Setting up session watchers');

  const initialSync = await sessionSynchronizerService.synchronizeSessions();
  console.log('Initial session synchronization complete', {
    processedByProvider: initialSync.processedByProvider,
    failures: initialSync.failures,
  });

  try {
    await sessionsService.syncSessionNames();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error('Failed to sync session names to history', { error: message });
  }

  for (const { provider, rootPath } of PROVIDER_WATCH_PATHS) {
    try {
      await fsPromises.mkdir(rootPath, { recursive: true });

      // Use native FS events (FSEvents/inotify) by default. Polling stat()s
      // every watched file each interval; with thousands of session files
      // (large multi-project setups) that pegs a CPU core continuously even
      // when idle. Opt back into polling only where native events don't work
      // (some network shares / container bind mounts) via CLOUDCLI_WATCH_POLLING.
      const usePolling = process.env.CLOUDCLI_WATCH_POLLING === 'true';
      const watcher = chokidar.watch(rootPath, {
        ignored: isIgnoredWatchPath,
        persistent: true,
        ignoreInitial: true,
        followSymlinks: false,
        depth: 6,
        usePolling,
        interval: 6_000,
        binaryInterval: 6_000,
      });

      watcher
        .on('add', (filePath: string) => {
          void onUpdate('add', filePath, provider);
        })
        .on('change', (filePath: string) => {
          void onUpdate('change', filePath, provider);
        })
        .on('error', (error: unknown) => {
          const message = error instanceof Error ? error.message : String(error);
          console.error(`Session watcher error for provider "${provider}"`, { error: message });
        });

      watchers.push(watcher);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.error(`Failed to initialize session watcher for provider "${provider}"`, {
        rootPath,
        error: message,
      });
    }
  }
}

/**
 * Stops all active provider session watchers.
 */
export async function closeSessionsWatcher(): Promise<void> {
  clearPendingWatcherFlushTimer();

  await Promise.all(
    watchers.map(async (watcher) => {
      try {
        await watcher.close();
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        console.error('Failed to close session watcher', { error: message });
      }
    })
  );
  watchers.length = 0;
  pendingWatcherUpdate = null;
  pendingWatcherUpdateStartedAt = null;
  watcherRefreshInFlight = false;
  watcherRescheduleAfterRefresh = false;
}
