export interface WorkerCapabilityFields {
  registered_tasks?: string[];
}

/** Return a stable capability inventory across rolling engine upgrades. */
export function registeredTasks(worker: WorkerCapabilityFields): string[] {
  return [...(worker.registered_tasks ?? [])].sort((a, b) => a.localeCompare(b));
}
