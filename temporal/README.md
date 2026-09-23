# Temporal orchestration infrastructure

This stack is an independent durable-workflow layer. It persists Temporal
workflow history only.

It never stores or determines:

- Choreography state
- Event-log order
- Commit index
- Leader election
- Snapshots
- Node recovery

Start this stack only after the C++ management API is ready for retry-safe
Activities. Docker Desktop or another container runtime is required then.

```powershell
Copy-Item temporal\.env.example temporal\.env
docker compose --project-directory temporal up --detach --wait
```
