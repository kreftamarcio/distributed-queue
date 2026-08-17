# Examples

These examples import the public `Queue` API from `src/index.ts`.

The root README still describes a future `DistributedQueue` with Redis/Postgres
backends. Until those backends exist, use this folder — it matches the code that
actually compiles.

```bash
npx tsx examples/01-lanes.ts
```

| File | What it proves |
|------|----------------|
| `01-lanes.ts` | Weighted lanes, windowed dedupe, claim/ack, stats |
