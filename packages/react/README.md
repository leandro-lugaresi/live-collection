# `@leandro-lugaresi/live-collection-react`

Optional React bindings for [`@leandro-lugaresi/live-collection`](https://github.com/leandro-lugaresi/live-collection/tree/main/packages/live-collection) — an Effect + TanStack DB live-sync engine for the frontend.

The core is already React-friendly: `defineCollection(...)` returns a **native** TanStack collection, so reads use `@tanstack/react-db`'s `useLiveQuery` directly — import it from there, this package doesn't wrap or re-export it. The only genuinely React-specific piece is lifecycle: `useLiveSync` forks broker ingest on mount and interrupts it on unmount.

```bash
npm install @leandro-lugaresi/live-collection-react @leandro-lugaresi/live-collection @tanstack/react-db effect
```

## Usage

Mount `useLiveSync` **once** near the app root; collections subscribe themselves when mounted:

```tsx
import { useLiveSync } from "@leandro-lugaresi/live-collection-react"
import { runtime } from "./collections"

export function App() {
  useLiveSync(runtime)
  return <Routes />
}
```

Unmounting stops the live connection but does **not** dispose collections — registry lifetime is the app's, so a remount reuses the warm local store.

## Documentation

- [React integration](https://github.com/leandro-lugaresi/live-collection/blob/main/docs/react.md) — `useLiveSync` and reading collections with `useLiveQuery`.
- [Repository](https://github.com/leandro-lugaresi/live-collection) — quick start and full docs.

## License

MIT
