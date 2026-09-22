---
"@leandro-lugaresi/live-collection-protocol": major
"@leandro-lugaresi/live-collection": major
"@leandro-lugaresi/live-collection-react": major
"@leandro-lugaresi/live-collection-server": major
---

Fork migration: rename all four published packages from the `@triargos/*`
scope to the `@leandro-lugaresi/*` scope for GitHub Packages publishing.

Consumers must update their import specifiers and dependencies to the new
scope. No wire-protocol or runtime changes.