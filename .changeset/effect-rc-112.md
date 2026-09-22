---
"@leandro-lugaresi/live-collection-protocol": patch
"@leandro-lugaresi/live-collection": patch
"@leandro-lugaresi/live-collection-server": patch
"@leandro-lugaresi/live-collection-react": patch
---

Upgrade Effect, @effect/platform-node, and @effect/vitest to 4.0.0-rc.112.
Pin the prerelease peers and Node shared runtime to the same version so
installation cannot select an incompatible newer release candidate. Consumers
must use Effect 4.0.0-rc.112; Node applications should also pin
@effect/platform-node-shared to 4.0.0-rc.112 during this compatibility window.
