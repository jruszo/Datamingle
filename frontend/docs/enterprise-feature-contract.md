# Enterprise Feature Module Contract

The OSS SPA composes feature modules from two sources:

- community feature manifests shipped in `frontend/src/features/*/manifest.ts`
- optional enterprise feature manifests imported from the `@enterprise-feature-modules` alias

An enterprise repo should provide a module that default-exports `FeatureModule[]`.

Each `FeatureModule` can contribute:

- `routes`: `vue-router` route records
- `navigation`: primary or settings navigation entries

The OSS repository resolves `@enterprise-feature-modules` to
`frontend/src/extensions/enterprise-feature-modules.ts`, which exports an empty array.
An enterprise repo can override that alias in Vite and TypeScript config to inject extra modules
without modifying the OSS app shell or router composition.
