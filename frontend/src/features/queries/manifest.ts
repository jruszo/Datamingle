import type { FeatureModule } from '@/app/feature-contract'
import QueriesPage from '@/features/queries/pages/QueriesPage.vue'

const queriesModule: FeatureModule = {
  id: 'queries',
  routes: [
    { path: '/queries', name: 'queries', component: QueriesPage, meta: { title: 'Queries' } },
  ],
  navigation: [],
}

export default queriesModule
