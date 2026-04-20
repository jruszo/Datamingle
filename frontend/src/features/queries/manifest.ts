import { Database } from 'lucide-vue-next'

import type { FeatureModule } from '@/app/feature-contract'
import QueriesPage from '@/features/queries/pages/QueriesPage.vue'

const queriesModule: FeatureModule = {
  id: 'queries',
  routes: [
    { path: '/queries', name: 'queries', component: QueriesPage, meta: { title: 'Queries' } },
  ],
  navigation: [
    {
      to: '/queries',
      label: 'Queries',
      section: 'primary',
      icon: Database,
      order: 50,
    },
  ],
}

export default queriesModule
