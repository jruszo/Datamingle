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
      group: { id: 'database', label: 'Database', icon: Database, order: 25 },
      order: 70,
    },
  ],
}

export default queriesModule
