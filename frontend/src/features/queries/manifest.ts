import { SearchCode } from 'lucide-vue-next'

import type { FeatureModule } from '@/app/feature-contract'
import { workNavigationGroup } from '@/app/navigation-groups'
import QueriesPage from '@/features/queries/pages/QueriesPage.vue'

const queriesModule: FeatureModule = {
  id: 'queries',
  routes: [
    { path: '/queries', name: 'queries', component: QueriesPage, meta: { title: 'Queries' } },
  ],
  navigation: [
    {
      to: '/queries',
      label: 'Query history',
      section: 'primary',
      icon: SearchCode,
      group: workNavigationGroup,
      order: 10,
    },
  ],
}

export default queriesModule
