import { ListTodo, SearchCode } from 'lucide-vue-next'

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
      label: 'Query history',
      section: 'primary',
      icon: SearchCode,
      group: { id: 'work', label: 'Work', icon: ListTodo, order: 30 },
      order: 10,
    },
  ],
}

export default queriesModule
