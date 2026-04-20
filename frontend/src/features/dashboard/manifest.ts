import { LayoutGrid } from 'lucide-vue-next'

import type { FeatureModule } from '@/app/feature-contract'
import HomePage from '@/features/dashboard/pages/HomePage.vue'

const dashboardModule: FeatureModule = {
  id: 'dashboard',
  routes: [
    { path: '/', name: 'home', component: HomePage, meta: { title: 'Dashboard' } },
  ],
  navigation: [
    {
      to: '/',
      label: 'Dashboard',
      section: 'primary',
      icon: LayoutGrid,
      order: 10,
      matchPrefix: '/',
    },
  ],
}

export default dashboardModule
