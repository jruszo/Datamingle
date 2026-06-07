import { House } from 'lucide-vue-next'

import type { FeatureModule } from '@/app/feature-contract'
import HomePage from '@/features/dashboard/pages/HomePage.vue'

const dashboardModule: FeatureModule = {
  id: 'dashboard',
  routes: [
    { path: '/', name: 'home', component: HomePage, meta: { title: 'Home' } },
  ],
  navigation: [
    {
      to: '/',
      label: 'Home',
      section: 'primary',
      icon: House,
      order: 10,
      matchPrefix: '/',
    },
  ],
}

export default dashboardModule
