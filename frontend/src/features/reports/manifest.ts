import type { FeatureModule } from '@/app/feature-contract'
import ReportsPage from '@/features/reports/pages/ReportsPage.vue'

const reportsModule: FeatureModule = {
  id: 'reports',
  routes: [
    { path: '/reports', name: 'reports', component: ReportsPage, meta: { title: 'Reports' } },
  ],
  navigation: [],
}

export default reportsModule
