import { ChartNoAxesCombined, ChartSpline } from 'lucide-vue-next'

import type { FeatureModule } from '@/app/feature-contract'
import ReportsPage from '@/features/reports/pages/ReportsPage.vue'

const reportsModule: FeatureModule = {
  id: 'reports',
  routes: [
    { path: '/reports', name: 'reports', component: ReportsPage, meta: { title: 'Reports' } },
  ],
  navigation: [
    {
      to: '/reports',
      label: 'Reports',
      section: 'primary',
      icon: ChartNoAxesCombined,
      group: { id: 'insights', label: 'Insights', icon: ChartSpline, order: 40 },
      order: 10,
    },
  ],
}

export default reportsModule
