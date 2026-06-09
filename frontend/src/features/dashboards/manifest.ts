import { LayoutDashboard } from 'lucide-vue-next'

import type { FeatureModule } from '@/app/feature-contract'
import DashboardDetailPage from '@/features/dashboards/pages/DashboardDetailPage.vue'
import DashboardsPage from '@/features/dashboards/pages/DashboardsPage.vue'

const dashboardsModule: FeatureModule = {
  id: 'dashboards',
  routes: [
    {
      path: '/dashboards',
      name: 'dashboards',
      component: DashboardsPage,
      meta: { title: 'Dashboards' },
    },
    {
      path: '/dashboards/:dashboardId',
      name: 'dashboard-detail',
      component: DashboardDetailPage,
      meta: { title: 'Dashboard' },
    },
  ],
  navigation: [
    {
      to: '/dashboards',
      label: 'Dashboards',
      section: 'primary',
      icon: LayoutDashboard,
      order: 11,
    },
  ],
}

export default dashboardsModule
