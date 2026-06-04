import { Activity, Network } from 'lucide-vue-next'

import type { FeatureModule } from '@/app/feature-contract'
import MetricsExplorerPage from '@/features/metrics/pages/MetricsExplorerPage.vue'

const metricsModule: FeatureModule = {
  id: 'metrics',
  routes: [
    {
      path: '/metrics',
      name: 'metrics',
      component: MetricsExplorerPage,
      meta: {
        title: 'Metrics',
        access: {
          anyPermissions: ['sql.menu_infrastructure', 'sql.menu_instance', 'api_agents.menu_agent'],
        },
      },
    },
  ],
  navigation: [
    {
      to: '/metrics',
      label: 'Metrics',
      section: 'primary',
      icon: Activity,
      group: { id: 'infrastructure', label: 'Infrastructure', icon: Network, order: 20 },
      order: 20,
      access: {
        anyPermissions: ['sql.menu_infrastructure', 'sql.menu_instance', 'api_agents.menu_agent'],
      },
    },
  ],
}

export default metricsModule
