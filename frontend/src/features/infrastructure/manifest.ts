import { Network } from 'lucide-vue-next'

import type { FeatureModule } from '@/app/feature-contract'
import InfrastructurePage from '@/features/infrastructure/pages/InfrastructurePage.vue'

const infrastructureModule: FeatureModule = {
  id: 'infrastructure',
  routes: [
    {
      path: '/infrastructure',
      name: 'infrastructure',
      component: InfrastructurePage,
      meta: {
        title: 'Infrastructure',
        access: {
          anyPermissions: [
            'sql.menu_instance',
            'sql.menu_instance_list',
            'sql.menu_database',
            'api_agents.menu_agent',
          ],
        },
      },
    },
  ],
  navigation: [
    {
      to: '/infrastructure',
      label: 'Infrastructure',
      section: 'primary',
      icon: Network,
      order: 20,
      access: {
        anyPermissions: [
          'sql.menu_instance',
          'sql.menu_instance_list',
          'sql.menu_database',
          'api_agents.menu_agent',
        ],
      },
    },
  ],
}

export default infrastructureModule
