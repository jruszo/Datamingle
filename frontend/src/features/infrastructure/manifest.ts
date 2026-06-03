import { Network, Server } from 'lucide-vue-next'

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
        access: { anyPermissions: ['sql.menu_infrastructure', 'sql.menu_instance'] },
      },
    },
  ],
  navigation: [
    {
      to: '/infrastructure',
      label: 'Nodes',
      section: 'primary',
      icon: Server,
      group: { id: 'infrastructure', label: 'Infrastructure', icon: Network, order: 20 },
      order: 10,
      access: { anyPermissions: ['sql.menu_infrastructure', 'sql.menu_instance'] },
    },
  ],
}

export default infrastructureModule
