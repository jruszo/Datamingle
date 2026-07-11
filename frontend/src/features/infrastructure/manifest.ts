import { Library, Network, Server, Workflow } from 'lucide-vue-next'

import type { FeatureModule } from '@/app/feature-contract'
import InfrastructurePage from '@/features/infrastructure/pages/InfrastructurePage.vue'
import ClusterTopologyPage from '@/features/infrastructure/pages/ClusterTopologyPage.vue'

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
    {
      path: '/infrastructure/topology',
      name: 'cluster-topology',
      component: ClusterTopologyPage,
      meta: {
        title: 'Cluster Topology',
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
      exactMatch: true,
      access: { anyPermissions: ['sql.menu_infrastructure', 'sql.menu_instance'] },
    },
    {
      to: '/infrastructure/topology',
      label: 'Cluster topology',
      section: 'primary',
      icon: Workflow,
      group: { id: 'catalog', label: 'Data catalog', icon: Library, order: 25 },
      order: 15,
      access: { anyPermissions: ['sql.menu_infrastructure', 'sql.menu_instance'] },
    },
  ],
}

export default infrastructureModule
