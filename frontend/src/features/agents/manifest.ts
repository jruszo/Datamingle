import { ServerCog } from 'lucide-vue-next'

import type { FeatureModule } from '@/app/feature-contract'
import AgentsListPage from '@/features/agents/pages/AgentsListPage.vue'

const agentsModule: FeatureModule = {
  id: 'agents',
  routes: [
    { path: '/agents', name: 'agents', component: AgentsListPage, meta: { title: 'Agents', access: { anyPermissions: ['api_agents.menu_agent'] } } },
  ],
  navigation: [
    {
      to: '/agents',
      label: 'Agents',
      section: 'primary',
      icon: ServerCog,
      order: 22,
      access: { anyPermissions: ['api_agents.menu_agent'] },
    },
  ],
}

export default agentsModule
