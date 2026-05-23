import type { FeatureModule } from '@/app/feature-contract'
import AgentsListPage from '@/features/agents/pages/AgentsListPage.vue'

const agentsModule: FeatureModule = {
  id: 'agents',
  routes: [
    { path: '/agents', name: 'agents', component: AgentsListPage, meta: { title: 'Agents', access: { anyPermissions: ['api_agents.menu_agent'] } } },
  ],
  navigation: [],
}

export default agentsModule
