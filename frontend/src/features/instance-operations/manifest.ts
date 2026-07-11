import { Library, SlidersHorizontal, SquareActivity, UserRound } from 'lucide-vue-next'

import type { FeatureModule } from '@/app/feature-contract'
import AccountManagementPage from '@/features/instance-operations/pages/AccountManagementPage.vue'
import ParameterSettingsPage from '@/features/instance-operations/pages/ParameterSettingsPage.vue'
import SessionDiagnosticsPage from '@/features/instance-operations/pages/SessionDiagnosticsPage.vue'

const instanceOperationsModule: FeatureModule = {
  id: 'instance-operations',
  routes: [
    {
      path: '/instance-operations/databases',
      name: 'instance-operation-databases',
      redirect: '/inventory/data-dictionary',
    },
    {
      path: '/instance-operations/accounts',
      name: 'instance-operation-accounts',
      component: AccountManagementPage,
      meta: {
        title: 'Instance Accounts',
        access: { anyPermissions: ['sql.menu_instance_account'] },
      },
    },
    {
      path: '/instance-operations/parameters',
      name: 'instance-operation-parameters',
      component: ParameterSettingsPage,
      meta: { title: 'Parameter Settings', access: { anyPermissions: ['sql.menu_param'] } },
    },
    {
      path: '/instance-operations/diagnostics',
      name: 'instance-operation-diagnostics',
      component: SessionDiagnosticsPage,
      meta: { title: 'Session Diagnostics', access: { anyPermissions: ['sql.menu_dbdiagnostic'] } },
    },
  ],
  navigation: [
    {
      to: '/instance-operations/accounts',
      label: 'Accounts',
      section: 'primary',
      icon: UserRound,
      group: { id: 'catalog', label: 'Data catalog', icon: Library, order: 25 },
      order: 40,
      access: { anyPermissions: ['sql.menu_instance_account'] },
    },
    {
      to: '/instance-operations/parameters',
      label: 'Parameters',
      section: 'primary',
      icon: SlidersHorizontal,
      group: { id: 'catalog', label: 'Data catalog', icon: Library, order: 25 },
      order: 50,
      access: { anyPermissions: ['sql.menu_param'] },
    },
    {
      to: '/instance-operations/diagnostics',
      label: 'Diagnostics',
      section: 'primary',
      icon: SquareActivity,
      group: { id: 'catalog', label: 'Data catalog', icon: Library, order: 25 },
      order: 60,
      access: { anyPermissions: ['sql.menu_dbdiagnostic'] },
    },
  ],
}

export default instanceOperationsModule
