import { Wrench } from 'lucide-vue-next'

import type { FeatureModule } from '@/app/feature-contract'
import AccountManagementPage from '@/features/instance-operations/pages/AccountManagementPage.vue'
import DatabaseManagementPage from '@/features/instance-operations/pages/DatabaseManagementPage.vue'
import ParameterSettingsPage from '@/features/instance-operations/pages/ParameterSettingsPage.vue'
import SessionDiagnosticsPage from '@/features/instance-operations/pages/SessionDiagnosticsPage.vue'

const instanceOperationsModule: FeatureModule = {
  id: 'instance-operations',
  routes: [
    { path: '/instance-operations/databases', name: 'instance-operation-databases', component: DatabaseManagementPage, meta: { title: 'Database Management', access: { anyPermissions: ['sql.menu_database'] } } },
    { path: '/instance-operations/accounts', name: 'instance-operation-accounts', component: AccountManagementPage, meta: { title: 'Instance Accounts', access: { anyPermissions: ['sql.menu_instance_account'] } } },
    { path: '/instance-operations/parameters', name: 'instance-operation-parameters', component: ParameterSettingsPage, meta: { title: 'Parameter Settings', access: { anyPermissions: ['sql.menu_param'] } } },
    { path: '/instance-operations/diagnostics', name: 'instance-operation-diagnostics', component: SessionDiagnosticsPage, meta: { title: 'Session Diagnostics', access: { anyPermissions: ['sql.menu_dbdiagnostic'] } } },
  ],
  navigation: [
    {
      to: '/instance-operations/databases',
      label: 'Instance Databases',
      section: 'primary',
      icon: Wrench,
      order: 22,
      access: { anyPermissions: ['sql.menu_database'] },
    },
    {
      to: '/instance-operations/accounts',
      label: 'Instance Accounts',
      section: 'primary',
      icon: Wrench,
      order: 23,
      access: { anyPermissions: ['sql.menu_instance_account'] },
    },
    {
      to: '/instance-operations/parameters',
      label: 'Parameters',
      section: 'primary',
      icon: Wrench,
      order: 24,
      access: { anyPermissions: ['sql.menu_param'] },
    },
    {
      to: '/instance-operations/diagnostics',
      label: 'Diagnostics',
      section: 'primary',
      icon: Wrench,
      order: 25,
      access: { anyPermissions: ['sql.menu_dbdiagnostic'] },
    },
  ],
}

export default instanceOperationsModule
