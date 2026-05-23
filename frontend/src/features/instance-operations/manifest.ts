import {
  Archive,
  BookOpen,
  ChartNoAxesCombined,
  Database,
  FileText,
  SlidersHorizontal,
  SquareActivity,
  UserRound,
} from 'lucide-vue-next'

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
      label: 'Database Management',
      section: 'primary',
      icon: Database,
      order: 21,
      children: [
        {
          to: '/queries',
          label: 'Queries',
          section: 'primary',
          icon: Database,
          order: 10,
        },
        {
          to: '/workflows',
          label: 'Workflows',
          section: 'primary',
          icon: FileText,
          order: 20,
        },
        {
          to: '/archives',
          label: 'Archives',
          section: 'primary',
          icon: Archive,
          order: 30,
          access: { anyPermissions: ['sql.menu_archive'] },
        },
        {
          to: '/inventory/data-dictionary',
          label: 'Data Dictionary',
          section: 'primary',
          icon: BookOpen,
          order: 40,
          access: { anyPermissions: ['sql.menu_data_dictionary'] },
        },
        {
          to: '/instance-operations/databases',
          label: 'Databases',
          section: 'primary',
          icon: Database,
          order: 50,
          access: { anyPermissions: ['sql.menu_database'] },
        },
        {
          to: '/instance-operations/accounts',
          label: 'Instance Accounts',
          section: 'primary',
          icon: UserRound,
          order: 60,
          access: { anyPermissions: ['sql.menu_instance_account'] },
        },
        {
          to: '/instance-operations/parameters',
          label: 'Parameters',
          section: 'primary',
          icon: SlidersHorizontal,
          order: 70,
          access: { anyPermissions: ['sql.menu_param'] },
        },
        {
          to: '/instance-operations/diagnostics',
          label: 'Diagnostics',
          section: 'primary',
          icon: SquareActivity,
          order: 80,
          access: { anyPermissions: ['sql.menu_dbdiagnostic'] },
        },
        {
          to: '/reports',
          label: 'Reports',
          section: 'primary',
          icon: ChartNoAxesCombined,
          order: 90,
        },
      ],
    },
  ],
}

export default instanceOperationsModule
