import { ShieldCheck } from 'lucide-vue-next'

import type { FeatureModule } from '@/app/feature-contract'
import PermissionManagementPage from '@/features/permissions/pages/PermissionManagementPage.vue'

const permissionsModule: FeatureModule = {
  id: 'permissions',
  routes: [
    { path: '/permission-management', name: 'permission-management', component: PermissionManagementPage, meta: { title: 'Permission Management', access: { anyPermissions: ['sql.menu_queryapplylist'] } } },
  ],
  navigation: [
    {
      to: '/permission-management',
      label: 'Permission Management',
      section: 'primary',
      icon: ShieldCheck,
      order: 60,
      access: { anyPermissions: ['sql.menu_queryapplylist'] },
    },
  ],
}

export default permissionsModule
