import { ShieldCheck } from 'lucide-vue-next'

import type { FeatureModule } from '@/app/feature-contract'
import { workNavigationGroup } from '@/app/navigation-groups'
import PermissionManagementPage from '@/features/permissions/pages/PermissionManagementPage.vue'

const permissionsModule: FeatureModule = {
  id: 'permissions',
  routes: [
    {
      path: '/permission-management',
      name: 'permission-management',
      component: PermissionManagementPage,
      meta: {
        title: 'Permission Management',
        access: { anyPermissions: ['sql.menu_queryapplylist'] },
      },
    },
  ],
  navigation: [
    {
      to: '/permission-management',
      label: 'Access requests',
      section: 'primary',
      icon: ShieldCheck,
      group: workNavigationGroup,
      order: 50,
      access: { anyPermissions: ['sql.menu_queryapplylist'] },
    },
  ],
}

export default permissionsModule
