import type { FeatureModule } from '@/app/feature-contract'
import SettingsInstanceTagDetailPage from '@/features/settings/pages/SettingsInstanceTagDetailPage.vue'
import SettingsInstanceTagsPage from '@/features/settings/pages/SettingsInstanceTagsPage.vue'
import SettingsLandingPage from '@/features/settings/pages/SettingsLandingPage.vue'
import SettingsResourceGroupDetailPage from '@/features/settings/pages/SettingsResourceGroupDetailPage.vue'
import SettingsResourceGroupsPage from '@/features/settings/pages/SettingsResourceGroupsPage.vue'
import SettingsSystemPage from '@/features/settings/pages/SettingsSystemPage.vue'
import SettingsUserDetailPage from '@/features/settings/pages/SettingsUserDetailPage.vue'
import SettingsUsersPage from '@/features/settings/pages/SettingsUsersPage.vue'

const settingsModule: FeatureModule = {
  id: 'settings',
  routes: [
    { path: '/settings', name: 'settings', component: SettingsLandingPage, meta: { title: 'Settings' } },
    { path: '/settings/system', name: 'settings-system', component: SettingsSystemPage, meta: { title: 'System Settings', access: { requiresStaffAdmin: true } } },
    { path: '/settings/instance-tags', name: 'settings-instance-tags', component: SettingsInstanceTagsPage, meta: { title: 'Instance Tags', access: { anyPermissions: ['sql.menu_instance'] } } },
    { path: '/settings/instance-tags/new', name: 'settings-instance-tags-new', component: SettingsInstanceTagDetailPage, meta: { title: 'Instance Tags', access: { anyPermissions: ['sql.menu_instance'] } } },
    { path: '/settings/instance-tags/:tagId', name: 'settings-instance-tags-detail', component: SettingsInstanceTagDetailPage, meta: { title: 'Instance Tags', access: { anyPermissions: ['sql.menu_instance'] } } },
    { path: '/settings/users', name: 'settings-users', component: SettingsUsersPage, meta: { title: 'User Management', access: { requiresSuperuser: true } } },
    { path: '/settings/users/:userId', name: 'settings-users-detail', component: SettingsUserDetailPage, meta: { title: 'User Management', access: { requiresSuperuser: true } } },
    { path: '/settings/resource-groups', name: 'settings-resource-groups', component: SettingsResourceGroupsPage, meta: { title: 'Resource Groups', access: { anyPermissions: ['sql.menu_system', 'sql.view_resourcegroup', 'sql.resource_group_owner'] } } },
    { path: '/settings/resource-groups/new', name: 'settings-resource-groups-new', component: SettingsResourceGroupDetailPage, meta: { title: 'Resource Groups', access: { anyPermissions: ['sql.menu_system', 'sql.add_resourcegroup'] } } },
    { path: '/settings/resource-groups/:groupId', name: 'settings-resource-groups-detail', component: SettingsResourceGroupDetailPage, meta: { title: 'Resource Groups', access: { anyPermissions: ['sql.menu_system', 'sql.view_resourcegroup', 'sql.resource_group_owner'] } } },
    { path: '/groups/management', redirect: { name: 'settings-resource-groups' } },
    { path: '/groups/management/new', redirect: { name: 'settings-resource-groups-new' } },
    { path: '/groups/management/:groupId', redirect: (to) => ({ name: 'settings-resource-groups-detail', params: { groupId: to.params.groupId } }) },
  ],
  navigation: [
    {
      to: '/settings/system',
      label: 'System Settings',
      section: 'settings',
      order: 10,
      access: { requiresStaffAdmin: true },
    },
    {
      to: '/settings/instance-tags',
      label: 'Instance Tags',
      section: 'settings',
      order: 20,
      access: { anyPermissions: ['sql.menu_instance'] },
    },
    {
      to: '/settings/users',
      label: 'User Management',
      section: 'settings',
      order: 30,
      access: { requiresSuperuser: true },
    },
    {
      to: '/settings/resource-groups',
      label: 'Resource Groups',
      section: 'settings',
      order: 50,
      access: { anyPermissions: ['sql.menu_system', 'sql.view_resourcegroup', 'sql.resource_group_owner'] },
    },
  ],
}

export default settingsModule
