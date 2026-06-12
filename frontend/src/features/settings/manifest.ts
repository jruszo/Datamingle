import type { FeatureModule } from '@/app/feature-contract'
import SettingsInstanceTagDetailPage from '@/features/settings/pages/SettingsInstanceTagDetailPage.vue'
import SettingsInstanceTagsPage from '@/features/settings/pages/SettingsInstanceTagsPage.vue'
import SettingsLandingPage from '@/features/settings/pages/SettingsLandingPage.vue'
import SettingsTeamDetailPage from '@/features/settings/pages/SettingsTeamDetailPage.vue'
import SettingsTeamsPage from '@/features/settings/pages/SettingsTeamsPage.vue'
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
    { path: '/settings/teams', name: 'settings-teams', component: SettingsTeamsPage, meta: { title: 'Teams', access: { anyPermissions: ['sql.menu_system', 'sql.view_team', 'sql.team_owner'] } } },
    { path: '/settings/teams/new', name: 'settings-teams-new', component: SettingsTeamDetailPage, meta: { title: 'Teams', access: { anyPermissions: ['sql.menu_system', 'sql.add_team'] } } },
    { path: '/settings/teams/:teamId', name: 'settings-teams-detail', component: SettingsTeamDetailPage, meta: { title: 'Teams', access: { anyPermissions: ['sql.menu_system', 'sql.view_team', 'sql.team_owner'] } } },
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
      to: '/settings/teams',
      label: 'Teams',
      section: 'settings',
      order: 50,
      access: { anyPermissions: ['sql.menu_system', 'sql.view_team', 'sql.team_owner'] },
    },
  ],
}

export default settingsModule
