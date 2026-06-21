import { User } from 'lucide-vue-next'

import type { FeatureModule } from '@/app/feature-contract'
import LoginPage from '@/features/auth/pages/LoginPage.vue'
import ProfilePage from '@/features/auth/pages/ProfilePage.vue'

const authModule: FeatureModule = {
  id: 'auth',
  routes: [
    { path: '/login', name: 'login', component: LoginPage, meta: { title: 'Login', access: { public: true } } },
    { path: '/profile', name: 'profile', component: ProfilePage, meta: { title: 'Profile' } },
  ],
  navigation: [
    {
      to: '/profile',
      label: 'Profile',
      section: 'primary',
      icon: User,
      order: 80,
    },
  ],
}

export default authModule
