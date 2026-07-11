import type { FeatureModule } from '@/app/feature-contract'
import LoginPage from '@/features/auth/pages/LoginPage.vue'
import ProfilePage from '@/features/auth/pages/ProfilePage.vue'

const authModule: FeatureModule = {
  id: 'auth',
  routes: [
    {
      path: '/login',
      name: 'login',
      component: LoginPage,
      meta: { title: 'Login', access: { public: true } },
    },
    { path: '/profile', name: 'profile', component: ProfilePage, meta: { title: 'Profile' } },
  ],
}

export default authModule
