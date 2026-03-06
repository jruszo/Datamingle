import { createApp } from 'vue'
import { createPinia } from 'pinia'

import { installAuthSessionHandling } from '@/lib/auth-session'
import App from './App.vue'
import router from './router'
import './assets/main.css'

const app = createApp(App)
const pinia = createPinia()

app.use(pinia)
app.use(router)
installAuthSessionHandling(pinia, router)

app.mount('#app')
