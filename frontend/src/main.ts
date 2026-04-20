import { createApp } from 'vue'
import { createPinia } from 'pinia'

import { installAuthSessionHandling } from '@/app/install-auth-session'
import App from './App.vue'
import router from '@/app/router'
import './assets/main.css'

const app = createApp(App)
const pinia = createPinia()

app.use(pinia)
app.use(router)
installAuthSessionHandling(pinia, router)

app.mount('#app')
