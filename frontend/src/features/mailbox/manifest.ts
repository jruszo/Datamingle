import type { FeatureModule } from '@/app/feature-contract'
import MailboxPage from '@/features/mailbox/pages/MailboxPage.vue'

const mailboxModule: FeatureModule = {
  id: 'mailbox',
  routes: [
    { path: '/mailbox', name: 'mailbox', component: MailboxPage, meta: { title: 'Mailbox' } },
  ],
}

export default mailboxModule
