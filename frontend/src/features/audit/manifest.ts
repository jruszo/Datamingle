import { ClipboardList } from 'lucide-vue-next'

import type { FeatureModule } from '@/app/feature-contract'
import AuditPage from '@/features/audit/pages/AuditPage.vue'

const auditModule: FeatureModule = {
  id: 'audit',
  routes: [
    { path: '/audit', name: 'audit', component: AuditPage, meta: { title: 'Audit', access: { anyPermissions: ['sql.audit_user'] } } },
  ],
  navigation: [
    {
      to: '/audit',
      label: 'Audit',
      section: 'primary',
      icon: ClipboardList,
      order: 75,
      access: { anyPermissions: ['sql.audit_user'] },
    },
  ],
}

export default auditModule
