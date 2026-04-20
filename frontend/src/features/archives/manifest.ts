import { Archive } from 'lucide-vue-next'

import type { FeatureModule } from '@/app/feature-contract'
import ArchiveCreatePage from '@/features/archives/pages/ArchiveCreatePage.vue'
import ArchiveDetailPage from '@/features/archives/pages/ArchiveDetailPage.vue'
import ArchivesPage from '@/features/archives/pages/ArchivesPage.vue'

const archivesModule: FeatureModule = {
  id: 'archives',
  routes: [
    { path: '/archives', name: 'archives', component: ArchivesPage, meta: { title: 'Archives', access: { anyPermissions: ['sql.menu_archive'] } } },
    { path: '/archives/new', name: 'archive-new', component: ArchiveCreatePage, meta: { title: 'New Archive', access: { anyPermissions: ['sql.menu_archive'] } } },
    { path: '/archives/:archiveId', name: 'archive-detail', component: ArchiveDetailPage, meta: { title: 'Archive Detail', access: { anyPermissions: ['sql.menu_archive'] } } },
  ],
  navigation: [
    {
      to: '/archives',
      label: 'Archives',
      section: 'primary',
      icon: Archive,
      order: 40,
      access: { anyPermissions: ['sql.menu_archive'] },
    },
  ],
}

export default archivesModule
