import { BookOpen, Server } from 'lucide-vue-next'

import type { FeatureModule } from '@/app/feature-contract'
import DataDictionaryPage from '@/features/inventory/pages/DataDictionaryPage.vue'
import InventoryEditorPage from '@/features/inventory/pages/InventoryEditorPage.vue'
import InventoryListPage from '@/features/inventory/pages/InventoryListPage.vue'

const inventoryModule: FeatureModule = {
  id: 'inventory',
  routes: [
    { path: '/inventory', name: 'inventory', component: InventoryListPage, meta: { title: 'Inventory' } },
    { path: '/inventory/new', name: 'inventory-new', component: InventoryEditorPage, meta: { title: 'Add Instance' } },
    { path: '/inventory/data-dictionary', name: 'inventory-data-dictionary', component: DataDictionaryPage, meta: { title: 'Data Dictionary', access: { anyPermissions: ['sql.menu_data_dictionary'] } } },
    { path: '/inventory/:instanceId', name: 'inventory-detail', component: InventoryEditorPage, meta: { title: 'Edit Instance' } },
  ],
  navigation: [
    {
      to: '/inventory',
      label: 'Inventory',
      section: 'primary',
      icon: Server,
      order: 20,
      access: { anyPermissions: ['sql.menu_instance'] },
    },
    {
      to: '/inventory/data-dictionary',
      label: 'Data Dictionary',
      section: 'primary',
      icon: BookOpen,
      order: 21,
      access: { anyPermissions: ['sql.menu_data_dictionary'] },
    },
  ],
}

export default inventoryModule
