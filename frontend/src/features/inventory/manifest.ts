import { Server } from 'lucide-vue-next'

import type { FeatureModule } from '@/app/feature-contract'
import InventoryEditorPage from '@/features/inventory/pages/InventoryEditorPage.vue'
import InventoryListPage from '@/features/inventory/pages/InventoryListPage.vue'

const inventoryModule: FeatureModule = {
  id: 'inventory',
  routes: [
    { path: '/inventory', name: 'inventory', component: InventoryListPage, meta: { title: 'Inventory' } },
    { path: '/inventory/new', name: 'inventory-new', component: InventoryEditorPage, meta: { title: 'Add Instance' } },
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
  ],
}

export default inventoryModule
