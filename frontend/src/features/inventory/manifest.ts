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
  navigation: [],
}

export default inventoryModule
