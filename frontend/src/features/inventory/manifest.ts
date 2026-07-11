import { BookOpen, Server } from 'lucide-vue-next'

import type { FeatureModule } from '@/app/feature-contract'
import { catalogNavigationGroup } from '@/app/navigation-groups'
import DataDictionaryPage from '@/features/inventory/pages/DataDictionaryPage.vue'
import InventoryEditorPage from '@/features/inventory/pages/InventoryEditorPage.vue'
import InventoryListPage from '@/features/inventory/pages/InventoryListPage.vue'

const inventoryModule: FeatureModule = {
  id: 'inventory',
  routes: [
    {
      path: '/inventory',
      name: 'inventory',
      component: InventoryListPage,
      meta: { title: 'Inventory' },
    },
    {
      path: '/inventory/new',
      name: 'inventory-new',
      component: InventoryEditorPage,
      meta: { title: 'Add Instance' },
    },
    {
      path: '/inventory/data-dictionary',
      name: 'inventory-data-dictionary',
      component: DataDictionaryPage,
      meta: {
        title: 'Data Dictionary',
        access: { anyPermissions: ['sql.menu_data_dictionary', 'sql.menu_database'] },
      },
    },
    {
      path: '/inventory/:instanceId',
      name: 'inventory-detail',
      component: InventoryEditorPage,
      meta: { title: 'Edit Instance' },
    },
  ],
  navigation: [
    {
      to: '/inventory',
      label: 'Instances',
      section: 'primary',
      icon: Server,
      group: catalogNavigationGroup,
      order: 10,
      exactMatch: true,
      access: { anyPermissions: ['sql.menu_instance'] },
    },
    {
      to: '/inventory/data-dictionary',
      label: 'Data Dictionary',
      section: 'primary',
      icon: BookOpen,
      group: catalogNavigationGroup,
      order: 20,
      access: { anyPermissions: ['sql.menu_data_dictionary', 'sql.menu_database'] },
    },
  ],
}

export default inventoryModule
