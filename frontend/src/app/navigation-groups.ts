import { Library, ListTodo } from 'lucide-vue-next'

import type { FeatureNavigationGroup } from '@/app/feature-contract'

export const catalogNavigationGroup: FeatureNavigationGroup = {
  id: 'catalog',
  label: 'Data catalog',
  icon: Library,
  order: 25,
}

export const workNavigationGroup: FeatureNavigationGroup = {
  id: 'work',
  label: 'Work',
  icon: ListTodo,
  order: 30,
}
