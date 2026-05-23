import type { Component } from 'vue'
import type { RouteRecordRaw } from 'vue-router'

import type { AccessRequirement } from '@/shared/auth/access'

export type NavigationSection = 'primary' | 'settings'

export type FeatureNavigationItem = {
  to: string
  label: string
  section: NavigationSection
  icon?: Component
  order?: number
  access?: AccessRequirement
  matchPrefix?: string
  children?: FeatureNavigationItem[]
}

export type AppRouteMeta = {
  title?: string
  access?: AccessRequirement
}

export type FeatureModule = {
  id: string
  routes: RouteRecordRaw[]
  navigation?: FeatureNavigationItem[]
}

declare module 'vue-router' {
  interface RouteMeta extends AppRouteMeta {}
}
