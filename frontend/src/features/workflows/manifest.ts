import { FileCheck2, ListChecks, ListTodo } from 'lucide-vue-next'

import type { FeatureModule } from '@/app/feature-contract'
import DdlWorkflowCreatePage from '@/features/workflows/pages/DdlWorkflowCreatePage.vue'
import DmlWorkflowCreatePage from '@/features/workflows/pages/DmlWorkflowCreatePage.vue'
import ExportWorkflowCreatePage from '@/features/workflows/pages/ExportWorkflowCreatePage.vue'
import WorkflowDetailPage from '@/features/workflows/pages/WorkflowDetailPage.vue'
import WorkflowPoliciesPage from '@/features/workflows/pages/WorkflowPoliciesPage.vue'
import WorkflowsPage from '@/features/workflows/pages/WorkflowsPage.vue'

const workflowsModule: FeatureModule = {
  id: 'workflows',
  routes: [
    {
      path: '/workflows',
      name: 'workflows',
      component: WorkflowsPage,
      meta: { title: 'Workflows' },
    },
    {
      path: '/workflows/ddl/new',
      name: 'workflow-ddl-new',
      component: DdlWorkflowCreatePage,
      meta: { title: 'New DDL Request' },
    },
    {
      path: '/workflows/dml/new',
      name: 'workflow-dml-new',
      component: DmlWorkflowCreatePage,
      meta: { title: 'New DML Request' },
    },
    {
      path: '/workflows/export/new',
      name: 'workflow-export-new',
      component: ExportWorkflowCreatePage,
      meta: { title: 'New Export Request' },
    },
    {
      path: '/workflows/policies',
      name: 'workflow-policies',
      component: WorkflowPoliciesPage,
      meta: { title: 'Workflow Policies' },
    },
    {
      path: '/workflows/:workflowId',
      name: 'workflow-detail',
      component: WorkflowDetailPage,
      meta: { title: 'Workflow Detail' },
    },
  ],
  navigation: [
    {
      to: '/workflows',
      label: 'Workflow requests',
      section: 'primary',
      icon: ListChecks,
      group: { id: 'work', label: 'Work', icon: ListTodo, order: 30 },
      order: 30,
    },
    {
      to: '/workflows/policies',
      label: 'Policies',
      section: 'primary',
      icon: FileCheck2,
      group: { id: 'work', label: 'Work', icon: ListTodo, order: 30 },
      order: 40,
    },
  ],
}

export default workflowsModule
