<script setup lang="ts">
import { computed, onMounted, onUnmounted, reactive, ref, watch } from 'vue'
import { Check, Database, Plus, RefreshCw, Search, ServerCog, Wand2, X } from 'lucide-vue-next'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import LabelFilterBar from '@/components/LabelFilterBar.vue'
import type { LabelFilter } from '@/shared/filters/labelFilters'
import { useAuthStore } from '@/stores/auth'
import MonitoringLabelsEditor from '../components/MonitoringLabelsEditor.vue'
import {
  createAgent,
  createDatabaseService,
  discoverInfrastructureNodeServices,
  fetchInfrastructureNode,
  fetchInfrastructureNodeLabelNames,
  fetchInfrastructureNodeLabelValues,
  fetchInfrastructureNodes,
  fetchInstanceInventoryMetadata,
  fetchWorkflowPolicies,
  issueAgentInstallKey,
  testDatabaseServiceConnection,
  updateDatabaseService,
  updateInfrastructureNode,
  updateMysqlCluster,
  updateServiceRecommendationStatus,
  type AgentCreateResponse,
  type DatabaseServicePayload,
  type DatabaseServiceRecord,
  type InfrastructureNodeAgentRecord,
  type InfrastructureNodeDetailRecord,
  type InfrastructureNodePayload,
  type InfrastructureNodeRecord,
  type InstanceInventoryMetadata,
  type ServiceRecommendationRecord,
  type WorkflowPolicyRecord,
} from '../api'

const authStore = useAuthStore()

const DEFAULT_NODE_EXPORTER_COLLECTORS = [
  'arp',
  'bcache',
  'bcachefs',
  'bonding',
  'btrfs',
  'conntrack',
  'cpu',
  'cpufreq',
  'diskstats',
  'dmi',
  'edac',
  'entropy',
  'fibrechannel',
  'filefd',
  'filesystem',
  'hwmon',
  'infiniband',
  'ipvs',
  'kernel_hung',
  'loadavg',
  'mdadm',
  'meminfo',
  'netclass',
  'netdev',
  'netstat',
  'nfs',
  'nfsd',
  'nvme',
  'os',
  'powersupplyclass',
  'pressure',
  'rapl',
  'schedstat',
  'selinux',
  'sockstat',
  'softnet',
  'stat',
  'tapestats',
  'textfile',
  'thermal_zone',
  'time',
  'timex',
  'udp_queues',
  'uname',
  'vmstat',
  'watchdog',
  'xfs',
  'zfs',
]

const MYSQLD_EXPORTER_COLLECTORS = [
  'heartbeat.utc',
  'info_schema.processlist.processes_by_user',
  'info_schema.processlist.processes_by_host',
  'mysql.user.privileges',
  'perf_schema.indexiowaits',
  'perf_schema.tablelocks',
  'perf_schema.eventsstatements',
  'perf_schema.eventsstatementssum',
  'perf_schema.eventswaits',
  'heartbeat',
  'slave_hosts',
  'info_schema.replica_host',
  'info_schema.rocksdb_perf_context',
  'perf_schema.file_events',
  'perf_schema.file_instances',
  'perf_schema.memory_events',
  'perf_schema.replication_group_members',
  'perf_schema.replication_group_member_stats',
  'perf_schema.replication_applier_status_by_worker',
  'sys.user_summary',
  'info_schema.userstats',
  'info_schema.clientstats',
  'info_schema.tablestats',
  'info_schema.schemastats',
  'info_schema.innodb_cmp',
  'info_schema.innodb_cmpmem',
  'info_schema.query_response_time',
  'engine_tokudb_status',
  'engine_innodb_status',
  'global_status',
  'global_variables',
  'slave_status',
  'info_schema.processlist',
  'mysql.user',
  'info_schema.tables',
  'info_schema.innodb_tablespaces',
  'info_schema.innodb_metrics',
  'auto_increment.columns',
  'binlog_size',
  'perf_schema.tableiowaits',
]

const DEFAULT_MYSQLD_EXPORTER_COLLECTORS = ['global_status', 'global_variables', 'slave_status']

const POSTGRES_EXPORTER_COLLECTORS = [
  'buffercache_summary',
  'database',
  'database_wraparound',
  'locks',
  'long_running_transactions',
  'postmaster',
  'process_idle',
  'replication',
  'replication_slot',
  'roles',
  'stat_activity_autovacuum',
  'stat_bgwriter',
  'stat_checkpointer',
  'stat_database',
  'stat_progress_vacuum',
  'stat_statements',
  'stat_statements.include_query',
  'stat_user_tables',
  'stat_wal_receiver',
  'statio_user_indexes',
  'statio_user_tables',
  'wal',
  'xlog_location',
]

const DEFAULT_POSTGRES_EXPORTER_COLLECTORS = [
  'database',
  'locks',
  'replication',
  'replication_slot',
  'roles',
  'stat_bgwriter',
  'stat_database',
  'stat_progress_vacuum',
  'stat_user_tables',
  'statio_user_tables',
  'wal',
]

type MonitoringCollectorForm = {
  monitoring_collectors: string[]
}

const nodes = ref<InfrastructureNodeRecord[]>([])
const selectedNode = ref<InfrastructureNodeDetailRecord | null>(null)
const metadata = ref<InstanceInventoryMetadata | null>(null)
const workflowPolicies = ref<WorkflowPolicyRecord[]>([])
const isLoading = ref(false)
const detailLoading = ref(false)
const totalCount = ref(0)
const currentPage = ref(1)
const pageSize = ref(20)
const searchQuery = ref('')
const nodeLabelFilters = ref<LabelFilter[]>([])
const nodeLabelNames = ref<string[]>([])
const error = ref('')
const feedback = ref('')
const nowMs = ref(Date.now())
const testingServiceId = ref<number | null>(null)
const discoveringNodeId = ref<number | null>(null)
const isDetailDialogOpen = ref(false)
let refreshTimer: ReturnType<typeof setInterval> | null = null
let clockTimer: ReturnType<typeof setInterval> | null = null

const isNodeDialogOpen = ref(false)
const editingNodeId = ref<number | null>(null)
const nodeSaving = ref(false)
const nodeFormError = ref('')
const nodeForm = reactive<InfrastructureNodePayload>({
  name: '',
  address: '',
  description: '',
  metadata: {},
  monitoring_enabled: true,
  monitoring_collectors: [...DEFAULT_NODE_EXPORTER_COLLECTORS],
  monitoring_labels: {},
})

const isServiceDialogOpen = ref(false)
const editingServiceId = ref<number | null>(null)
const serviceSaving = ref(false)
const serviceFormError = ref('')
const serviceForm = reactive<DatabaseServicePayload>({
  node_id: 0,
  service_name: '',
  role: 'master',
  engine: 'mysql',
  host: '',
  port: 3306,
  user: '',
  password: '',
  monitoring_enabled: true,
  queryable: false,
  workflow_enabled: false,
  workflow_policy: null,
  monitoring_collectors: [...DEFAULT_MYSQLD_EXPORTER_COLLECTORS],
  monitoring_labels: {},
  is_ssl: false,
  verify_ssl: true,
  db_name: '',
  show_db_name_regex: '',
  denied_db_name_regex: '',
  charset: '',
  team_ids: [],
})
const editingService = computed(() => {
  if (!editingServiceId.value || !selectedNode.value) {
    return null
  }
  return selectedNode.value.services.find((service) => service.id === editingServiceId.value) ?? null
})
const clusterSaving = ref(false)
const clusterFormError = ref('')
const clusterForm = reactive({
  name: '',
  label_value: '',
})

const isAgentDialogOpen = ref(false)
const agentSaving = ref(false)
const agentInstallIssuing = ref(false)
const agentFormError = ref('')
const createdAgent = ref<AgentCreateResponse | null>(null)
const agentForm = reactive({
  node_name: '',
  monitoring_enabled: true,
  monitoring_collectors: [...DEFAULT_NODE_EXPORTER_COLLECTORS],
})
const agentTargetNodeId = ref<number | null>(null)

const fieldClass =
  'w-full rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm focus:border-slate-400 focus:outline-none focus:ring-1 focus:ring-slate-400'
const selectClass =
  'h-10 w-full rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-700 shadow-sm focus:border-slate-400 focus:outline-none'
const multiSelectClass =
  'min-h-[7rem] w-full rounded-md border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700 shadow-sm focus:border-slate-400 focus:outline-none'

const canAccessInfrastructure = computed(() => {
  if (authStore.currentUser?.is_superuser) {
    return true
  }
  const permissions = authStore.currentUser?.permissions ?? []
  return (
    permissions.includes('sql.menu_infrastructure') || permissions.includes('sql.menu_instance')
  )
})

const recommendedServices = computed(
  () =>
    selectedNode.value?.recommendations.filter(
      (recommendation: ServiceRecommendationRecord) => recommendation.status === 'recommended',
    ) ?? [],
)
const serviceWorkflowPolicyOptions = computed(() =>
  workflowPolicies.value.filter(
    (policy) => policy.is_active || policy.id === serviceForm.workflow_policy,
  ),
)

function requireToken() {
  if (!authStore.accessToken) {
    throw new Error('Missing access token. Please login again.')
  }
  return authStore.accessToken
}

function toUserFacingMessage(errorValue: unknown, fallback: string) {
  if (!(errorValue instanceof Error)) {
    return fallback
  }
  const separator = '): '
  const separatorIndex = errorValue.message.indexOf(separator)
  return separatorIndex === -1
    ? errorValue.message
    : errorValue.message.slice(separatorIndex + separator.length)
}

function orderedCollectors(collectors?: string[]) {
  const selected = new Set((collectors ?? DEFAULT_NODE_EXPORTER_COLLECTORS).filter(Boolean))
  return DEFAULT_NODE_EXPORTER_COLLECTORS.filter((collector) => selected.has(collector))
}

function setMonitoringCollectors(target: MonitoringCollectorForm, collectors?: string[]) {
  target.monitoring_collectors.splice(
    0,
    target.monitoring_collectors.length,
    ...orderedCollectors(collectors),
  )
}

function isMonitoringCollectorSelected(target: MonitoringCollectorForm, collector: string) {
  return target.monitoring_collectors.includes(collector)
}

function toggleMonitoringCollector(
  target: MonitoringCollectorForm,
  collector: string,
  event: Event,
) {
  const checked = (event.target as HTMLInputElement).checked
  if (checked && !target.monitoring_collectors.includes(collector)) {
    setMonitoringCollectors(target, [...target.monitoring_collectors, collector])
  }
  if (!checked) {
    setMonitoringCollectors(
      target,
      target.monitoring_collectors.filter((selected) => selected !== collector),
    )
  }
}

function selectAllMonitoringCollectors(target: MonitoringCollectorForm) {
  setMonitoringCollectors(target, DEFAULT_NODE_EXPORTER_COLLECTORS)
}

function resetDefaultMonitoringCollectors(target: MonitoringCollectorForm) {
  setMonitoringCollectors(target, DEFAULT_NODE_EXPORTER_COLLECTORS)
}

function serviceCollectorOptions(engine: DatabaseServicePayload['engine']) {
  return engine === 'pgsql' ? POSTGRES_EXPORTER_COLLECTORS : MYSQLD_EXPORTER_COLLECTORS
}

function defaultServiceCollectors(engine: DatabaseServicePayload['engine']) {
  return engine === 'pgsql'
    ? DEFAULT_POSTGRES_EXPORTER_COLLECTORS
    : DEFAULT_MYSQLD_EXPORTER_COLLECTORS
}

function orderedServiceCollectors(engine: DatabaseServicePayload['engine'], collectors?: string[]) {
  const fallback = defaultServiceCollectors(engine)
  const selected = new Set((collectors === undefined ? fallback : collectors).filter(Boolean))
  return serviceCollectorOptions(engine).filter((collector) => selected.has(collector))
}

function setServiceMonitoringCollectors(
  target: MonitoringCollectorForm & { engine: DatabaseServicePayload['engine'] },
  collectors?: string[],
) {
  target.monitoring_collectors.splice(
    0,
    target.monitoring_collectors.length,
    ...orderedServiceCollectors(target.engine, collectors),
  )
}

function selectAllServiceMonitoringCollectors(
  target: MonitoringCollectorForm & { engine: DatabaseServicePayload['engine'] },
) {
  setServiceMonitoringCollectors(target, serviceCollectorOptions(target.engine))
}

function resetDefaultServiceMonitoringCollectors(
  target: MonitoringCollectorForm & { engine: DatabaseServicePayload['engine'] },
) {
  setServiceMonitoringCollectors(target, defaultServiceCollectors(target.engine))
}

function statusBadgeClass(status: string | null) {
  switch (status) {
    case 'online':
      return 'bg-emerald-100 text-emerald-800'
    case 'offline':
    case 'pending':
      return 'bg-amber-100 text-amber-800'
    case 'disabled':
    case 'revoked':
      return 'bg-red-100 text-red-800'
    default:
      return 'bg-slate-100 text-slate-700'
  }
}

function serviceStatusClass(status: DatabaseServiceRecord['inventory_status']) {
  switch (status) {
    case 'ok':
      return 'bg-emerald-100 text-emerald-800'
    case 'failed':
      return 'bg-red-100 text-red-800'
    case 'stale':
      return 'bg-amber-100 text-amber-800'
    default:
      return 'bg-slate-100 text-slate-700'
  }
}

function inventoryStatusLabel(status: DatabaseServiceRecord['inventory_status']) {
  switch (status) {
    case 'ok':
      return 'Current'
    case 'failed':
      return 'Refresh failed'
    case 'stale':
      return 'Stale'
    default:
      return 'Not collected'
  }
}

function inventoryStatusTitle(service: DatabaseServiceRecord) {
  const lastRefresh = service.inventory_last_refresh_at
    ? ` Last successful refresh: ${formatDateTime(service.inventory_last_refresh_at)}.`
    : ''
  return `Inventory is collected by the server. Test and monitoring check connectivity from the node agent.${lastRefresh}`
}

function mysqlClusterLabel(service: DatabaseServiceRecord) {
  if (service.engine !== 'mysql') {
    return 'N/A'
  }
  if (service.mysql_cluster_name) {
    return service.mysql_cluster_name
  }
  if (service.mysql_topology_status === 'standalone') {
    return 'Standalone'
  }
  return 'Unknown'
}

function mysqlClusterRoleLabel(service: DatabaseServiceRecord) {
  const role = service.mysql_cluster_role || ''
  if (role === 'primary') {
    return 'Master'
  }
  if (role === 'replica') {
    return 'Replica'
  }
  if (role === 'standalone' && service.mysql_topology_status === 'standalone') {
    return 'Standalone'
  }
  return 'Unknown'
}

function mysqlTopologyStatusLabel(service: DatabaseServiceRecord) {
  const status = service.mysql_cluster_status || service.mysql_topology_status || ''
  if (status === 'clustered' || status === 'ok') {
    return 'Clustered'
  }
  if (status === 'standalone') {
    return 'Standalone'
  }
  if (status === 'missing_master') {
    return 'Missing master'
  }
  if (status === 'ambiguous_master') {
    return 'Ambiguous master'
  }
  if (status === 'drift') {
    return 'Drift'
  }
  if (status === 'unknown') {
    return 'Not collected'
  }
  return 'Not collected'
}

function mysqlClusterBadgeLabel(service: DatabaseServiceRecord) {
  const role = mysqlClusterRoleLabel(service)
  const status = mysqlTopologyStatusLabel(service)
  return role === status ? role : `${role} - ${status}`
}

function mysqlClusterStatusClass(service: DatabaseServiceRecord) {
  if (service.engine !== 'mysql') {
    return 'bg-slate-100 text-slate-600'
  }
  if (service.mysql_topology_status === 'drift' || service.mysql_cluster_status === 'drift') {
    return 'bg-red-100 text-red-800'
  }
  if (
    service.mysql_cluster_status === 'missing_master' ||
    service.mysql_topology_status === 'missing_master' ||
    service.mysql_cluster_status === 'ambiguous_master' ||
    service.mysql_topology_status === 'ambiguous_master'
  ) {
    return 'bg-amber-100 text-amber-800'
  }
  if (service.mysql_cluster_status === 'ok' || service.mysql_topology_status === 'clustered') {
    return 'bg-emerald-100 text-emerald-800'
  }
  if (service.mysql_topology_status === 'standalone') {
    return service.mysql_ddl_dml_eligible
      ? 'bg-emerald-100 text-emerald-800'
      : 'bg-amber-100 text-amber-800'
  }
  return 'bg-slate-100 text-slate-700'
}

function mysqlClusterTitle(service: DatabaseServiceRecord) {
  if (service.engine !== 'mysql') {
    return 'Cluster topology applies to MySQL services.'
  }
  const peers = service.mysql_cluster_unmanaged_peers ?? []
  const peerText = peers.length
    ? ` Detected unmanaged peers: ${peers.map((peer) => `${peer.host}:${peer.port}`).join(', ')}.`
    : ''
  const reason = service.mysql_ddl_dml_block_reason ? ` ${service.mysql_ddl_dml_block_reason}` : ''
  return `${mysqlClusterLabel(service)} ${mysqlClusterBadgeLabel(service)}.${reason}${peerText}`.trim()
}

function formatDateTime(value: string | null) {
  if (!value) {
    return 'Never'
  }
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString()
}

function formatRelativeTime(value: string | null) {
  if (!value) {
    return 'Never'
  }
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value
  }
  const seconds = Math.max(0, Math.floor((nowMs.value - date.getTime()) / 1000))
  if (seconds < 60) {
    return `${seconds}s ago`
  }
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) {
    return `${minutes}m ago`
  }
  const hours = Math.floor(minutes / 60)
  if (hours < 24) {
    return `${hours}h ago`
  }
  return `${Math.floor(hours / 24)}d ago`
}

function displayNodeAddress(address: string) {
  return address.trim() || 'Pending agent registration'
}

function displayAgentVersion(version: string) {
  return version.trim() || 'Version pending'
}

function displayAgentHost(hostname: string, platform: string, architecture: string) {
  const host = hostname.trim() || 'Host pending'
  const runtime = [platform, architecture].filter(Boolean).join('/')
  return runtime ? `${host} · ${runtime}` : host
}

function agentConfigInSync(agent: InfrastructureNodeAgentRecord) {
  return agent.last_config_revision >= agent.desired_config_revision
}

function updateNumericSelections(event: Event, target: 'service_groups' | 'service_tags') {
  const element = event.target as HTMLSelectElement
  const values = Array.from(element.selectedOptions)
    .map((option) => Number(option.value))
    .filter((value) => Number.isFinite(value))
  if (target === 'service_groups') {
    serviceForm.team_ids = values
  }
}

async function loadMetadata() {
  metadata.value = await fetchInstanceInventoryMetadata(requireToken())
}

async function loadWorkflowPolicies() {
  try {
    const payload = await fetchWorkflowPolicies(requireToken())
    workflowPolicies.value = payload.results
  } catch {
    workflowPolicies.value = []
  }
}

async function loadNodeLabelNames() {
  nodeLabelNames.value = await fetchInfrastructureNodeLabelNames(requireToken())
}

function loadNodeLabelValues(labelName: string, filters: LabelFilter[]) {
  return fetchInfrastructureNodeLabelValues(labelName, requireToken(), filters)
}

async function loadNodes() {
  isLoading.value = true
  error.value = ''
  try {
    await authStore.loadCurrentUser()
    if (!canAccessInfrastructure.value) {
      nodes.value = []
      totalCount.value = 0
      error.value = 'You do not have permission to access infrastructure.'
      return
    }
    const response = await fetchInfrastructureNodes(requireToken(), {
      page: currentPage.value,
      size: pageSize.value,
      search: searchQuery.value,
      labelFilters: nodeLabelFilters.value,
    })
    nodes.value = response.results
    totalCount.value = response.count
    if (isDetailDialogOpen.value && selectedNode.value) {
      await loadSelectedNode()
    }
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to load infrastructure nodes.')
  } finally {
    isLoading.value = false
  }
}

function searchNodes() {
  if (currentPage.value !== 1) {
    currentPage.value = 1
    return
  }
  void loadNodes()
}

async function selectNode(nodeId: number) {
  detailLoading.value = true
  error.value = ''
  try {
    selectedNode.value = await fetchInfrastructureNode(nodeId, requireToken())
  } catch (errorValue) {
    selectedNode.value = null
    error.value = toUserFacingMessage(errorValue, 'Failed to load node detail.')
  } finally {
    detailLoading.value = false
  }
}

async function openNodeDetail(nodeId: number) {
  isDetailDialogOpen.value = true
  await selectNode(nodeId)
}

function closeNodeDetail() {
  isDetailDialogOpen.value = false
  selectedNode.value = null
}

async function loadSelectedNode() {
  if (!selectedNode.value) {
    return
  }
  await selectNode(selectedNode.value.id)
}

function resetNodeForm() {
  editingNodeId.value = null
  nodeForm.name = ''
  nodeForm.address = ''
  nodeForm.description = ''
  nodeForm.metadata = {}
  nodeForm.monitoring_enabled = true
  nodeForm.monitoring_labels = {}
  setMonitoringCollectors(nodeForm)
  nodeFormError.value = ''
}

function openNodeDialog(node?: InfrastructureNodeRecord) {
  resetNodeForm()
  if (node) {
    editingNodeId.value = node.id
    nodeForm.name = node.name
    nodeForm.address = node.address
    nodeForm.description = node.description
    nodeForm.metadata = node.metadata
    nodeForm.monitoring_enabled = node.monitoring_enabled
    nodeForm.monitoring_labels = { ...node.monitoring_labels }
    setMonitoringCollectors(nodeForm, node.monitoring_collectors)
  }
  isNodeDialogOpen.value = true
}

function closeNodeDialog() {
  isNodeDialogOpen.value = false
  resetNodeForm()
}

async function submitNode() {
  if (!editingNodeId.value) {
    nodeFormError.value = 'Use Add New Node to create nodes with an installable agent.'
    return
  }
  if (!nodeForm.name.trim()) {
    nodeFormError.value = 'Node name is required.'
    return
  }
  nodeSaving.value = true
  nodeFormError.value = ''
  try {
    const payload = {
      name: nodeForm.name.trim(),
      address: nodeForm.address.trim(),
      description: nodeForm.description.trim(),
      metadata: nodeForm.metadata,
      monitoring_enabled: nodeForm.monitoring_enabled,
      monitoring_collectors: [...nodeForm.monitoring_collectors],
      monitoring_labels: { ...nodeForm.monitoring_labels },
    }
    const detail = await updateInfrastructureNode(editingNodeId.value, payload, requireToken())
    selectedNode.value = detail
    isDetailDialogOpen.value = true
    feedback.value = 'Node updated.'
    closeNodeDialog()
    await loadNodes()
  } catch (errorValue) {
    nodeFormError.value = toUserFacingMessage(errorValue, 'Failed to save node.')
  } finally {
    nodeSaving.value = false
  }
}

function resetServiceForm() {
  editingServiceId.value = null
  serviceForm.node_id = selectedNode.value?.id ?? 0
  serviceForm.service_name = ''
  serviceForm.role = 'master'
  serviceForm.engine = 'mysql'
  serviceForm.host = selectedNode.value?.address ?? ''
  serviceForm.port = 3306
  serviceForm.user = ''
  serviceForm.password = ''
  serviceForm.monitoring_enabled = true
  serviceForm.queryable = false
  serviceForm.workflow_enabled = false
  serviceForm.workflow_policy = null
  serviceForm.monitoring_labels = {}
  setServiceMonitoringCollectors(serviceForm)
  serviceForm.is_ssl = false
  serviceForm.verify_ssl = true
  serviceForm.db_name = ''
  serviceForm.show_db_name_regex = ''
  serviceForm.denied_db_name_regex = ''
  serviceForm.charset = ''
  serviceForm.team_ids = []
  delete serviceForm.recommendation_id
  serviceFormError.value = ''
  clusterForm.name = ''
  clusterForm.label_value = ''
  clusterFormError.value = ''
}

function openServiceDialog(
  service?: DatabaseServiceRecord,
  recommendation?: ServiceRecommendationRecord,
) {
  resetServiceForm()
  if (service) {
    editingServiceId.value = service.id
    serviceForm.node_id = service.node_id ?? selectedNode.value?.id ?? 0
    serviceForm.service_name = service.service_name
    serviceForm.role = service.role
    serviceForm.engine = service.engine
    serviceForm.host = service.host
    serviceForm.port = service.port
    serviceForm.user = service.user
    serviceForm.monitoring_enabled = service.monitoring_enabled
    serviceForm.queryable = service.queryable
    serviceForm.workflow_enabled = service.workflow_enabled
    serviceForm.workflow_policy = service.workflow_policy ?? null
    serviceForm.monitoring_labels = { ...service.monitoring_labels }
    setServiceMonitoringCollectors(serviceForm, service.monitoring_collectors)
    serviceForm.is_ssl = service.is_ssl
    serviceForm.verify_ssl = service.verify_ssl
    serviceForm.db_name = service.db_name
    serviceForm.show_db_name_regex = service.show_db_name_regex
    serviceForm.denied_db_name_regex = service.denied_db_name_regex
    serviceForm.charset = service.charset
    serviceForm.team_ids = [...service.team_ids]
    clusterForm.name = service.mysql_cluster_name || ''
    clusterForm.label_value = service.mysql_cluster_label || ''
  }
  if (recommendation) {
    serviceForm.recommendation_id = recommendation.id
    serviceForm.engine = recommendation.engine
    serviceForm.host = recommendation.host
    serviceForm.port = recommendation.port
    serviceForm.service_name =
      recommendation.service_name ||
      `${selectedNode.value?.name || 'node'}-${recommendation.engine}-${recommendation.port}`
  }
  isServiceDialogOpen.value = true
}

function closeServiceDialog() {
  isServiceDialogOpen.value = false
  resetServiceForm()
}

function applyEngineDefaultPort() {
  serviceForm.port = serviceForm.engine === 'pgsql' ? 5432 : 3306
  setServiceMonitoringCollectors(serviceForm)
}

async function submitService() {
  if (!serviceForm.node_id || !serviceForm.service_name.trim() || !serviceForm.host.trim()) {
    serviceFormError.value = 'Service name, node, and host are required.'
    return
  }
  if ((serviceForm.queryable || serviceForm.workflow_enabled) && !serviceForm.workflow_policy) {
    serviceFormError.value = 'Select a workflow policy before enabling SQL queries or DDL/DML workflows.'
    return
  }
  if (serviceForm.workflow_policy) {
    const selectedPolicy = workflowPolicies.value.find((policy) => policy.id === serviceForm.workflow_policy)
    if (!selectedPolicy?.is_active) {
      serviceFormError.value = 'Select an active workflow policy before saving this service.'
      return
    }
  }
  serviceSaving.value = true
  serviceFormError.value = ''
  try {
    const payload = {
      ...serviceForm,
      service_name: serviceForm.service_name.trim(),
      host: serviceForm.host.trim(),
    }
    if (editingServiceId.value) {
      await updateDatabaseService(editingServiceId.value, payload, requireToken())
      feedback.value = 'Service updated.'
    } else {
      await createDatabaseService(payload, requireToken())
      feedback.value = 'Service added.'
    }
    closeServiceDialog()
    await loadSelectedNode()
    await loadNodes()
  } catch (errorValue) {
    serviceFormError.value = toUserFacingMessage(errorValue, 'Failed to save service.')
  } finally {
    serviceSaving.value = false
  }
}

async function submitClusterUpdate() {
  const clusterId = editingService.value?.mysql_cluster_id
  if (!clusterId) {
    clusterFormError.value = 'This service is not assigned to a MySQL cluster.'
    return
  }
  if (!clusterForm.name.trim() || !clusterForm.label_value.trim()) {
    clusterFormError.value = 'Cluster name and metric label are required.'
    return
  }
  clusterSaving.value = true
  clusterFormError.value = ''
  try {
    await updateMysqlCluster(
      clusterId,
      {
        name: clusterForm.name.trim(),
        label_value: clusterForm.label_value.trim(),
      },
      requireToken(),
    )
    feedback.value = 'MySQL cluster updated.'
    await loadSelectedNode()
    await loadNodes()
  } catch (errorValue) {
    clusterFormError.value = toUserFacingMessage(errorValue, 'Failed to save MySQL cluster.')
  } finally {
    clusterSaving.value = false
  }
}

function openNewNodeDialog() {
  agentTargetNodeId.value = null
  agentForm.node_name = ''
  agentForm.monitoring_enabled = true
  setMonitoringCollectors(agentForm)
  agentFormError.value = ''
  createdAgent.value = null
  isAgentDialogOpen.value = true
}

function openAgentDialog() {
  agentTargetNodeId.value = selectedNode.value?.id ?? null
  agentForm.node_name = selectedNode.value?.name ?? ''
  agentForm.monitoring_enabled = selectedNode.value?.monitoring_enabled ?? true
  setMonitoringCollectors(agentForm, selectedNode.value?.monitoring_collectors)
  agentFormError.value = ''
  createdAgent.value = null
  isAgentDialogOpen.value = true
}

function closeAgentDialog() {
  isAgentDialogOpen.value = false
  createdAgent.value = null
  agentTargetNodeId.value = null
}

async function submitAgent() {
  if (!agentTargetNodeId.value && !agentForm.node_name.trim()) {
    agentFormError.value = 'Node name is required.'
    return
  }
  agentSaving.value = true
  agentFormError.value = ''
  try {
    createdAgent.value = await createAgent(
      agentTargetNodeId.value
        ? {
            local_node: agentTargetNodeId.value,
            monitoring_enabled: agentForm.monitoring_enabled,
            monitoring_collectors: [...agentForm.monitoring_collectors],
          }
        : {
            node_name: agentForm.node_name.trim(),
            monitoring_enabled: agentForm.monitoring_enabled,
            monitoring_collectors: [...agentForm.monitoring_collectors],
          },
      requireToken(),
    )
    await loadSelectedNode()
    await loadNodes()
  } catch (errorValue) {
    agentFormError.value = toUserFacingMessage(errorValue, 'Failed to create agent.')
  } finally {
    agentSaving.value = false
  }
}

async function issueExistingAgentInstallKey() {
  if (!selectedNode.value?.agent_id) {
    return
  }
  agentInstallIssuing.value = true
  error.value = ''
  feedback.value = ''
  try {
    createdAgent.value = await issueAgentInstallKey(selectedNode.value.agent_id, requireToken())
    isAgentDialogOpen.value = true
    feedback.value = 'Agent API key created for install.'
    await loadSelectedNode()
    await loadNodes()
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to create agent API key.')
  } finally {
    agentInstallIssuing.value = false
  }
}

async function copyText(value: string) {
  error.value = ''
  feedback.value = ''
  try {
    await navigator.clipboard.writeText(value)
    feedback.value = 'Copied to clipboard.'
  } catch (errorValue) {
    console.error('Failed to copy text to clipboard.', errorValue)
    error.value = toUserFacingMessage(errorValue, 'Failed to copy to clipboard.')
  }
}

async function discoverServices() {
  if (!selectedNode.value) {
    return
  }
  discoveringNodeId.value = selectedNode.value.id
  error.value = ''
  feedback.value = ''
  try {
    await discoverInfrastructureNodeServices(selectedNode.value.id, requireToken())
    feedback.value = 'Discovery started.'
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, 'Failed to start discovery.')
  } finally {
    discoveringNodeId.value = null
  }
}

async function ignoreRecommendation(recommendation: ServiceRecommendationRecord) {
  error.value = ''
  feedback.value = ''
  try {
    await updateServiceRecommendationStatus(recommendation.id, 'ignored', requireToken())
    await loadSelectedNode()
    await loadNodes()
  } catch (errorValue) {
    console.error('Failed to ignore service recommendation.', errorValue)
    error.value = toUserFacingMessage(errorValue, 'Failed to ignore recommendation.')
  }
}

async function testService(service: DatabaseServiceRecord) {
  testingServiceId.value = service.id
  error.value = ''
  feedback.value = ''
  try {
    feedback.value = `${service.service_name}: ${await testDatabaseServiceConnection(service.id, requireToken())}`
  } catch (errorValue) {
    error.value = toUserFacingMessage(errorValue, `Failed to test ${service.service_name}.`)
  } finally {
    testingServiceId.value = null
  }
}

onMounted(async () => {
  clockTimer = setInterval(() => {
    nowMs.value = Date.now()
  }, 1000)
  await authStore.loadCurrentUser()
  if (!canAccessInfrastructure.value) {
    error.value = 'You do not have permission to access infrastructure.'
    return
  }
  await Promise.all([loadMetadata(), loadWorkflowPolicies(), loadNodeLabelNames(), loadNodes()])
  refreshTimer = setInterval(() => {
    if (
      !isLoading.value &&
      !detailLoading.value &&
      !isServiceDialogOpen.value &&
      !isNodeDialogOpen.value
    ) {
      void loadNodes()
    }
  }, 30_000)
})

onUnmounted(() => {
  if (refreshTimer) {
    clearInterval(refreshTimer)
  }
  if (clockTimer) {
    clearInterval(clockTimer)
  }
})

watch([currentPage, pageSize], () => {
  void loadNodes()
})
watch(
  nodeLabelFilters,
  () => {
    if (currentPage.value !== 1) {
      currentPage.value = 1
      return
    }
    void loadNodes()
  },
  { deep: true },
)
</script>

<template>
  <section class="grid gap-6">
    <div class="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
      <div>
        <h2 class="text-2xl font-semibold text-slate-900">Infrastructure</h2>
      </div>
      <div class="flex flex-wrap gap-2">
        <Button variant="outline" type="button" @click="void loadNodes()">
          <RefreshCw class="h-4 w-4" />
          Refresh
        </Button>
        <Button type="button" @click="openNewNodeDialog">
          <Plus class="h-4 w-4" />
          Add New Node
        </Button>
      </div>
    </div>

    <p
      v-if="error"
      class="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
    >
      {{ error }}
    </p>
    <p
      v-if="feedback"
      class="rounded-lg border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700"
    >
      {{ feedback }}
    </p>

    <div class="overflow-hidden rounded-lg border border-slate-200 bg-white shadow-sm">
      <div class="grid gap-3 border-b border-slate-200 p-4">
        <div class="flex flex-col gap-3 lg:flex-row lg:items-center">
          <div class="relative flex-1">
            <Search class="pointer-events-none absolute left-3 top-2.5 h-4 w-4 text-slate-400" />
            <Input
              v-model="searchQuery"
              class="h-10 pl-9 text-sm"
              placeholder="Search node name, address, service, or agent"
              @keyup.enter="searchNodes"
            />
          </div>
          <Button variant="outline" type="button" :disabled="isLoading" @click="searchNodes">
            Search
          </Button>
        </div>
        <LabelFilterBar
          v-model="nodeLabelFilters"
          :label-names="nodeLabelNames"
          :load-values="loadNodeLabelValues"
          placeholder="Filter nodes with environment:prod or -team:legacy"
        />
      </div>

      <div
        class="flex flex-wrap items-center justify-between gap-3 border-b border-slate-100 px-4 py-3 text-sm"
      >
        <div class="flex items-center gap-3 text-slate-500">
          <span class="font-medium text-slate-900">{{ totalCount }} nodes</span>
          <span>{{ isLoading ? 'Loading...' : 'Updated' }}</span>
        </div>
        <div class="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            type="button"
            :disabled="currentPage === 1 || isLoading"
            @click="currentPage -= 1"
          >
            Previous
          </Button>
          <span class="px-2 text-sm text-slate-500">Page {{ currentPage }}</span>
          <Button
            variant="outline"
            size="sm"
            type="button"
            :disabled="nodes.length < pageSize || isLoading"
            @click="currentPage += 1"
          >
            Next
          </Button>
        </div>
      </div>

      <div class="overflow-x-auto">
        <table class="min-w-full divide-y divide-slate-200 text-sm">
          <thead class="bg-slate-50 text-left text-xs font-semibold uppercase text-slate-500">
            <tr>
              <th class="px-4 py-3">Node</th>
              <th class="px-4 py-3">Agent</th>
              <th class="px-4 py-3">Monitoring</th>
              <th class="px-4 py-3">Services</th>
              <th class="px-4 py-3">Recommendations</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-slate-100 bg-white">
            <tr
              v-for="node in nodes"
              :key="node.id"
              tabindex="0"
              class="cursor-pointer transition hover:bg-slate-50 focus:bg-slate-50 focus:outline-none"
              @click="void openNodeDetail(node.id)"
              @keyup.enter="void openNodeDetail(node.id)"
            >
              <td class="px-4 py-3">
                <div class="grid gap-1">
                  <span class="font-medium text-slate-900">{{ node.name }}</span>
                  <span class="font-mono text-xs text-slate-500">
                    {{ displayNodeAddress(node.address) }}
                  </span>
                  <div
                    v-if="Object.keys(node.monitoring_labels).length"
                    class="flex flex-wrap gap-1"
                  >
                    <Badge
                      v-for="(value, label) in node.monitoring_labels"
                      :key="label"
                      variant="outline"
                      class="font-mono text-[11px] font-normal"
                    >
                      {{ label }}:{{ value }}
                    </Badge>
                  </div>
                </div>
              </td>
              <td class="px-4 py-3">
                <div class="grid gap-1">
                  <div class="flex items-center gap-2">
                    <Badge variant="secondary" :class="statusBadgeClass(node.agent_status)">
                      {{ node.agent_status || 'No agent' }}
                    </Badge>
                  </div>
                  <span v-if="node.agent" class="text-xs text-slate-500">
                    {{ displayAgentVersion(node.agent.agent_version) }}
                  </span>
                </div>
              </td>
              <td class="px-4 py-3">
                <Badge
                  variant="secondary"
                  :class="
                    node.monitoring_enabled
                      ? 'bg-emerald-100 text-emerald-800'
                      : 'bg-slate-100 text-slate-700'
                  "
                >
                  {{ node.monitoring_enabled ? 'Enabled' : 'Disabled' }}
                </Badge>
              </td>
              <td class="px-4 py-3 text-slate-700">{{ node.service_count }}</td>
              <td class="px-4 py-3 text-slate-700">{{ node.recommendation_count }}</td>
            </tr>
            <tr v-if="!isLoading && nodes.length === 0">
              <td colspan="5" class="px-4 py-10 text-center text-sm text-slate-500">
                No nodes found.
              </td>
            </tr>
            <tr v-if="isLoading">
              <td colspan="5" class="px-4 py-10 text-center text-sm text-slate-500">
                Loading nodes...
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <div
      v-if="isDetailDialogOpen"
      class="fixed inset-0 z-40 flex items-center justify-center bg-slate-950/45 px-4 py-6"
      @click.self="closeNodeDetail"
    >
      <div class="max-h-[92vh] w-full max-w-6xl overflow-y-auto rounded-lg bg-white shadow-xl">
        <div class="sticky top-0 z-10 border-b border-slate-200 bg-white px-6 py-4">
          <div class="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
            <div>
              <h3 class="text-lg font-semibold text-slate-900">
                {{ selectedNode?.name || 'Node detail' }}
              </h3>
              <p v-if="selectedNode" class="mt-1 text-sm text-slate-500">
                {{ displayNodeAddress(selectedNode.address) }}
              </p>
            </div>
            <div class="flex flex-wrap items-center gap-2">
              <template v-if="selectedNode">
                <Button variant="outline" type="button" @click="openNodeDialog(selectedNode)"
                  >Edit Node</Button
                >
                <Button
                  v-if="!selectedNode.agent_id"
                  variant="outline"
                  type="button"
                  @click="openAgentDialog"
                >
                  <ServerCog class="h-4 w-4" />
                  Install Agent
                </Button>
                <Button
                  v-else
                  variant="outline"
                  type="button"
                  :disabled="agentInstallIssuing"
                  @click="void issueExistingAgentInstallKey()"
                >
                  <ServerCog class="h-4 w-4" />
                  Install Agent
                </Button>
                <Button
                  variant="outline"
                  type="button"
                  :disabled="!selectedNode.agent_id || discoveringNodeId === selectedNode.id"
                  @click="void discoverServices()"
                >
                  <Wand2 class="h-4 w-4" />
                  Discover
                </Button>
                <Button type="button" @click="openServiceDialog()">
                  <Plus class="h-4 w-4" />
                  Add Service
                </Button>
              </template>
              <Button variant="ghost" size="icon" type="button" @click="closeNodeDetail">
                <X class="h-4 w-4" />
              </Button>
            </div>
          </div>
        </div>

        <div class="px-6 py-5">
          <div v-if="detailLoading" class="py-12 text-center text-sm text-slate-500">
            Loading node...
          </div>
          <div v-else-if="!selectedNode" class="py-12 text-center text-sm text-slate-500">
            Node detail unavailable.
          </div>
          <div v-else class="grid gap-8">
            <section class="grid gap-3">
              <h4 class="text-sm font-semibold uppercase text-slate-500">Agent</h4>
              <div class="rounded-lg border border-slate-200 bg-white p-4">
                <div v-if="selectedNode.agent" class="grid gap-4 md:grid-cols-4">
                  <div class="grid gap-1">
                    <span class="text-xs font-semibold uppercase text-slate-500">Status</span>
                    <Badge
                      variant="secondary"
                      class="w-fit"
                      :class="statusBadgeClass(selectedNode.agent.status)"
                    >
                      {{ selectedNode.agent.status }}
                    </Badge>
                  </div>
                  <div class="grid gap-1">
                    <span class="text-xs font-semibold uppercase text-slate-500">Version</span>
                    <span class="text-sm text-slate-900">
                      {{ displayAgentVersion(selectedNode.agent.agent_version) }}
                    </span>
                  </div>
                  <div class="grid gap-1">
                    <span class="text-xs font-semibold uppercase text-slate-500">Host</span>
                    <span class="text-sm text-slate-900">
                      {{
                        displayAgentHost(
                          selectedNode.agent.hostname,
                          selectedNode.agent.platform,
                          selectedNode.agent.architecture,
                        )
                      }}
                    </span>
                  </div>
                  <div class="grid gap-1">
                    <span class="text-xs font-semibold uppercase text-slate-500">Last Seen</span>
                    <div class="grid gap-0.5">
                      <span class="text-sm text-slate-900">
                        {{ formatRelativeTime(selectedNode.agent.last_seen_at) }}
                      </span>
                      <span class="text-xs text-slate-500">
                        {{ formatDateTime(selectedNode.agent.last_seen_at) }}
                      </span>
                    </div>
                  </div>
                  <div class="grid gap-1">
                    <span class="text-xs font-semibold uppercase text-slate-500">WS Heartbeat</span>
                    <div class="grid gap-0.5">
                      <span class="text-sm text-slate-900">
                        {{ formatRelativeTime(selectedNode.agent.last_websocket_pong_at) }}
                      </span>
                      <span class="text-xs text-slate-500">
                        {{ formatDateTime(selectedNode.agent.last_websocket_pong_at) }}
                      </span>
                    </div>
                  </div>
                  <div class="grid gap-1">
                    <span class="text-xs font-semibold uppercase text-slate-500"
                      >Configuration</span
                    >
                    <Badge
                      variant="secondary"
                      class="w-fit"
                      :class="
                        agentConfigInSync(selectedNode.agent)
                          ? 'bg-emerald-100 text-emerald-800'
                          : 'bg-amber-100 text-amber-800'
                      "
                    >
                      {{ agentConfigInSync(selectedNode.agent) ? 'In sync' : 'Change pending' }}
                    </Badge>
                  </div>
                  <div class="grid gap-1">
                    <span class="text-xs font-semibold uppercase text-slate-500">Connected</span>
                    <span class="text-sm text-slate-900">
                      {{ formatDateTime(selectedNode.agent.last_connected_at) }}
                    </span>
                  </div>
                  <div class="grid gap-1">
                    <span class="text-xs font-semibold uppercase text-slate-500">Enabled</span>
                    <span class="text-sm text-slate-900">
                      {{ selectedNode.agent.enabled ? 'Yes' : 'No' }}
                    </span>
                  </div>
                </div>
                <div v-else class="text-sm text-slate-500">No agent is attached to this node.</div>
              </div>
            </section>

            <section class="grid gap-3">
              <h4 class="text-sm font-semibold uppercase text-slate-500">Services</h4>
              <div class="overflow-x-auto rounded-lg border border-slate-200">
                <table class="min-w-full divide-y divide-slate-200 text-sm">
                  <thead class="bg-slate-50 text-left text-xs uppercase text-slate-500">
                    <tr>
                      <th class="px-4 py-3">Service</th>
                      <th class="px-4 py-3">Engine</th>
                      <th class="px-4 py-3">Cluster</th>
                      <th class="px-4 py-3">Endpoint</th>
                      <th class="px-4 py-3">Monitoring</th>
                      <th class="px-4 py-3">Workflows</th>
                      <th class="px-4 py-3">Inventory</th>
                      <th class="px-4 py-3 text-right">Actions</th>
                    </tr>
                  </thead>
                  <tbody class="divide-y divide-slate-100 bg-white">
                    <tr v-for="service in selectedNode.services" :key="service.id">
                      <td class="px-4 py-3 font-medium text-slate-900">
                        {{ service.service_name }}
                      </td>
                      <td class="px-4 py-3 text-slate-600">{{ service.engine.toUpperCase() }}</td>
                      <td class="px-4 py-3" :title="mysqlClusterTitle(service)">
                        <div class="flex min-w-36 flex-col gap-1">
                          <span class="text-sm font-medium text-slate-900">
                            {{ mysqlClusterLabel(service) }}
                          </span>
                          <Badge
                            variant="secondary"
                            class="w-fit"
                            :class="mysqlClusterStatusClass(service)"
                            :aria-label="mysqlClusterTitle(service)"
                          >
                            {{ mysqlClusterBadgeLabel(service) }}
                          </Badge>
                        </div>
                      </td>
                      <td class="px-4 py-3 text-slate-600">
                        {{ service.host }}:{{ service.port }}
                      </td>
                      <td class="px-4 py-3">
                        <Badge
                          variant="secondary"
                          :class="
                            service.monitoring_enabled
                              ? 'bg-emerald-100 text-emerald-800'
                              : 'bg-slate-100 text-slate-600'
                          "
                        >
                          {{ service.monitoring_enabled ? 'Enabled' : 'Disabled' }}
                        </Badge>
                      </td>
                      <td class="px-4 py-3">
                        <Badge
                          variant="secondary"
                          :class="
                            service.workflow_enabled
                              ? 'bg-sky-100 text-sky-800'
                              : 'bg-slate-100 text-slate-600'
                          "
                        >
                          {{ service.workflow_enabled ? 'Enabled' : 'Disabled' }}
                        </Badge>
                      </td>
                      <td class="px-4 py-3" :title="inventoryStatusTitle(service)">
                        <Badge
                          variant="secondary"
                          :class="serviceStatusClass(service.inventory_status)"
                        >
                          {{ inventoryStatusLabel(service.inventory_status) }}
                        </Badge>
                      </td>
                      <td class="px-4 py-3">
                        <div class="flex justify-end gap-2">
                          <Button
                            variant="outline"
                            size="sm"
                            type="button"
                            @click="openServiceDialog(service)"
                          >
                            Edit
                          </Button>
                          <Button
                            variant="outline"
                            size="sm"
                            type="button"
                            :disabled="testingServiceId === service.id"
                            @click="void testService(service)"
                          >
                            Test
                          </Button>
                        </div>
                      </td>
                    </tr>
                    <tr v-if="selectedNode.services.length === 0">
                      <td colspan="6" class="px-4 py-8 text-center text-slate-500">
                        No services added.
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </section>

            <section class="grid gap-3">
              <h4 class="text-sm font-semibold uppercase text-slate-500">Recommendations</h4>
              <div class="overflow-x-auto rounded-lg border border-slate-200">
                <table class="min-w-full divide-y divide-slate-200 text-sm">
                  <thead class="bg-slate-50 text-left text-xs uppercase text-slate-500">
                    <tr>
                      <th class="px-4 py-3">Engine</th>
                      <th class="px-4 py-3">Endpoint</th>
                      <th class="px-4 py-3">Source</th>
                      <th class="px-4 py-3">Last Seen</th>
                      <th class="px-4 py-3 text-right">Actions</th>
                    </tr>
                  </thead>
                  <tbody class="divide-y divide-slate-100 bg-white">
                    <tr v-for="recommendation in recommendedServices" :key="recommendation.id">
                      <td class="px-4 py-3 font-medium text-slate-900">
                        {{ recommendation.engine.toUpperCase() }}
                      </td>
                      <td class="px-4 py-3 text-slate-600">
                        {{ recommendation.host }}:{{ recommendation.port }}
                      </td>
                      <td class="px-4 py-3 text-slate-600">
                        {{ recommendation.source }} · {{ recommendation.confidence }}%
                      </td>
                      <td class="px-4 py-3 text-slate-600">
                        {{ formatDateTime(recommendation.last_seen_at) }}
                      </td>
                      <td class="px-4 py-3">
                        <div class="flex justify-end gap-2">
                          <Button
                            size="sm"
                            type="button"
                            @click="openServiceDialog(undefined, recommendation)"
                          >
                            <Check class="h-4 w-4" />
                            Add
                          </Button>
                          <Button
                            variant="outline"
                            size="sm"
                            type="button"
                            @click="void ignoreRecommendation(recommendation)"
                          >
                            Ignore
                          </Button>
                        </div>
                      </td>
                    </tr>
                    <tr v-if="recommendedServices.length === 0">
                      <td colspan="5" class="px-4 py-8 text-center text-slate-500">
                        No recommendations.
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </section>
          </div>
        </div>
      </div>
    </div>

    <div
      v-if="isNodeDialogOpen"
      class="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/45 px-4 py-6"
      @click.self="closeNodeDialog"
    >
      <form
        class="max-h-[90vh] w-full max-w-2xl overflow-y-auto rounded-lg bg-white shadow-xl"
        @submit.stop.prevent="submitNode"
      >
        <div class="flex items-start justify-between border-b border-slate-200 px-6 py-4">
          <h3 class="text-lg font-semibold text-slate-900">Edit Node</h3>
          <Button variant="ghost" size="icon" type="button" @click="closeNodeDialog">
            <X class="h-4 w-4" />
          </Button>
        </div>
        <div class="grid gap-5 px-6 py-5">
          <p
            v-if="nodeFormError"
            class="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
          >
            {{ nodeFormError }}
          </p>
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Name</span>
            <Input v-model="nodeForm.name" required placeholder="prod-db-node-01" />
          </label>
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Address</span>
            <Input v-model="nodeForm.address" placeholder="Pending agent registration" />
          </label>
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Description</span>
            <textarea v-model="nodeForm.description" :class="fieldClass" rows="3" />
          </label>
          <label class="flex items-center gap-2 text-sm font-medium text-slate-700">
            <input
              v-model="nodeForm.monitoring_enabled"
              type="checkbox"
              class="h-4 w-4 rounded border-slate-300"
            />
            Enable monitoring
          </label>
          <MonitoringLabelsEditor v-model="nodeForm.monitoring_labels" />
          <details
            class="rounded-lg border border-slate-200 bg-slate-50 px-4 py-3"
            :class="{ 'opacity-60': !nodeForm.monitoring_enabled }"
          >
            <summary class="cursor-pointer text-sm font-medium text-slate-800">
              Advanced collectors
              <span class="ml-2 text-xs font-normal text-slate-500">
                {{ nodeForm.monitoring_collectors.length }} selected
              </span>
            </summary>
            <div class="mt-4 grid gap-4">
              <div class="flex flex-wrap gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  type="button"
                  :disabled="!nodeForm.monitoring_enabled"
                  @click="selectAllMonitoringCollectors(nodeForm)"
                >
                  Select all
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  type="button"
                  :disabled="!nodeForm.monitoring_enabled"
                  @click="resetDefaultMonitoringCollectors(nodeForm)"
                >
                  Reset defaults
                </Button>
              </div>
              <div class="grid max-h-56 gap-2 overflow-y-auto pr-1 sm:grid-cols-2 md:grid-cols-3">
                <label
                  v-for="collector in DEFAULT_NODE_EXPORTER_COLLECTORS"
                  :key="collector"
                  class="flex min-w-0 items-center gap-2 rounded-md bg-white px-2 py-1.5 text-sm text-slate-700"
                >
                  <input
                    :checked="isMonitoringCollectorSelected(nodeForm, collector)"
                    :disabled="!nodeForm.monitoring_enabled"
                    type="checkbox"
                    class="h-4 w-4 rounded border-slate-300"
                    @change="toggleMonitoringCollector(nodeForm, collector, $event)"
                  />
                  <span class="truncate">{{ collector }}</span>
                </label>
              </div>
            </div>
          </details>
        </div>
        <div class="flex justify-end gap-2 border-t border-slate-200 px-6 py-4">
          <Button variant="outline" type="button" @click="closeNodeDialog">Cancel</Button>
          <button
            data-testid="node-save"
            type="button"
            class="inline-flex h-9 items-center justify-center rounded-md bg-slate-900 px-4 py-2 text-sm font-medium text-white shadow hover:bg-slate-800 disabled:pointer-events-none disabled:opacity-50"
            :disabled="nodeSaving"
            @click.stop.prevent="submitNode"
          >
            {{ nodeSaving ? 'Saving...' : 'Save' }}
          </button>
        </div>
      </form>
    </div>

    <div
      v-if="isServiceDialogOpen"
      class="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/45 px-4 py-6"
      @click.self="closeServiceDialog"
    >
      <form
        class="max-h-[92vh] w-full max-w-3xl overflow-y-auto rounded-lg bg-white shadow-xl"
        @submit.stop.prevent="submitService"
      >
        <div class="flex items-start justify-between border-b border-slate-200 px-6 py-4">
          <h3 class="text-lg font-semibold text-slate-900">
            {{ editingServiceId ? 'Edit Service' : 'Add Service' }}
          </h3>
          <Button variant="ghost" size="icon" type="button" @click="closeServiceDialog">
            <X class="h-4 w-4" />
          </Button>
        </div>
        <div class="grid gap-5 px-6 py-5 md:grid-cols-2">
          <p
            v-if="serviceFormError"
            class="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700 md:col-span-2"
          >
            {{ serviceFormError }}
          </p>
          <div
            v-if="editingService && editingService.engine === 'mysql'"
            class="grid gap-2 rounded-lg border border-slate-200 bg-slate-50 px-4 py-3 md:col-span-2"
          >
            <div class="flex flex-wrap items-center gap-2">
              <span class="text-sm font-medium text-slate-900">
                {{ mysqlClusterLabel(editingService) }}
              </span>
              <Badge
                variant="secondary"
                class="w-fit"
                :class="mysqlClusterStatusClass(editingService)"
                :aria-label="mysqlClusterTitle(editingService)"
              >
                {{ mysqlClusterBadgeLabel(editingService) }}
              </Badge>
              <span
                v-if="editingService.mysql_cluster_label"
                class="text-xs text-slate-500"
              >
                dm_mysql_cluster={{ editingService.mysql_cluster_label }}
                  </span>
            </div>
            <div v-if="editingService.mysql_cluster_id" class="grid gap-3 md:grid-cols-[1fr_1fr_auto]">
              <label class="grid gap-1">
                <span class="text-xs font-medium uppercase text-slate-500">Cluster name</span>
                <Input v-model="clusterForm.name" :disabled="clusterSaving" />
              </label>
              <label class="grid gap-1">
                <span class="text-xs font-medium uppercase text-slate-500">Metric label</span>
                <Input v-model="clusterForm.label_value" :disabled="clusterSaving" />
              </label>
              <Button
                type="button"
                variant="outline"
                class="self-end"
                :disabled="clusterSaving"
                @click="submitClusterUpdate"
              >
                {{ clusterSaving ? 'Saving...' : 'Save cluster' }}
              </Button>
            </div>
            <p v-if="clusterFormError" class="text-xs text-red-600">
              {{ clusterFormError }}
            </p>
            <p
              v-if="editingService.mysql_ddl_dml_block_reason"
              class="text-xs text-amber-700"
            >
              {{ editingService.mysql_ddl_dml_block_reason }}
            </p>
            <p
              v-if="(editingService.mysql_cluster_unmanaged_peers ?? []).length > 0"
              class="text-xs text-slate-600"
            >
              Unmanaged peers:
              {{
                (editingService.mysql_cluster_unmanaged_peers ?? [])
                  .map((peer) => `${peer.host}:${peer.port}`)
                  .join(', ')
              }}
            </p>
          </div>
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Service Name</span>
            <Input v-model="serviceForm.service_name" required placeholder="orders-primary" />
          </label>
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Engine</span>
            <select
              v-model="serviceForm.engine"
              :class="selectClass"
              @change="applyEngineDefaultPort"
            >
              <option value="mysql">MySQL</option>
              <option value="pgsql">PostgreSQL</option>
            </select>
          </label>
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Host</span>
            <Input v-model="serviceForm.host" required />
          </label>
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Port</span>
            <Input v-model.number="serviceForm.port" required type="number" min="1" />
          </label>
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">User</span>
            <Input v-model="serviceForm.user" />
          </label>
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Password</span>
            <Input v-model="serviceForm.password" type="password" autocomplete="new-password" />
          </label>
          <label class="flex items-center gap-2 text-sm text-slate-700">
            <input
              v-model="serviceForm.monitoring_enabled"
              type="checkbox"
              class="h-4 w-4 rounded border-slate-300"
            />
            Enable monitoring
          </label>
          <label class="flex items-center gap-2 text-sm text-slate-700">
            <input
              v-model="serviceForm.queryable"
              type="checkbox"
              class="h-4 w-4 rounded border-slate-300"
            />
            Enable SQL queries
          </label>
          <label class="flex items-center gap-2 text-sm text-slate-700">
            <input
              v-model="serviceForm.workflow_enabled"
              type="checkbox"
              class="h-4 w-4 rounded border-slate-300"
            />
            Enable DDL/DML workflows
          </label>
          <label
            v-if="serviceForm.queryable || serviceForm.workflow_enabled"
            class="grid gap-2 md:col-span-2"
          >
            <span class="text-sm font-medium text-slate-700">Workflow policy</span>
            <select v-model.number="serviceForm.workflow_policy" :class="selectClass">
              <option :value="null">Select a workflow policy</option>
              <option
                v-for="policy in serviceWorkflowPolicyOptions"
                :key="policy.id"
                :value="policy.id"
              >
                {{ policy.name }}{{ policy.is_active ? '' : ' (inactive)' }}
              </option>
            </select>
            <span class="text-xs text-slate-500">
              Required for SQL query exports and DDL/DML workflow submissions.
            </span>
          </label>
          <MonitoringLabelsEditor
            v-model="serviceForm.monitoring_labels"
            class="md:col-span-2"
            :inherited="selectedNode?.monitoring_labels ?? {}"
          />
          <details
            class="rounded-lg border border-slate-200 bg-slate-50 px-4 py-3 md:col-span-2"
            :class="{ 'opacity-60': !serviceForm.monitoring_enabled }"
          >
            <summary class="cursor-pointer text-sm font-medium text-slate-800">
              Advanced collectors
              <span class="ml-2 text-xs font-normal text-slate-500">
                {{ serviceForm.monitoring_collectors.length }} selected
              </span>
            </summary>
            <div class="mt-4 grid gap-4">
              <div class="flex flex-wrap gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  type="button"
                  :disabled="!serviceForm.monitoring_enabled"
                  @click="selectAllServiceMonitoringCollectors(serviceForm)"
                >
                  Select all
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  type="button"
                  :disabled="!serviceForm.monitoring_enabled"
                  @click="resetDefaultServiceMonitoringCollectors(serviceForm)"
                >
                  Reset defaults
                </Button>
              </div>
              <div class="grid max-h-56 gap-2 overflow-y-auto pr-1 sm:grid-cols-2 md:grid-cols-3">
                <label
                  v-for="collector in serviceCollectorOptions(serviceForm.engine)"
                  :key="collector"
                  class="flex min-w-0 items-center gap-2 rounded-md bg-white px-2 py-1.5 text-sm text-slate-700"
                >
                  <input
                    :checked="isMonitoringCollectorSelected(serviceForm, collector)"
                    :disabled="!serviceForm.monitoring_enabled"
                    type="checkbox"
                    class="h-4 w-4 rounded border-slate-300"
                    @change="toggleMonitoringCollector(serviceForm, collector, $event)"
                  />
                  <span class="truncate">{{ collector }}</span>
                </label>
              </div>
            </div>
          </details>
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Default Database</span>
            <Input v-model="serviceForm.db_name" />
          </label>
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Charset</span>
            <Input v-model="serviceForm.charset" placeholder="utf8mb4" />
          </label>
          <label class="flex items-center gap-2 text-sm text-slate-700">
            <input
              v-model="serviceForm.is_ssl"
              type="checkbox"
              class="h-4 w-4 rounded border-slate-300"
            />
            SSL
          </label>
          <label class="flex items-center gap-2 text-sm text-slate-700">
            <input
              v-model="serviceForm.verify_ssl"
              type="checkbox"
              class="h-4 w-4 rounded border-slate-300"
            />
            Verify SSL
          </label>
          <label class="grid gap-2 md:col-span-2">
            <span class="text-sm font-medium text-slate-700">Teams</span>
            <select
              :class="multiSelectClass"
              multiple
              :value="serviceForm.team_ids.map(String)"
              @change="updateNumericSelections($event, 'service_groups')"
            >
              <option
                v-for="group in metadata?.teams ?? []"
                :key="group.team_id"
                :value="group.team_id"
              >
                {{ group.team_name }}
              </option>
            </select>
          </label>
        </div>
        <div class="flex justify-end gap-2 border-t border-slate-200 px-6 py-4">
          <Button variant="outline" type="button" @click="closeServiceDialog">Cancel</Button>
          <button
            data-testid="service-save"
            type="button"
            class="inline-flex h-9 items-center justify-center gap-2 rounded-md bg-slate-900 px-4 py-2 text-sm font-medium text-white shadow hover:bg-slate-800 disabled:pointer-events-none disabled:opacity-50"
            :disabled="serviceSaving"
            @click.stop.prevent="submitService"
          >
            <Database class="h-4 w-4" />
            {{ serviceSaving ? 'Saving...' : 'Save' }}
          </button>
        </div>
      </form>
    </div>

    <div
      v-if="isAgentDialogOpen"
      class="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/45 px-4 py-6"
      @click.self="closeAgentDialog"
    >
      <div class="max-h-[90vh] w-full max-w-2xl overflow-y-auto rounded-lg bg-white shadow-xl">
        <div class="flex items-start justify-between border-b border-slate-200 px-6 py-4">
          <h3 class="text-lg font-semibold text-slate-900">
            {{
              createdAgent ? 'Install Agent' : agentTargetNodeId ? 'Install Agent' : 'Add New Node'
            }}
          </h3>
          <Button variant="ghost" size="icon" type="button" @click="closeAgentDialog">
            <X class="h-4 w-4" />
          </Button>
        </div>
        <form
          v-if="!createdAgent"
          class="grid gap-5 px-6 py-5"
          @submit.prevent="void submitAgent()"
        >
          <p
            v-if="agentFormError"
            class="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
          >
            {{ agentFormError }}
          </p>
          <label class="grid gap-2">
            <span class="text-sm font-medium text-slate-700">Node Name</span>
            <Input
              v-model="agentForm.node_name"
              required
              :readonly="Boolean(agentTargetNodeId)"
              placeholder="prod-db-node-01"
            />
          </label>
          <label class="flex items-center gap-2 text-sm font-medium text-slate-700">
            <input
              v-model="agentForm.monitoring_enabled"
              type="checkbox"
              class="h-4 w-4 rounded border-slate-300"
            />
            Enable monitoring
          </label>
          <details
            class="rounded-lg border border-slate-200 bg-slate-50 px-4 py-3"
            :class="{ 'opacity-60': !agentForm.monitoring_enabled }"
          >
            <summary class="cursor-pointer text-sm font-medium text-slate-800">
              Advanced collectors
              <span class="ml-2 text-xs font-normal text-slate-500">
                {{ agentForm.monitoring_collectors.length }} selected
              </span>
            </summary>
            <div class="mt-4 grid gap-4">
              <div class="flex flex-wrap gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  type="button"
                  :disabled="!agentForm.monitoring_enabled"
                  @click="selectAllMonitoringCollectors(agentForm)"
                >
                  Select all
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  type="button"
                  :disabled="!agentForm.monitoring_enabled"
                  @click="resetDefaultMonitoringCollectors(agentForm)"
                >
                  Reset defaults
                </Button>
              </div>
              <div class="grid max-h-56 gap-2 overflow-y-auto pr-1 sm:grid-cols-2 md:grid-cols-3">
                <label
                  v-for="collector in DEFAULT_NODE_EXPORTER_COLLECTORS"
                  :key="collector"
                  class="flex min-w-0 items-center gap-2 rounded-md bg-white px-2 py-1.5 text-sm text-slate-700"
                >
                  <input
                    :checked="isMonitoringCollectorSelected(agentForm, collector)"
                    :disabled="!agentForm.monitoring_enabled"
                    type="checkbox"
                    class="h-4 w-4 rounded border-slate-300"
                    @change="toggleMonitoringCollector(agentForm, collector, $event)"
                  />
                  <span class="truncate">{{ collector }}</span>
                </label>
              </div>
            </div>
          </details>
          <div class="flex justify-end gap-2 border-t border-slate-200 pt-4">
            <Button variant="outline" type="button" @click="closeAgentDialog">Cancel</Button>
            <Button type="submit" :disabled="agentSaving">
              {{ agentTargetNodeId ? 'Create Agent Key' : 'Create Node and Key' }}
            </Button>
          </div>
        </form>
        <div v-else class="grid gap-5 px-6 py-5">
          <div
            class="rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800"
          >
            An agent API key was created. The full key is shown once.
          </div>
          <p class="text-sm text-slate-600">
            Run the install command on the node. Creating new install instructions rotates the agent
            key and invalidates the previous key.
          </p>
          <div class="grid gap-1 rounded-lg border border-slate-200 bg-slate-50 px-4 py-3 text-sm">
            <span class="font-medium text-slate-700">Key backend</span>
            <span class="text-slate-600">{{ createdAgent.api_key_backend }}</span>
          </div>
          <div class="grid gap-2">
            <div class="flex items-center justify-between">
              <span class="text-sm font-medium text-slate-700">Agent API Key</span>
              <Button
                variant="outline"
                size="sm"
                type="button"
                @click="void copyText(createdAgent.api_key)"
                >Copy</Button
              >
            </div>
            <textarea :class="fieldClass" rows="3" readonly :value="createdAgent.api_key" />
          </div>
          <div class="grid gap-2">
            <div class="flex items-center justify-between">
              <span class="text-sm font-medium text-slate-700">Install Command</span>
              <Button
                variant="outline"
                size="sm"
                type="button"
                @click="void copyText(createdAgent.install_command)"
                >Copy</Button
              >
            </div>
            <textarea :class="fieldClass" rows="5" readonly :value="createdAgent.install_command" />
          </div>
          <div class="flex justify-end border-t border-slate-200 pt-4">
            <Button type="button" @click="closeAgentDialog">Done</Button>
          </div>
        </div>
      </div>
    </div>
  </section>
</template>
