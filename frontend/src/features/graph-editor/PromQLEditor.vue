<script setup lang="ts">
import { nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { EditorState } from '@codemirror/state'
import { EditorView, keymap } from '@codemirror/view'
import { basicSetup } from 'codemirror'
import { PromQLExtension, type PrometheusClient } from '@prometheus-io/codemirror-promql'

import {
  fetchMetricLabelNames,
  fetchMetricLabelValues,
  fetchMetricMetadata,
  fetchMetricNames,
  fetchMetricSeries,
} from '@/features/metrics/api'

const props = defineProps<{
  modelValue: string
  token: string
}>()

const emit = defineEmits<{
  'update:modelValue': [value: string]
  run: []
}>()

const editorRoot = ref<HTMLDivElement | null>(null)
let editorView: EditorView | null = null
let promqlExtension: PromQLExtension | null = null

function labelsFromSeries(series: Array<Record<string, string>>) {
  return [
    ...new Set(series.flatMap((item) => Object.keys(item)).filter((label) => label !== '__name__')),
  ].sort()
}

function createPromqlClient(): PrometheusClient {
  return {
    labelNames: async (metricName?: string) =>
      metricName
        ? labelsFromSeries(await fetchMetricSeries(metricName, props.token))
        : fetchMetricLabelNames(props.token),
    labelValues: async (labelName: string, metricName?: string) => {
      if (!metricName) {
        return fetchMetricLabelValues(labelName, props.token)
      }
      const series = await fetchMetricSeries(metricName, props.token)
      return [
        ...new Set(
          series
            .map((item) => item[labelName])
            .filter((value): value is string => Boolean(value)),
        ),
      ].sort()
    },
    metricMetadata: async () => {
      const metadata = await fetchMetricMetadata('', props.token)
      return Object.fromEntries(
        Object.entries(metadata).map(([name, entries]) => [
          name,
          entries.map((entry) => ({
            type: entry.type ?? '',
            help: entry.help ?? '',
          })),
        ]),
      )
    },
    series: async (metricName: string) =>
      (await fetchMetricSeries(metricName || '{__name__!=""}', props.token)).map(
        (item) => new Map(Object.entries(item)),
      ),
    metricNames: async (prefix?: string) => fetchMetricNames(props.token, prefix ?? '', 300),
    flags: async () => ({}),
  }
}

function createEditor() {
  if (!editorRoot.value || editorView) {
    return
  }
  promqlExtension = new PromQLExtension().setComplete({ remote: createPromqlClient() })
  editorView = new EditorView({
    parent: editorRoot.value,
    state: EditorState.create({
      doc: props.modelValue,
      extensions: [
        basicSetup,
        promqlExtension.asExtension(),
        EditorView.lineWrapping,
        keymap.of([
          {
            key: 'Mod-Enter',
            run: () => {
              emit('run')
              return true
            },
          },
        ]),
        EditorView.updateListener.of((update) => {
          if (update.docChanged) {
            emit('update:modelValue', update.state.doc.toString())
          }
        }),
        EditorView.theme({
          '&': {
            minHeight: '8rem',
            border: '1px solid #cbd5e1',
            borderRadius: '0.375rem',
            backgroundColor: '#ffffff',
            fontSize: '0.875rem',
          },
          '&.cm-focused': {
            outline: '2px solid #94a3b8',
            outlineOffset: '-1px',
          },
          '.cm-scroller': {
            minHeight: '8rem',
            fontFamily:
              'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace',
          },
          '.cm-content': { padding: '0.75rem' },
          '.cm-tooltip': {
            border: '1px solid #cbd5e1',
            borderRadius: '0.375rem',
            boxShadow: '0 10px 25px rgb(15 23 42 / 0.16)',
          },
        }),
      ],
    }),
  })
}

onMounted(() => {
  void nextTick(createEditor)
})

onBeforeUnmount(() => {
  promqlExtension?.destroy()
  editorView?.destroy()
})

watch(
  () => props.modelValue,
  (value) => {
    if (!editorView) {
      return
    }
    const current = editorView.state.doc.toString()
    if (current !== value) {
      editorView.dispatch({ changes: { from: 0, to: current.length, insert: value } })
    }
  },
)
</script>

<template>
  <div ref="editorRoot" />
</template>
