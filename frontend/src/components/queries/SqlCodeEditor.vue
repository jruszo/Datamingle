<script setup lang="ts">
import { Compartment, EditorSelection, EditorState } from '@codemirror/state'
import { EditorView, keymap, placeholder as editorPlaceholder } from '@codemirror/view'
import { sql, Cassandra, MSSQL, MySQL, PLSQL, PostgreSQL, StandardSQL } from '@codemirror/lang-sql'
import { indentWithTab } from '@codemirror/commands'
import { basicSetup } from 'codemirror'
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'

const props = withDefaults(
  defineProps<{
    modelValue: string
    dbType?: string
    disabled?: boolean
    placeholder?: string
    minHeight?: number
    height?: number
  }>(),
  {
    dbType: '',
    disabled: false,
    placeholder: 'Write a query or paste a statement here...',
    minHeight: 240,
    height: undefined,
  },
)

const emit = defineEmits<{
  'update:modelValue': [value: string]
  submit: []
}>()

const container = ref<HTMLDivElement | null>(null)
const containerHeight = computed(() => `${props.height ?? props.minHeight}px`)

let editorView: EditorView | null = null

const languageCompartment = new Compartment()
const editableCompartment = new Compartment()

function sqlDialect(dbType: string) {
  switch (dbType.toLowerCase()) {
    case 'mysql':
    case 'clickhouse':
    case 'doris':
      return MySQL
    case 'pgsql':
      return PostgreSQL
    case 'oracle':
      return PLSQL
    case 'mssql':
      return MSSQL
    case 'cassandra':
      return Cassandra
    default:
      return StandardSQL
  }
}

function createState(documentValue: string) {
  return EditorState.create({
    doc: documentValue,
    extensions: [
      basicSetup,
      keymap.of([
        {
          key: 'Mod-Enter',
          run: () => {
            emit('submit')
            return true
          },
        },
        indentWithTab,
      ]),
      editorPlaceholder(props.placeholder),
      EditorView.lineWrapping,
      EditorView.theme({
        '&': {
          fontSize: '14px',
          height: '100%',
          minHeight: '100%',
          backgroundColor: '#f8fafc',
          color: '#0f172a',
        },
        '.cm-scroller': {
          fontFamily: 'IBM Plex Mono, ui-monospace, SFMono-Regular, Menlo, monospace',
          height: '100%',
          minHeight: '100%',
          lineHeight: '1.6',
        },
        '.cm-content': {
          padding: '16px',
        },
        '.cm-gutters': {
          backgroundColor: '#eff6ff',
          borderRight: '1px solid #dbeafe',
          color: '#64748b',
        },
        '.cm-activeLine': {
          backgroundColor: 'rgba(219, 234, 254, 0.35)',
        },
        '.cm-activeLineGutter': {
          backgroundColor: '#dbeafe',
        },
        '.cm-focused': {
          outline: 'none',
        },
        '.cm-selectionBackground': {
          backgroundColor: 'rgba(56, 189, 248, 0.24)',
        },
        '.cm-cursor': {
          borderLeftColor: '#0f172a',
        },
      }),
      languageCompartment.of(sql({ dialect: sqlDialect(props.dbType) })),
      editableCompartment.of(EditorView.editable.of(!props.disabled)),
      EditorView.updateListener.of((update) => {
        if (update.docChanged) {
          emit('update:modelValue', update.state.doc.toString())
        }
      }),
    ],
  })
}

function mountEditor(documentValue: string) {
  if (!container.value) {
    return
  }

  editorView?.destroy()
  editorView = new EditorView({
    state: createState(documentValue),
    parent: container.value,
  })
}

function setValue(value: string) {
  if (!editorView) {
    emit('update:modelValue', value)
    return
  }

  const currentValue = editorView.state.doc.toString()
  if (currentValue === value) {
    return
  }

  editorView.dispatch({
    changes: {
      from: 0,
      to: currentValue.length,
      insert: value,
    },
    selection: EditorSelection.cursor(value.length),
  })
}

function getSelectedText() {
  if (!editorView) {
    return ''
  }

  const selection = editorView.state.selection.main
  if (selection.empty) {
    return ''
  }

  return editorView.state.sliceDoc(selection.from, selection.to)
}

function focus() {
  editorView?.focus()
}

function insertText(value: string) {
  if (!editorView) {
    emit('update:modelValue', `${props.modelValue}${value}`)
    return
  }

  const selection = editorView.state.selection.main
  const from = selection.from
  editorView.dispatch({
    changes: {
      from,
      to: selection.to,
      insert: value,
    },
    selection: EditorSelection.cursor(from + value.length),
  })
  editorView.focus()
}

defineExpose({
  getSelectedText,
  setValue,
  focus,
  insertText,
})

onMounted(() => {
  mountEditor(props.modelValue)
})

watch(
  () => props.modelValue,
  (value) => {
    if (!editorView || editorView.state.doc.toString() === value) {
      return
    }
    setValue(value)
  },
)

watch(
  () => props.dbType,
  () => {
    if (!editorView) {
      return
    }
    const value = editorView.state.doc.toString()
    editorView.dispatch({
      effects: languageCompartment.reconfigure(sql({ dialect: sqlDialect(props.dbType) })),
    })
    if (value !== props.modelValue) {
      emit('update:modelValue', value)
    }
  },
)

watch(
  () => props.disabled,
  () => {
    if (!editorView) {
      return
    }
    editorView.dispatch({
      effects: editableCompartment.reconfigure(EditorView.editable.of(!props.disabled)),
    })
  },
)

onBeforeUnmount(() => {
  editorView?.destroy()
})
</script>

<template>
  <div
    ref="container"
    class="overflow-hidden rounded-3xl border border-sky-100 bg-slate-50 shadow-inner"
    :style="{ height: containerHeight }"
  />
</template>
