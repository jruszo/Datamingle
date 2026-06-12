export type LabelFilterMode = 'include' | 'exclude'

export type LabelFilter = {
  label: string
  mode: LabelFilterMode
  values: string[]
}
