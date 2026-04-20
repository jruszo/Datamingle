import { reactive } from 'vue'

export function useListQueryState<T extends Record<string, unknown>>(initialState: T) {
  return reactive({
    ...initialState,
  }) as T
}
