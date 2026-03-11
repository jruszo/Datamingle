export type QueryMetadataNodeKind = 'database' | 'schema' | 'table'

export type QueryMetadataTableColumn = {
  name: string
  type: string
  details: string
}

export type QueryMetadataTableIndex = {
  name: string
  type: string
  columns: string
}

export type QueryMetadataTableDetails = {
  nodeId: string
  columns: QueryMetadataTableColumn[]
  indexes: QueryMetadataTableIndex[]
}

export type QueryMetadataTableDetailsMap = Record<string, QueryMetadataTableDetails>

export type QueryMetadataNode = {
  id: string
  kind: QueryMetadataNodeKind
  name: string
  dbName: string
  schemaName: string
  children: QueryMetadataNode[]
  isExpanded: boolean
  isLoading: boolean
  isLoaded: boolean
}
