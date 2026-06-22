export const ALLAUTH_HEADLESS_CLIENT = 'app'

export function allauthHeadlessPath(path: string): string {
  const normalizedPath = path.startsWith('/') ? path : `/${path}`
  return `/_allauth/${ALLAUTH_HEADLESS_CLIENT}/v1${normalizedPath}`
}
