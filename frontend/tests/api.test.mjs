import assert from 'node:assert/strict'
import test from 'node:test'
import { readFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import ts from 'typescript'

test('session token travels only in Authorization; failed authentication clears the session', async () => {
  const values = new Map()
  Object.defineProperty(globalThis, 'sessionStorage', { configurable: true, value: {
    getItem: key => values.get(key) ?? null,
    setItem: (key, value) => values.set(key, value),
    removeItem: key => values.delete(key),
  } })
  globalThis.window = { setTimeout, clearTimeout }
  const originalFetch = globalThis.fetch
  const requests = []
  globalThis.fetch = async (url, options) => {
    requests.push({ url, options })
    return new Response(JSON.stringify(url.endsWith('/auth/session') ? { authenticated: true, username: 'operator' } : { total_cpus: 2 }), { status: 200 })
  }
  try {
    const source = readFileSync(new URL('../src/api.ts', import.meta.url), 'utf8')
    const compiled = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText
    const module = { exports: {} }
    new Function('require', 'module', 'exports', compiled)(createRequire(import.meta.url), module, module.exports)
    const { signIn, signOut, restoreSession, api, session } = module.exports
    await signIn('  frontend-unit-test-token  ')
    assert.equal(session.authenticated, true)
    assert.equal(session.username, 'operator')
    assert.equal(values.get('trainplane.access-token'), 'frontend-unit-test-token')
    await api('/cluster')
    for (const request of requests) {
      assert.equal(request.options.headers.get('Authorization'), 'Bearer frontend-unit-test-token')
      assert.equal(request.url.includes('token'), false)
    }
    session.authenticated = false
    await restoreSession()
    assert.equal(session.authenticated, true)
    let finishOldRequest
    globalThis.fetch = () => new Promise(resolve => { finishOldRequest = resolve })
    const lateFailure = api('/jobs')
    globalThis.fetch = async () => new Response(JSON.stringify({ authenticated: true, username: 'operator' }), { status: 200 })
    await signIn('replacement-unit-test-token')
    finishOldRequest(new Response('{}', { status: 401 }))
    await assert.rejects(lateFailure)
    assert.equal(session.authenticated, true)
    assert.equal(values.get('trainplane.access-token'), 'replacement-unit-test-token')
    globalThis.fetch = async () => new Response(JSON.stringify({ detail: 'frontend-unit-test-token' }), { status: 401 })
    await assert.rejects(api('/cluster'), error => error.message.includes('重新登录') && !error.message.includes('frontend-unit-test-token'))
    assert.equal(session.authenticated, false)
    assert.equal(values.size, 0)
    signOut()
  } finally { globalThis.fetch = originalFetch }
})
