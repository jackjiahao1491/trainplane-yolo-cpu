import { readFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { dirname, resolve } from 'node:path'
import ts from 'typescript'

// Compile the actual source for Node versions that do not strip TypeScript.
export function frontendLoader() {
  const cache = new Map()
  function load(filename) {
    if (cache.has(filename)) return cache.get(filename).exports
    const source = readFileSync(filename, 'utf8')
    const compiled = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText
    const module = { exports: {} }
    cache.set(filename, module)
    const require = createRequire(pathToFileURL(filename))
    const requireSource = specifier => specifier.startsWith('.')
      ? load(resolve(dirname(filename), `${specifier}.ts`)) : require(specifier)
    new Function('require', 'module', 'exports', compiled)(requireSource, module, module.exports)
    return module.exports
  }
  return relative => load(fileURLToPath(new URL(relative, import.meta.url)))
}

export const loadFrontend = relative => frontendLoader()(relative)
