import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import { createRequire } from 'node:module';
import path from 'node:path';
import ts from 'typescript';

// Explicit offline qualification against the actual shared API source/dependency.
// Regular Broker tests need neither an API checkout nor an API dependency tree.
export const sharedAssignmentParserConfigured = Boolean(process.env.BROKER_API_ASSIGNMENT_SOURCE
  || process.env.BROKER_API_DEPENDENCY_ROOT);

export function loadSharedAssignmentParser() {
  const sourcePath = process.env.BROKER_API_ASSIGNMENT_SOURCE;
  const dependencyRoot = process.env.BROKER_API_DEPENDENCY_ROOT;
  assert.ok(sourcePath && dependencyRoot, 'Both shared parser qualification paths are required');
  const source = fs.readFileSync(sourcePath, 'utf8');
  const blob = createHash('sha1').update(`blob ${Buffer.byteLength(source)}\0`).update(source).digest('hex');
  assert.equal(blob, '2ee5eafbc0819b69664e8bc0c4cf3e3e6600a8e5', 'Exact API 4f00cd9 parser source');
  const ast = ts.createSourceFile('phoneTaskContext.ts', source, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TS);
  const names = ['uuid', 'phoneCallAssignmentSchema', 'phoneCallAssignmentRefSchema'];
  const declarations = ast.statements.filter(statement => ts.isVariableStatement(statement)
    && statement.declarationList.declarations.some(declaration => ts.isIdentifier(declaration.name)
      && names.includes(declaration.name.text)));
  assert.equal(declarations.length, names.length);
  const apiRequire = createRequire(path.join(dependencyRoot, 'package.json'));
  const zodVersion = apiRequire('zod/package.json').version as string;
  assert.match(zodVersion, /^3\./, 'Use the API Zod 3 dependency, not Broker Zod 4');
  // Preserve the exact schema declaration bytes. Only imports and unrelated
  // runtime code are excluded; no copied or rewritten validation predicates.
  const code = ts.transpileModule(declarations.map(statement => statement.getText(ast)).join('\n'), {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS },
  }).outputText;
  const exports: Record<string, { safeParse(value: unknown): { success: boolean }; parse(value: unknown): unknown }> = {};
  new Function('z', 'exports', code)(apiRequire('zod').z, exports);
  return { phoneCallAssignmentSchema: exports.phoneCallAssignmentSchema,
    phoneCallAssignmentRefSchema: exports.phoneCallAssignmentRefSchema, zodVersion, blob };
}
