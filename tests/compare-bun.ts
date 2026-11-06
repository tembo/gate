// Optional differential check: bun tests/compare-bun.ts
// The same fixtures run through Gate in tests/gateway.rs.
import { readFileSync } from 'node:fs';

const fixtures = JSON.parse(
  readFileSync(new URL('./fixtures/code-compat.json', import.meta.url), 'utf8')
) as { name: string; code: string; expected: unknown; bunExpected?: unknown }[];

console.log(`Bun ${Bun.version}`);
for (const fixture of fixtures) {
  const source = `export default async function run() {\n${fixture.code}\n}`;
  const javascript = new Bun.Transpiler({ loader: 'ts' }).transformSync(source);
  const module = await import(
    `data:text/javascript;base64,${Buffer.from(javascript).toString('base64')}`
  );
  const actual = JSON.stringify(await module.default());
  if (actual === JSON.stringify(fixture.expected)) {
    console.log(`PASS ${fixture.name}`);
  } else if ('bunExpected' in fixture && actual === JSON.stringify(fixture.bunExpected)) {
    console.log(`KNOWN BUN DIFFERENCE ${fixture.name}: ${actual}`);
  } else {
    throw new Error(`${fixture.name}: ${actual}`);
  }
}
