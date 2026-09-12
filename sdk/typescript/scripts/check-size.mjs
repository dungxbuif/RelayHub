import { readdir, readFile } from "node:fs/promises";
async function size(directory) {
  let total = 0;
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = new URL(`${entry.name}${entry.isDirectory() ? "/" : ""}`, directory);
    total += entry.isDirectory() ? await size(path) : (await readFile(path)).byteLength;
  }
  return total;
}
const bytes = await size(new URL("../dist/", import.meta.url));
if (bytes > 150_000) throw new Error(`SDK build ${bytes} bytes exceeds 150000-byte budget`);
console.log(`SDK build size: ${bytes} bytes`);
