import { createHash } from "node:crypto";
import { mkdtemp, readFile, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, relative } from "node:path";
import { spawnSync } from "node:child_process";
import { afterAll, describe, expect, it } from "vitest";

const temporaryDirectories: string[] = [];

afterAll(async () => {
  await Promise.all(temporaryDirectories.map((directory) => rm(directory, { recursive: true, force: true })));
});

async function files(root: string, directory = root): Promise<string[]> {
  const entries = await readdir(directory, { withFileTypes: true });
  const nested = await Promise.all(entries.map(async (entry) => {
    const path = join(directory, entry.name);
    return entry.isDirectory() ? files(root, path) : [relative(root, path)];
  }));
  return nested.flat().sort();
}

async function buildSnapshot(): Promise<{ directory: string; hashes: Record<string, string> }> {
  const directory = await mkdtemp(join(tmpdir(), "relayhub-admin-build-"));
  temporaryDirectories.push(directory);
  const result = spawnSync(process.execPath, ["./node_modules/vite/bin/vite.js", "build", "--emptyOutDir", "--outDir", directory], {
    cwd: process.cwd(),
    encoding: "utf8",
  });
  expect(result.status, result.stderr || result.stdout).toBe(0);
  const hashes: Record<string, string> = {};
  for (const name of await files(directory)) {
    hashes[name] = createHash("sha256").update(await readFile(join(directory, name))).digest("hex");
  }
  return { directory, hashes };
}

describe("production build contract", () => {
  it("emits local /admin assets and a manifest", async () => {
    const { directory } = await buildSnapshot();
    const index = await readFile(join(directory, "index.html"), "utf8");
    const manifest = JSON.parse(await readFile(join(directory, ".vite/manifest.json"), "utf8")) as Record<string, unknown>;
    expect(index).toContain('src="/admin/assets/');
    expect(index).toContain('href="/admin/assets/');
    expect(Object.keys(manifest)).toContain("index.html");
    expect(index).not.toMatch(/https?:\/\//);
  });

  it("is byte deterministic", async () => {
    const first = await buildSnapshot();
    const second = await buildSnapshot();
    expect(second.hashes).toEqual(first.hashes);
  });
});
