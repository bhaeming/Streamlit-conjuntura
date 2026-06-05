import fs from "node:fs";
import path from "node:path";

export type Row = Record<string, string | number | null>;

export function loadData(name: string): Row[] {
  const file = path.join(process.cwd(), "public", "data", `${name}.json`);
  return JSON.parse(fs.readFileSync(file, "utf8")) as Row[];
}
