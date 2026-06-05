import type { Row } from "./data";

export const number = (value: number, digits = 1) =>
  new Intl.NumberFormat("pt-BR", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  }).format(value);

export const compact = (value: number) =>
  new Intl.NumberFormat("pt-BR", {
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value);

export const month = (date: string) =>
  new Intl.DateTimeFormat("pt-BR", { month: "short", year: "numeric", timeZone: "UTC" }).format(
    new Date(date),
  );

export const quarter = (date: string) => {
  const parsed = new Date(date);
  return `${Math.floor(parsed.getUTCMonth() / 3) + 1}T${parsed.getUTCFullYear()}`;
};

export function lastValue(rows: Row[], key: string): { value: number; date: string } | null {
  for (let index = rows.length - 1; index >= 0; index -= 1) {
    const value = rows[index][key];
    const date = rows[index].date ?? rows[index].Date;
    if (typeof value === "number" && typeof date === "string") return { value, date };
  }
  return null;
}
