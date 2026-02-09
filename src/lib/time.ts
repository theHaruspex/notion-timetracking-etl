export function utcDateStamp(input: Date = new Date()): string {
  return input.toISOString().slice(0, 10);
}
