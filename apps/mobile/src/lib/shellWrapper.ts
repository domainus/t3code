export function stripShellWrapper(value: string): string {
  const trimmed = value.trim();
  const shellMatch = trimmed.match(/^(?:(?:\/usr)?\/bin\/)?(?:zsh|bash|sh)\s+-lc\s+([\s\S]+)$/);
  if (shellMatch?.[1]) {
    return stripMatchingQuotes(shellMatch[1]).trim();
  }
  const powershellMatch = trimmed.match(/^(?:pwsh|powershell)(?:\.exe)?(?:\s+-NoProfile)?\s+-Command\s+([\s\S]+)$/i);
  if (powershellMatch?.[1]) {
    return stripMatchingQuotes(powershellMatch[1]).trim();
  }
  return trimmed;
}

function stripMatchingQuotes(value: string): string {
  const trimmed = value.trim();
  if (trimmed.length < 2) return trimmed;
  const first = trimmed[0];
  const last = trimmed[trimmed.length - 1];
  if ((first === "'" && last === "'") || (first === '"' && last === '"')) {
    return trimmed.slice(1, -1);
  }
  return trimmed;
}
