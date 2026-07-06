import { describe, expect, it } from "vite-plus/test";

import { stripShellWrapper } from "./shellWrapper";

describe("stripShellWrapper", () => {
  it("strips common POSIX shell -lc wrappers", () => {
    expect(stripShellWrapper("/bin/zsh -lc 'pnpm test'")).toBe("pnpm test");
    expect(stripShellWrapper('/bin/bash -lc "pnpm test"')).toBe("pnpm test");
    expect(stripShellWrapper("bash -lc pnpm test")).toBe("pnpm test");
  });

  it("strips common PowerShell command wrappers", () => {
    expect(stripShellWrapper('pwsh -NoProfile -Command "pnpm test"')).toBe("pnpm test");
    expect(stripShellWrapper("powershell.exe -Command 'pnpm test'")).toBe("pnpm test");
  });

  it("leaves ordinary commands unchanged", () => {
    expect(stripShellWrapper("pnpm test")).toBe("pnpm test");
  });
});
