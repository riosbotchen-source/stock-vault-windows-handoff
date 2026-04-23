import fs from "node:fs/promises";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { validateSnapshotContract } from "./openclaw-snapshot-contract-validator.mjs";

const projectRoot = new URL("..", import.meta.url).pathname.replace(/\/$/, "");
const sampleDir = path.join(projectRoot, "samples");

async function readJson(fileName) {
  const raw = await fs.readFile(path.join(sampleDir, fileName), "utf-8");
  return JSON.parse(raw);
}

describe("openclaw snapshot contract validator", () => {
  it("sample snapshot 在新鮮度範圍內時可通過", async () => {
    const manifest = await readJson("openclaw-manifest.sample.json");
    const reportSnapshot = await readJson("openclaw-report-snapshot.sample.json");

    const result = validateSnapshotContract(
      {
        manifest,
        reportSnapshot,
        manifestPath: path.join(sampleDir, "openclaw-manifest.sample.json"),
        reportPath: path.join(sampleDir, "openclaw-report-snapshot.sample.json"),
      },
      {
        nowIso: "2026-03-21T00:20:00.000Z",
        maxAgeMinutes: 30,
      },
    );

    expect(result.ok).toBe(true);
    expect(result.errors).toHaveLength(0);
    expect(result.checks.readyForReport.ok).toBe(true);
    expect(result.checks.freshness.ok).toBe(true);
  });

  it("readyForReport=false 時會 fail closed", async () => {
    const manifest = await readJson("openclaw-manifest.sample.json");
    const reportSnapshot = await readJson("openclaw-report-snapshot.sample.json");
    reportSnapshot.report.reportIntegrationCheck.summary.readyForReport = false;

    const result = validateSnapshotContract(
      {
        manifest,
        reportSnapshot,
      },
      {
        nowIso: "2026-03-21T00:20:00.000Z",
      },
    );

    expect(result.ok).toBe(false);
    expect(result.errors.some((error) => error.code === "READY_FOR_REPORT_FALSE")).toBe(true);
  });

  it("top5Holdings 超過 5 筆時會被視為筆數不合理", async () => {
    const manifest = await readJson("openclaw-manifest.sample.json");
    const reportSnapshot = await readJson("openclaw-report-snapshot.sample.json");
    reportSnapshot.report.top5Holdings = [
      { symbol: "A" },
      { symbol: "B" },
      { symbol: "C" },
      { symbol: "D" },
      { symbol: "E" },
      { symbol: "F" },
    ];

    const result = validateSnapshotContract(
      {
        manifest,
        reportSnapshot,
      },
      {
        nowIso: "2026-03-21T00:20:00.000Z",
      },
    );

    expect(result.ok).toBe(false);
    expect(result.errors.some((error) => error.code === "TOP5_COUNT_OUT_OF_RANGE")).toBe(true);
    expect(result.checks.countReasonableness.ok).toBe(false);
  });
});
