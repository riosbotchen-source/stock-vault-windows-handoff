import fs from "node:fs/promises";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { buildReportMarkdown } from "./openclaw-generate-report.mjs";

const projectRoot = new URL("..", import.meta.url).pathname.replace(/\/$/, "");
const sampleDir = path.join(projectRoot, "samples");

async function readJson(fileName) {
  const raw = await fs.readFile(path.join(sampleDir, fileName), "utf-8");
  return JSON.parse(raw);
}

describe("openclaw performance report template guards", () => {
  it("月/年績效資料不足時使用保守語句，不硬下完整結論", async () => {
    const manifest = await readJson("openclaw-manifest.sample.json");
    const snapshot = await readJson("openclaw-report-snapshot.sample.json");
    const markdown = buildReportMarkdown("測試報告", manifest, snapshot);

    expect(markdown).toContain("本月績效");
    expect(markdown).toContain("今年績效");
    expect(markdown).toContain("期間總損益資料不足（total_pnl 不可算）");
    expect(markdown).toContain("XIRR：不可算");
    expect(markdown).toContain("已完整覆蓋標的");
  });

  it("保留 performanceSummary 與 performanceV14 共存，不依賴單一路徑", async () => {
    const manifest = await readJson("openclaw-manifest.sample.json");
    const snapshot = await readJson("openclaw-report-snapshot.sample.json");
    expect(snapshot.report.performanceSummary).toBeDefined();
    expect(snapshot.report.performanceV14).toBeDefined();

    const markdown = buildReportMarkdown("測試報告", manifest, snapshot);
    expect(markdown).toContain("投資總覽");
    expect(markdown).toContain("schemaVersion");
  });

  it("有 contribution explainability 時，報告會輸出排序說明句", async () => {
    const manifest = await readJson("openclaw-manifest.sample.json");
    const snapshot = await readJson("openclaw-report-snapshot.sample.json");
    snapshot.report.performanceV14.contribution_explainability = {
      monthly_latest: {
        ranking_status: "PARTIAL",
        explanation_sentence: "本期可排序 3 檔，另有 2 檔因資料不足未納入排序。",
        ranking_data_quality_flags: ["PARTIAL_DATA_COVERAGE"],
      },
      yearly_latest: null,
    };

    const markdown = buildReportMarkdown("測試報告", manifest, snapshot);
    expect(markdown).toContain("貢獻排序說明");
    expect(markdown).toContain("本期可排序 3 檔");
    expect(markdown).toContain("PARTIAL_DATA_COVERAGE");
  });
});
