# GitHub Actions — Windows Portable Build

這個 workflow 會在 GitHub 的 `windows-latest` runner 上幫你打包「股市亨通」Windows 綠色版。

## 一次性設定(照做就好)

1. 把整個專案資料夾放到一個 GitHub repo(可以是 private)。
   - `.github/workflows/build-windows-portable.yml` 要跟其他程式碼一起 push 上去。
2. 第一次 push 到 `main` 或 `master` 之後,到 repo 的 **Actions** 分頁,你會看到 `Build Windows Portable` 這個 workflow 開始跑。
3. 首次 build 大約 **8–15 分鐘**(要下載 + 編譯所有 Rust 相依)。之後因為有 Cargo cache,通常 **3–6 分鐘** 就能跑完。

## 三種觸發方式

- **Push 到 main / master** → 自動 build 一次。
- **手動觸發** → 到 Actions 頁面 → 左邊點 `Build Windows Portable` → 右上角 `Run workflow`。隨時可以重打包。
- **Push 版本 tag**(例如 `git tag v1.7.2 && git push --tags`)→ 除了 build,還會自動建 GitHub Release 並把 zip 附上去,方便發佈。

## 成品在哪裡拿

Build 成功後,到那次 workflow run 的頁面最下方 **Artifacts** 區:

- `stock-vault-windows-portable-zip` → 直接可以解壓執行的綠色版 zip(推薦拿這個)。
- `stock-vault-windows-portable-folder` → 同樣內容但未壓縮,想直接看資料夾結構可以看這個。

解壓之後長這樣:

```
windows-portable/
├─ 股市亨通.exe              ← 雙擊就開
├─ WebView2Loader.dll       ← Tauri runtime 需要
├─ stock_vault_lib.dll      ← (若產生)
├─ data/                    ← SQLite DB 存這
├─ snapshots/               ← OpenClaw JSON 存這
├─ backups/                 ← 完整備份存這
├─ logs/                    ← 執行記錄
└─ README-Windows.txt
```

## Windows 使用者需要先裝什麼?

理論上只需要 **WebView2 Runtime**,Windows 11 跟近期的 Windows 10 都預載了。如果目標機器是老舊 Windows 10,萬一跑不起來,去微軟官網下載 Evergreen Bootstrapper 裝一次就好(免費、一次性)。

## 改成同時產出安裝檔(選用)

預設為了省時間只產出 raw exe(`--no-bundle`)。如果之後要做正式發佈版、要附 NSIS / MSI 安裝檔,把 workflow 裡這一行:

```yaml
run: npx tauri build --config src-tauri/tauri.windows.conf.json --no-bundle
```

換成:

```yaml
run: npm run tauri:build:windows
```

即可。runner 會多花 2–5 分鐘下載 WiX 工具並產出安裝檔,你可以再加一步把 `src-tauri/target/release/bundle/` 也傳成 artifact。

## 常見失敗怎麼看

到 Actions 頁面點進失敗那次 run,展開紅色的那個 step 看 log。最容易卡住的幾個:

- **`Handoff sanity check` 失敗** → 代表你不小心把 History DB / Forecast 相關的程式碼放回去了,按錯誤訊息刪掉對應檔即可。
- **`Build Tauri Windows exe` 失敗** → 99% 是 Rust 端的編譯錯誤,log 會直接指出哪個 crate、哪一行。常見情況是相依套件版本衝突,解掉 `src-tauri/Cargo.lock` 重新 push 通常能解。
- **`Prepare portable folder structure` 找不到 exe** → workflow 裡我有加 debug 列目錄的步驟,直接看那份 tree,99% 是 tauri 的輸出路徑被 config 改過,照實際路徑改 workflow 裡的 `$exe` 變數即可。
