# 股市亨通 (Stock Vault)

公開版 GitHub handoff repo。

目前這個公開版本以 **Windows 版** 為主，方便外部查看、保存與後續協作。

## 目前版本狀態

- Windows: 目前可提供與維護
- macOS: 目前尚未隨此公開包一起提供
- Mac 版本: 如後續有需要，可以再補提供

## 這個公開 repo 目前包含什麼

這份 repo 目前公開的是可分享的 handoff 內容，包含：

- 授權檔
- 專案說明
- Windows portable GitHub Actions workflow
- 部分測試檔
- 代表性的 Tauri / Rust 核心檔案

這表示它目前是 **Windows 公開交接版本**，不是完整的商業發佈包或完整跨平台原始碼全集。

## Windows 說明

目前對外說明請以 Windows 版本為準。

- 使用定位: Windows 桌面版 / portable 流程
- 公開說明: 目前只有 Windows 版本
- 後續規劃: 若需要 macOS 版本，可再補上對應內容

## GitHub Actions

repo 內包含 Windows portable build workflow：

- `.github/workflows/build-windows-portable.yml`

如果後續補齊完整專案檔案，就可以延續這個 workflow 做 Windows 版建置與發佈。

## 後續如果要補 Mac 版本

後續如需提供 macOS 版本，建議一起補上：

- macOS 專用建置說明
- 對應的 Tauri / Xcode 配置
- Mac 發佈或簽章流程
- 與 Windows 版差異說明

## License

本 repo 目前公開內容採用 [MIT](./LICENSE)。
