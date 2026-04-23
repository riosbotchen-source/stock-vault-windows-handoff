# 股市亨通 (Stock Vault)

這是一份對外公開的 GitHub 交接版本，定位為股市亨通的 Windows-first 公開 handoff repo。

台灣股市專屬的本地桌面記帳工具。直接串接 TWSE / TPEX、支援 TDCC 對帳單匯入、FIFO 精確配對。資料存在自己電腦，不上雲。Windows 綠色版免安裝。

[返回首頁](../README.md) | [English Version](./README.en.md)

## 專案定位

此 repo 主要用來：

- 對外展示目前可公開的專案資訊
- 提供 Windows 版本相關交接內容
- 作為開源分享入口
- 保留未來擴充 macOS 版本的空間

## 目前版本狀態

| 項目 | 狀態 |
| --- | --- |
| Windows 版本 | 目前可提供 |
| macOS 版本 | 目前未隨此公開包提供 |
| 後續 macOS 支援 | 如有需要可後續提供 |
| 開源狀態 | 已公開 |
| 授權 | MIT |

## 目前公開內容

這份 repo 目前公開的是可分享的 handoff 內容，不是完整跨平台原始碼全集。

目前包含：

- 專案對外說明文件
- MIT 授權檔
- 部分測試檔案
- 代表性的 Tauri / Rust 核心檔案

## 平台說明

### Windows

- 目前請以 Windows 版本資訊為準
- 這是此公開 repo 的主要平台定位
- 對外介紹、分享、交接都以 Windows 版優先

### macOS

- 目前不在這份公開包內
- 後續如有需要，可以再補充 macOS 版本相關內容
- 若要擴充，建議同步補上建置、簽章與平台差異說明

更細的結構預留請參考：

- [平台總覽](./platforms/README.md)
- [Windows 頁面](./platforms/windows.md)
- [macOS 頁面](./platforms/macos.md)

## 協作建議

如果之後要補齊 macOS 版本，建議同時整理：

- macOS 建置說明
- Tauri / Xcode 相關配置
- 平台差異與功能差異說明
- 發佈與簽章流程文件

## 重要說明

這份公開 repo 的目標是清楚、誠實地呈現目前可公開的內容。

因此目前會明確標示：

- 現階段以 Windows 版本為主
- macOS 版本可後續提供
- 目前公開內容屬於 handoff package

## 授權

本 repo 採用 [MIT License](../LICENSE)。
