use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::fs::File;
use std::io::Cursor;
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use base64::{engine::general_purpose, Engine as _};
use chrono::{Datelike, Duration as ChronoDuration, NaiveDate, Utc, Weekday};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use reqwest::blocking::Client;
use rusqlite::types::ValueRef;
use rusqlite::{params, Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha384};
use tar::{Archive, Builder};
use tauri::{AppHandle, Manager};
use tauri_plugin_sql::{Migration, MigrationKind};

const SETTINGS_BACKUP_FORMAT_VERSION: &str = "stock-vault.settings-backup.v1";
const SETTINGS_BACKUP_MANIFEST_ENTRY: &str = "manifest.json";
const SETTINGS_BACKUP_README_ENTRY: &str = "README.txt";
const SETTINGS_BACKUP_SQLITE_ENTRY: &str = "data/stock-vault.db";
const SETTINGS_BACKUP_APP_SETTINGS_ENTRY: &str = "data/app-settings.json";
const SETTINGS_BACKUP_STATUS_FILE: &str = "settings-backup-status.json";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TransactionPayload {
    id: Option<i64>,
    account_id: i64,
    security_id: Option<i64>,
    #[serde(rename = "type")]
    tx_type: String,
    trade_date: String,
    quantity: Option<f64>,
    price: Option<f64>,
    fee: Option<f64>,
    tax: Option<f64>,
    interest: Option<f64>,
    amount: Option<f64>,
    note: Option<String>,
    manual_allocations: Option<Vec<ManualAllocationPayload>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ManualAllocationPayload {
    buy_transaction_id: i64,
    quantity: f64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PricePayload {
    id: Option<i64>,
    security_id: i64,
    price_date: String,
    close_price: f64,
    currency: String,
    source: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PriceImportPayload {
    security_id: i64,
    price_date: String,
    close_price: f64,
    currency: String,
    source: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CashTransactionPayload {
    id: Option<i64>,
    account_id: i64,
    #[serde(rename = "type")]
    cash_type: String,
    trade_date: String,
    amount: f64,
    currency: String,
    note: Option<String>,
    related_transaction_id: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateAccountPayload {
    name: String,
    broker: Option<String>,
    currency: String,
    bank_name: Option<String>,
    bank_branch: Option<String>,
    bank_account_last4: Option<String>,
    settlement_account_note: Option<String>,
    is_primary: Option<bool>,
    note: Option<String>,
    is_archived: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UpdateAccountPayload {
    id: i64,
    name: String,
    broker: Option<String>,
    currency: String,
    bank_name: Option<String>,
    bank_branch: Option<String>,
    bank_account_last4: Option<String>,
    settlement_account_note: Option<String>,
    is_primary: Option<bool>,
    note: Option<String>,
    is_archived: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TdccAccountSeedPayload {
    account_name: String,
    broker_name: String,
    account_no: String,
    source: String,
    source_note: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TdccHoldingSeedPayload {
    account_no: String,
    broker_name: String,
    account_name: String,
    symbol: String,
    name: String,
    quantity: f64,
    snapshot_date: String,
    close_price: Option<f64>,
    market_value: Option<f64>,
    source: String,
    source_pdf: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TdccTransactionDraftPayload {
    account_no: String,
    broker_name: String,
    account_name: String,
    trade_date: String,
    symbol: String,
    name: String,
    side: String,
    quantity: f64,
    running_balance_from_pdf: Option<f64>,
    source_memo: Option<String>,
    source_pdf: Option<String>,
    page_no: Option<i64>,
    source: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TdccSkippedEventPayload {
    account_no: String,
    broker_name: String,
    account_name: String,
    trade_date: Option<String>,
    symbol: Option<String>,
    name: Option<String>,
    memo: Option<String>,
    withdrawal_qty: Option<f64>,
    deposit_qty: Option<f64>,
    running_balance_from_pdf: Option<f64>,
    skip_reason: String,
    source_pdf: Option<String>,
    page_no: Option<i64>,
    source: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TdccIntermediateImportPayload {
    accounts: Vec<TdccAccountSeedPayload>,
    holdings: Vec<TdccHoldingSeedPayload>,
    drafts: Vec<TdccTransactionDraftPayload>,
    skipped_events: Vec<TdccSkippedEventPayload>,
    import_summary_json: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SaveTransactionResult {
    id: i64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ImportTransactionsResult {
    inserted: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DeleteAccountResult {
    deleted: bool,
    blocked_by_transactions: bool,
    transaction_count: i64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DeleteHoldingResult {
    deleted_transaction_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SettingsBackupEntries {
    sqlite_db: String,
    app_settings_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SettingsBackupIncludes {
    sqlite_db: bool,
    app_settings_snapshot: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SettingsBackupSummary {
    app_settings_count: usize,
    account_count: i64,
    security_count: i64,
    transaction_count: i64,
    price_snapshot_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SettingsBackupSource {
    current_db_path: String,
    portable_mode: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SettingsBackupManifest {
    format_version: String,
    created_at: String,
    app_version: String,
    bundle_identifier: String,
    schema_version: String,
    schema_version_number: i64,
    entries: SettingsBackupEntries,
    includes: SettingsBackupIncludes,
    summary: SettingsBackupSummary,
    source: SettingsBackupSource,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SettingsBackupStatusMetadata {
    backup_path: String,
    created_at: String,
    file_size_bytes: u64,
    format_version: String,
    schema_version: String,
    schema_version_number: i64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TdccImportReasonCount {
    reason: String,
    count: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TdccIntermediateImportResult {
    run_key: String,
    accounts_seeded: usize,
    accounts_reused: usize,
    mappings_upserted: usize,
    holdings_seeded: usize,
    draft_imported: usize,
    skipped_logged: usize,
    skipped_by_reason: Vec<TdccImportReasonCount>,
    warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TdccBootstrapBackfillResult {
    executed: bool,
    reason: String,
    accounts_count: usize,
    holdings_count: usize,
    drafts_count: usize,
    skipped_count: usize,
    imported_transactions: usize,
}

#[derive(Debug, Clone)]
struct BootstrapHoldingTarget {
    quantity: f64,
    snapshot_date: String,
    close_price: Option<f64>,
}

#[derive(Debug, Clone)]
struct BootstrapDraftEvent {
    trade_date: String,
    side: String,
    quantity: f64,
    running_balance_from_pdf: Option<f64>,
    source_memo: Option<String>,
    source_pdf: Option<String>,
    page_no: Option<i64>,
    source: String,
    order: usize,
}

#[derive(Debug)]
struct TxRow {
    id: i64,
    tx_type: String,
    quantity: f64,
}

#[derive(Debug)]
struct Lot {
    buy_id: i64,
    remaining_qty: f64,
}

#[derive(Debug)]
struct BuyLotState {
    account_id: i64,
    security_id: i64,
    remaining_qty: f64,
}

#[derive(Debug)]
struct CashFlowMapping {
    cash_type: String,
    amount: f64,
    note: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TwStockMasterRecordPayload {
    symbol: String,
    name: String,
    market: String,
    currency: String,
    board_lot_size: i64,
    category: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TwStockLookupDiagnostic {
    queried: bool,
    url: String,
    http_status: Option<u16>,
    response_preview: String,
    parsed_rows: usize,
    matched: bool,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TwStockLookupResult {
    ok: bool,
    reason_type: Option<String>,
    reason_message: Option<String>,
    record: Option<TwStockMasterRecordPayload>,
    diagnostics: Vec<TwStockLookupDiagnostic>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RepairStockMasterItem {
    security_id: i64,
    symbol: String,
    old_name: String,
    status: String,
    reason: String,
    new_name: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RepairStockMasterResult {
    checked: usize,
    repaired: usize,
    skipped: usize,
    items: Vec<RepairStockMasterItem>,
}

#[derive(Debug, Clone)]
struct TwPriceSyncTarget {
    security_id: i64,
    symbol: String,
    name: String,
    market: String,
    currency: String,
}

#[derive(Debug, Clone)]
struct TwMarketQuote {
    trade_date: String,
    close_price: f64,
    previous_close_price: Option<f64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TwPriceSyncItem {
    security_id: i64,
    symbol: String,
    name: String,
    market: String,
    status: String,
    latest_price_date: Option<String>,
    previous_price_date: Option<String>,
    source: String,
    reason: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TwPriceSyncResult {
    synced_at: String,
    source: String,
    scope: String,
    success_count: usize,
    failed_count: usize,
    missing_count: usize,
    updated_rows: usize,
    updated_through_date: Option<String>,
    benchmark_index: Option<TwBenchmarkIndexSnapshot>,
    dividend_sync: Option<TwDividendSyncSnapshot>,
    items: Vec<TwPriceSyncItem>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct TwDividendSyncSnapshot {
    synced_at: String,
    status: String,
    source: String,
    mode: String,
    start_date: Option<String>,
    end_date: Option<String>,
    upserted_rows: usize,
    cash_upserted_rows: usize,
    stock_upserted_rows: usize,
    matched_events: usize,
    skipped_manual_rows: usize,
    skipped_non_holding_rows: usize,
    skipped_existing_rows: usize,
    tpex_failed: bool,
    tpex_fail_streak: i64,
    warning_count: usize,
    warnings: Vec<String>,
    detected_items: Vec<TwDividendDetectedItem>,
    summary: String,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct TwDividendDetectedItem {
    symbol: String,
    name: String,
    ex_dividend_date: String,
    dividend_type: String,
    cash_dividend_per_share: Option<f64>,
    stock_dividend_per_thousand: Option<f64>,
    estimated_total: Option<f64>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct TwBenchmarkIndexSnapshot {
    name: String,
    latest_close: f64,
    previous_close: f64,
    today_change: f64,
    today_change_percent: f64,
    latest_date: String,
    previous_date: String,
    month_to_date_return: Option<f64>,
    year_to_date_return: Option<f64>,
    source: String,
}

#[derive(Debug, Clone)]
struct TaiexDailyQuote {
    trade_date: String,
    close: f64,
    change_points: Option<f64>,
    change_percent_ratio: Option<f64>,
}

#[derive(Debug, Clone)]
struct TwDividendMarketEvent {
    symbol: String,
    ex_dividend_date: String,
    cash_dividend_per_share: Option<f64>,
    stock_dividend_per_thousand: Option<f64>,
    record_date: Option<String>,
    payment_date: Option<String>,
}

#[derive(Debug, Clone)]
struct AutoDividendUpsertRow {
    account_id: i64,
    security_id: i64,
    dividend_type: String,
    ex_dividend_date: String,
    record_date: Option<String>,
    payment_date: Option<String>,
    cash_dividend_per_share: Option<f64>,
    total_cash_dividend: Option<f64>,
    stock_dividend_per_thousand: Option<f64>,
    total_stock_dividend: Option<f64>,
    holding_quantity_at_ex_date: f64,
    source: String,
    fetched_at: String,
}

#[derive(Debug, Clone, Default)]
struct AutoDividendBuildStats {
    matched_events: usize,
    skipped_manual_rows: usize,
    skipped_non_holding_rows: usize,
    skipped_existing_rows: usize,
    cash_rows: usize,
    stock_rows: usize,
}

fn sqlite_migrations() -> Vec<Migration> {
    vec![
        Migration {
            version: 1,
            description: "create_stock_vault_tables",
            sql: include_str!("../migrations/0001_init.sql"),
            kind: MigrationKind::Up,
        },
        Migration {
            version: 2,
            description: "upgrade_domain_transaction_schema",
            sql: include_str!("../migrations/0002_domain_logic.sql"),
            kind: MigrationKind::Up,
        },
        Migration {
            version: 3,
            description: "canonical_cash_dividend_type",
            sql: include_str!("../migrations/0003_cash_dividend_canonical.sql"),
            kind: MigrationKind::Up,
        },
        Migration {
            version: 4,
            description: "add_security_board_lot_size",
            sql: include_str!("../migrations/0004_security_board_lot_size.sql"),
            kind: MigrationKind::Up,
        },
        Migration {
            version: 5,
            description: "add_security_category_tags",
            sql: include_str!("../migrations/0005_security_category_tags.sql"),
            kind: MigrationKind::Up,
        },
        Migration {
            version: 6,
            description: "add_accounts_management_fields",
            sql: include_str!("../migrations/0006_accounts_management_fields.sql"),
            kind: MigrationKind::Up,
        },
        Migration {
            version: 7,
            description: "add_cash_transactions",
            sql: include_str!("../migrations/0007_cash_transactions.sql"),
            kind: MigrationKind::Up,
        },
        Migration {
            version: 8,
            description: "fix_cash_transactions_conflict_target",
            sql: include_str!("../migrations/0008_cash_transactions_conflict_fix.sql"),
            kind: MigrationKind::Up,
        },
        Migration {
            version: 9,
            description: "cash_transactions_related_type_unique",
            sql: include_str!("../migrations/0009_cash_transactions_related_type_unique.sql"),
            kind: MigrationKind::Up,
        },
        Migration {
            version: 10,
            description: "tdcc_intermediate_import_tables",
            sql: include_str!("../migrations/0010_tdcc_intermediate_import.sql"),
            kind: MigrationKind::Up,
        },
        Migration {
            version: 11,
            description: "tdcc_bootstrap_formal_tracking",
            sql: include_str!("../migrations/0011_bootstrap_formal_tracking.sql"),
            kind: MigrationKind::Up,
        },
        Migration {
            version: 12,
            description: "auto_dividend_events",
            sql: include_str!("../migrations/0012_auto_dividend_events.sql"),
            kind: MigrationKind::Up,
        },
        Migration {
            version: 13,
            description: "pledge_financing_profiles",
            sql: include_str!("../migrations/0013_pledge_financing_profiles.sql"),
            kind: MigrationKind::Up,
        },
        Migration {
            version: 14,
            description: "pledge_financing_profile_extensions",
            sql: include_str!("../migrations/0014_pledge_financing_profile_extensions.sql"),
            kind: MigrationKind::Up,
        },
        Migration {
            version: 15,
            description: "cash_transactions_auto_dividend_link",
            sql: include_str!("../migrations/0015_cash_transactions_auto_dividend_link.sql"),
            kind: MigrationKind::Up,
        },
    ]
}

fn db_candidates(app: &AppHandle) -> Result<Vec<PathBuf>, String> {
    let mut result = Vec::new();
    if let Some(portable_root) = portable_root_dir()? {
        let data_dir = portable_root.join("data");
        fs::create_dir_all(&data_dir).map_err(|err| format!("建立 portable data 資料夾失敗: {err}"))?;
        result.push(data_dir.join("stock-vault.db"));
    }
    let app_data = app
        .path()
        .app_data_dir()
        .map_err(|err| format!("無法取得 AppData 目錄: {err}"))?;
    let app_config = app
        .path()
        .app_config_dir()
        .map_err(|err| format!("無法取得 AppConfig 目錄: {err}"))?;

    for dir in [app_data, app_config] {
        fs::create_dir_all(&dir).map_err(|err| format!("建立資料夾失敗: {err}"))?;
        result.push(dir.join("stock-vault.db"));
    }
    Ok(result)
}

fn is_inside_app_bundle(path: &Path) -> bool {
    path.ancestors().any(|ancestor| ancestor.extension().map(|ext| ext == "app").unwrap_or(false))
}

fn portable_mode_for_exe_path(exe_path: &Path, force_enable: bool) -> Option<PathBuf> {
    if is_inside_app_bundle(exe_path) {
        return None;
    }

    let exe_dir = exe_path.parent()?;
    let has_portable_markers = ["data", "snapshots", "backups"]
        .iter()
        .any(|segment| exe_dir.join(segment).exists());

    if force_enable || has_portable_markers {
        Some(exe_dir.to_path_buf())
    } else {
        None
    }
}

fn portable_root_dir() -> Result<Option<PathBuf>, String> {
    let exe_path = std::env::current_exe().map_err(|err| format!("無法取得目前執行檔路徑: {err}"))?;
    let force_enable = std::env::var("STOCK_VAULT_PORTABLE")
        .ok()
        .map(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false);

    let Some(root) = portable_mode_for_exe_path(&exe_path, force_enable) else {
        return Ok(None);
    };

    fs::create_dir_all(root.join("data")).map_err(|err| format!("建立 portable data 資料夾失敗: {err}"))?;
    fs::create_dir_all(root.join("snapshots"))
        .map_err(|err| format!("建立 portable snapshots 資料夾失敗: {err}"))?;
    fs::create_dir_all(root.join("backups")).map_err(|err| format!("建立 portable backups 資料夾失敗: {err}"))?;

    Ok(Some(root))
}

fn current_db_path(app: &AppHandle) -> Result<PathBuf, String> {
    let candidates = db_candidates(app)?;
    for path in &candidates {
        if path.exists() {
            return Ok(path.clone());
        }
    }
    Ok(candidates[0].clone())
}

fn current_sqlite_schema_version_number() -> i64 {
    sqlite_migrations()
        .into_iter()
        .filter(|migration| matches!(migration.kind, MigrationKind::Up))
        .map(|migration| migration.version)
        .max()
        .unwrap_or_default()
}

fn current_sqlite_schema_version_label() -> String {
    format!(
        "stock-vault.sqlite.v{}",
        current_sqlite_schema_version_number()
    )
}

fn normalize_tx_type(raw: &str) -> String {
    let upper = raw.trim().to_uppercase();
    if upper == "DIVIDEND" {
        "CASH_DIVIDEND".to_string()
    } else {
        upper
    }
}

fn normalize_cash_type(raw: &str) -> String {
    raw.trim().to_uppercase()
}

fn ensure_supported_tx_type(tx_type: &str) -> Result<(), String> {
    match tx_type {
        "BUY" | "SELL" | "CASH_DIVIDEND" | "STOCK_DIVIDEND" | "FEE_ADJUSTMENT" => Ok(()),
        _ => Err(format!("不支援的交易類型：{tx_type}")),
    }
}

fn ensure_supported_cash_type(cash_type: &str) -> Result<(), String> {
    match cash_type {
        "DEPOSIT" | "WITHDRAWAL" | "BUY_SETTLEMENT" | "SELL_SETTLEMENT" | "CASH_DIVIDEND_IN"
        | "FEE_OUT" | "TAX_OUT" | "INTEREST_IN" | "INTEREST_OUT" | "MANUAL_ADJUSTMENT" => Ok(()),
        _ => Err(format!("不支援的現金流水類型：{cash_type}")),
    }
}

fn normalize_cash_amount(cash_type: &str, amount: f64) -> f64 {
    match cash_type {
        "DEPOSIT" | "SELL_SETTLEMENT" | "CASH_DIVIDEND_IN" | "INTEREST_IN" => amount.abs(),
        "WITHDRAWAL" | "BUY_SETTLEMENT" | "FEE_OUT" | "TAX_OUT" | "INTEREST_OUT" => -amount.abs(),
        "MANUAL_ADJUSTMENT" => amount,
        _ => amount,
    }
}

fn round_money(value: f64) -> f64 {
    (value * 10000.0).round() / 10000.0
}

fn affects_lots(tx_type: &str) -> bool {
    tx_type == "BUY" || tx_type == "SELL" || tx_type == "STOCK_DIVIDEND"
}

fn open_conn(app: &AppHandle) -> Result<Connection, String> {
    let path = current_db_path(app)?;
    let conn = Connection::open(path).map_err(|err| format!("開啟資料庫失敗: {err}"))?;
    conn.busy_timeout(Duration::from_millis(5000))
        .map_err(|err| format!("設定 busy timeout 失敗: {err}"))?;
    conn.execute_batch(
        "
        PRAGMA foreign_keys = ON;
        PRAGMA journal_mode = WAL;
        PRAGMA busy_timeout = 5000;
    ",
    )
    .map_err(|err| format!("設定資料庫 pragma 失敗: {err}"))?;
    ensure_sqlite_schema(&conn)?;
    Ok(conn)
}

fn migration_checksum(sql: &str) -> Vec<u8> {
    let mut hasher = Sha384::new();
    hasher.update(sql.as_bytes());
    hasher.finalize().to_vec()
}

fn ensure_sqlite_schema(conn: &Connection) -> Result<(), String> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS _sqlx_migrations (
            version BIGINT PRIMARY KEY,
            description TEXT NOT NULL,
            installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            success BOOLEAN NOT NULL,
            checksum BLOB NOT NULL,
            execution_time BIGINT NOT NULL
        );
    ",
    )
    .map_err(|err| format!("建立 migration 紀錄表失敗: {err}"))?;

    let mut applied_stmt = conn
        .prepare("SELECT version, success, checksum FROM _sqlx_migrations")
        .map_err(|err| format!("讀取 migration 紀錄失敗: {err}"))?;
    let mut applied_rows = applied_stmt
        .query([])
        .map_err(|err| format!("查詢 migration 紀錄失敗: {err}"))?;

    let mut applied: HashMap<i64, (bool, Vec<u8>)> = HashMap::new();
    while let Some(row) = applied_rows
        .next()
        .map_err(|err| format!("讀取 migration 紀錄列失敗: {err}"))?
    {
        let version: i64 = row
            .get(0)
            .map_err(|err| format!("讀取 migration version 失敗: {err}"))?;
        let success: bool = row
            .get(1)
            .map_err(|err| format!("讀取 migration success 失敗: {err}"))?;
        let checksum: Vec<u8> = row
            .get(2)
            .map_err(|err| format!("讀取 migration checksum 失敗: {err}"))?;
        applied.insert(version, (success, checksum));
    }
    drop(applied_rows);
    drop(applied_stmt);

    for migration in sqlite_migrations() {
        if !matches!(migration.kind, MigrationKind::Up) {
            continue;
        }

        let version = migration.version;
        let checksum = migration_checksum(migration.sql);
        if let Some((success, existing_checksum)) = applied.get(&version) {
            if !*success {
                return Err(format!(
                    "migration {} ({}) 上次套用失敗，請先檢查資料庫狀態。",
                    version, migration.description
                ));
            }
            if existing_checksum != &checksum {
                return Err(format!(
                    "migration {} ({}) checksum 不一致，無法安全啟動。",
                    version, migration.description
                ));
            }
            continue;
        }

        let started = Instant::now();
        conn.execute_batch(migration.sql).map_err(|err| {
            format!(
                "套用 migration {} ({}) 失敗: {err}",
                version, migration.description
            )
        })?;
        let execution_time = started.elapsed().as_nanos() as i64;
        conn.execute(
            "
            INSERT INTO _sqlx_migrations
              (version, description, success, checksum, execution_time)
            VALUES (?1, ?2, 1, ?3, ?4)
            ",
            params![version, migration.description, checksum, execution_time],
        )
        .map_err(|err| {
            format!(
                "寫入 migration {} ({}) 紀錄失敗: {err}",
                version, migration.description
            )
        })?;
    }

    Ok(())
}

fn normalize_optional_text(value: Option<String>) -> Option<String> {
    let trimmed = value.unwrap_or_default().trim().to_string();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

fn normalize_required_text(value: String, field_label: &str) -> Result<String, String> {
    let trimmed = value.trim().to_string();
    if trimmed.is_empty() {
        return Err(format!("{field_label}不可空白。"));
    }
    Ok(trimmed)
}

fn validate_bank_account_last4(last4: &Option<String>) -> Result<(), String> {
    if let Some(value) = last4 {
        if !(value.len() == 4 || value.len() == 5) || !value.chars().all(|ch| ch.is_ascii_digit()) {
            return Err("交割帳號末碼請輸入 4 或 5 碼數字。".to_string());
        }
    }
    Ok(())
}

fn infer_market_from_symbol(symbol: &str) -> &'static str {
    if symbol.chars().all(|ch| ch.is_ascii_digit()) {
        "TWSE"
    } else {
        "US"
    }
}

fn default_currency_for_market(market: &str) -> &'static str {
    if market.eq_ignore_ascii_case("US") {
        "USD"
    } else {
        "TWD"
    }
}

fn default_board_lot_for_market(market: &str) -> i64 {
    if market.eq_ignore_ascii_case("US") {
        1
    } else {
        1000
    }
}

fn resolve_tdcc_account_id_tx(
    tx: &Transaction,
    account_no: &str,
    broker_name: &str,
    account_name: &str,
    source: &str,
) -> Result<Option<i64>, String> {
    let by_mapping: Option<i64> = tx
        .query_row(
            "
            SELECT account_id
            FROM tdcc_account_mappings
            WHERE account_no = ?1 AND source = ?2
            LIMIT 1
        ",
            params![account_no, source],
            |row| row.get(0),
        )
        .optional()
        .map_err(|err| format!("查詢匯入帳戶對應失敗: {err}"))?;
    if by_mapping.is_some() {
        return Ok(by_mapping);
    }

    let by_account_no: Option<i64> = tx
        .query_row(
            "
            SELECT account_id
            FROM tdcc_account_mappings
            WHERE account_no = ?1
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
        ",
            params![account_no],
            |row| row.get(0),
        )
        .optional()
        .map_err(|err| format!("查詢匯入帳戶號碼對應失敗: {err}"))?;
    if by_account_no.is_some() {
        return Ok(by_account_no);
    }

    tx.query_row(
        "
        SELECT id
        FROM accounts
        WHERE name = ?1
          AND COALESCE(broker, '') = ?2
        ORDER BY id ASC
        LIMIT 1
    ",
        params![account_name, broker_name],
        |row| row.get(0),
    )
    .optional()
    .map_err(|err| format!("查詢既有帳戶失敗: {err}"))
}

fn ensure_security_for_seed_tx(tx: &Transaction, symbol: &str, name: &str) -> Result<i64, String> {
    let symbol_upper = symbol.trim().to_uppercase();
    if symbol_upper.is_empty() {
        return Err("匯入 seed 股票代號不可空白。".to_string());
    }
    let normalized_name = name.trim();
    if normalized_name.is_empty() {
        return Err(format!(
            "匯入 seed 股票 {symbol_upper} 缺少名稱，無法建立主檔。"
        ));
    }

    let existing: Option<i64> = tx
        .query_row(
            "SELECT id FROM securities WHERE symbol = ?1 LIMIT 1",
            params![symbol_upper],
            |row| row.get(0),
        )
        .optional()
        .map_err(|err| format!("查詢股票主檔失敗: {err}"))?;
    if let Some(id) = existing {
        return Ok(id);
    }

    let market = infer_market_from_symbol(&symbol_upper);
    let currency = default_currency_for_market(market);
    let board_lot_size = default_board_lot_for_market(market);

    tx.execute(
        "
        INSERT INTO securities (symbol, name, market, currency, board_lot_size, category, tags)
        VALUES (?1, ?2, ?3, ?4, ?5, NULL, '[]')
    ",
        params![
            symbol_upper,
            normalized_name,
            market,
            currency,
            board_lot_size
        ],
    )
    .map_err(|err| format!("建立股票主檔失敗: {err}"))?;

    Ok(tx.last_insert_rowid())
}

fn clear_bootstrap_formal_rows_tx(tx: &Transaction) -> Result<(), String> {
    tx.execute(
        "
        DELETE FROM cash_transactions
        WHERE related_transaction_id IN (
          SELECT transaction_id FROM tdcc_imported_formal_transactions
        )
    ",
        [],
    )
    .map_err(|err| format!("清除匯入現金流水失敗: {err}"))?;

    tx.execute(
        "
        DELETE FROM sell_allocations
        WHERE sell_transaction_id IN (
          SELECT transaction_id FROM tdcc_imported_formal_transactions
        )
           OR buy_transaction_id IN (
          SELECT transaction_id FROM tdcc_imported_formal_transactions
        )
    ",
        [],
    )
    .map_err(|err| format!("清除匯入 allocation 失敗: {err}"))?;

    tx.execute(
        "
        DELETE FROM transactions
        WHERE id IN (
          SELECT transaction_id FROM tdcc_imported_formal_transactions
        )
    ",
        [],
    )
    .map_err(|err| format!("清除匯入交易失敗: {err}"))?;

    tx.execute("DELETE FROM tdcc_imported_formal_transactions", [])
        .map_err(|err| format!("清除匯入交易追蹤失敗: {err}"))?;

    let mut tracked_snapshot_ids = Vec::new();
    {
        let mut stmt = tx
            .prepare(
                "
                SELECT price_snapshot_id
                FROM tdcc_imported_formal_price_snapshots
            ",
            )
            .map_err(|err| format!("讀取匯入價格追蹤失敗: {err}"))?;
        let rows = stmt
            .query_map([], |row| row.get::<_, i64>(0))
            .map_err(|err| format!("解析匯入價格追蹤失敗: {err}"))?;
        for row in rows {
            tracked_snapshot_ids.push(row.map_err(|err| format!("解析匯入價格追蹤列失敗: {err}"))?);
        }
    }

    tx.execute("DELETE FROM tdcc_imported_formal_price_snapshots", [])
        .map_err(|err| format!("清除匯入價格追蹤失敗: {err}"))?;
    for snapshot_id in tracked_snapshot_ids {
        tx.execute(
            "DELETE FROM price_snapshots WHERE id = ?1",
            params![snapshot_id],
        )
        .map_err(|err| format!("清除匯入價格失敗: {err}"))?;
    }

    Ok(())
}

fn day_before_or_same(date_str: &str) -> String {
    NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
        .ok()
        .map(|date| {
            (date - ChronoDuration::days(1))
                .format("%Y-%m-%d")
                .to_string()
        })
        .unwrap_or_else(|| date_str.to_string())
}

fn format_bootstrap_trade_note(
    label: &str,
    source: &str,
    source_memo: Option<&str>,
    source_pdf: Option<&str>,
    page_no: Option<i64>,
    running_balance_from_pdf: Option<f64>,
) -> String {
    let mut parts = vec![
        "[匯入中介檔]".to_string(),
        label.to_string(),
        format!("source={source}"),
    ];
    if let Some(memo) = source_memo {
        if !memo.trim().is_empty() {
            parts.push(format!("memo={}", memo.trim()));
        }
    }
    if let Some(pdf) = source_pdf {
        if !pdf.trim().is_empty() {
            parts.push(format!("pdf={}", pdf.trim()));
        }
    }
    if let Some(page) = page_no {
        parts.push(format!("page={page}"));
    }
    if let Some(balance) = running_balance_from_pdf {
        parts.push(format!("pdf_balance={balance}"));
    }
    parts.join(" | ")
}

fn upsert_bootstrap_price_snapshot_tx(
    tx: &Transaction,
    run_key: &str,
    security_id: i64,
    snapshot_date: &str,
    close_price: f64,
) -> Result<(), String> {
    tx.execute(
        "
        INSERT INTO price_snapshots (security_id, price_date, close_price, currency, source)
        VALUES (?1, ?2, ?3, 'TWD', 'BOOTSTRAP_HOLDINGS_SEED')
        ON CONFLICT(security_id, price_date)
        DO UPDATE SET
          close_price = excluded.close_price,
          currency = excluded.currency,
          source = excluded.source
    ",
        params![security_id, snapshot_date, close_price],
    )
    .map_err(|err| format!("寫入匯入價格失敗: {err}"))?;

    let snapshot_id: i64 = tx
        .query_row(
            "
            SELECT id
            FROM price_snapshots
            WHERE security_id = ?1 AND price_date = ?2
            LIMIT 1
        ",
            params![security_id, snapshot_date],
            |row| row.get(0),
        )
        .map_err(|err| format!("讀取匯入價格 id 失敗: {err}"))?;

    tx.execute(
        "
        INSERT INTO tdcc_imported_formal_price_snapshots (price_snapshot_id, run_key, updated_at)
        VALUES (?1, ?2, CURRENT_TIMESTAMP)
        ON CONFLICT(price_snapshot_id)
        DO UPDATE SET
          run_key = excluded.run_key,
          updated_at = CURRENT_TIMESTAMP
    ",
        params![snapshot_id, run_key],
    )
    .map_err(|err| format!("寫入匯入價格追蹤失敗: {err}"))?;

    Ok(())
}

fn insert_bootstrap_formal_transaction_tx(
    tx: &Transaction,
    run_key: &str,
    account_id: i64,
    security_id: i64,
    tx_type: &str,
    trade_date: &str,
    quantity: f64,
    price: f64,
    note: &str,
) -> Result<i64, String> {
    let amount = round_money(quantity * price);
    tx.execute(
        "
        INSERT INTO transactions
          (account_id, security_id, type, trade_date, quantity, price, fee, tax, interest, amount, note)
        VALUES
          (?1, ?2, ?3, ?4, ?5, ?6, 0, 0, 0, ?7, ?8)
    ",
        params![
            account_id,
            security_id,
            tx_type,
            trade_date,
            quantity,
            price,
            amount,
            note
        ],
    )
    .map_err(|err| format!("寫入匯入正式交易失敗: {err}"))?;
    let transaction_id = tx.last_insert_rowid();

    tx.execute(
        "
        INSERT INTO tdcc_imported_formal_transactions (transaction_id, run_key, updated_at)
        VALUES (?1, ?2, CURRENT_TIMESTAMP)
        ON CONFLICT(transaction_id)
        DO UPDATE SET
          run_key = excluded.run_key,
          updated_at = CURRENT_TIMESTAMP
    ",
        params![transaction_id, run_key],
    )
    .map_err(|err| format!("寫入匯入正式交易追蹤失敗: {err}"))?;

    sync_cash_flow_for_transaction_tx(
        tx,
        transaction_id,
        account_id,
        tx_type,
        trade_date,
        quantity,
        price,
        0.0,
        0.0,
        0.0,
        amount,
    )?;

    Ok(transaction_id)
}

fn build_allocations(rows: &[TxRow]) -> Result<Vec<(i64, i64, f64)>, String> {
    let mut lots: Vec<Lot> = Vec::new();
    let mut allocations: Vec<(i64, i64, f64)> = Vec::new();

    for tx in rows {
        if tx.tx_type == "BUY" || tx.tx_type == "STOCK_DIVIDEND" {
            lots.push(Lot {
                buy_id: tx.id,
                remaining_qty: tx.quantity,
            });
            continue;
        }

        if tx.tx_type != "SELL" {
            continue;
        }

        let mut remaining = tx.quantity;
        for lot in lots.iter_mut() {
            if remaining <= 0.0000001 {
                break;
            }
            if lot.remaining_qty <= 0.0000001 {
                continue;
            }

            let allocated = lot.remaining_qty.min(remaining);
            lot.remaining_qty -= allocated;
            remaining -= allocated;
            allocations.push((tx.id, lot.buy_id, allocated));
        }

        if remaining > 0.0000001 {
            return Err(format!("賣出數量超過可用庫存，尚缺 {remaining}"));
        }
    }

    Ok(allocations)
}

fn build_buy_lot_state_map_tx(
    tx: &Transaction,
    account_id: i64,
    security_id: i64,
    skip_sell_id: Option<i64>,
) -> Result<HashMap<i64, BuyLotState>, String> {
    let mut lots = HashMap::new();
    let mut stmt = tx
        .prepare(
            "
            SELECT id, account_id, security_id, type, quantity
            FROM transactions
            WHERE account_id = ?1 AND security_id = ?2
              AND type IN ('BUY', 'STOCK_DIVIDEND')
        ",
        )
        .map_err(|err| format!("讀取 buy lots 失敗: {err}"))?;
    let rows = stmt
        .query_map(params![account_id, security_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, f64>(4)?,
            ))
        })
        .map_err(|err| format!("解析 buy lots 失敗: {err}"))?;

    for row in rows {
        let (buy_id, buy_account_id, buy_security_id, _tx_type, qty) =
            row.map_err(|err| format!("解析 buy lots 內容失敗: {err}"))?;
        lots.insert(
            buy_id,
            BuyLotState {
                account_id: buy_account_id,
                security_id: buy_security_id,
                remaining_qty: qty,
            },
        );
    }

    let skip_id = skip_sell_id.unwrap_or(-1);
    let mut alloc_stmt = tx
        .prepare(
            "
            SELECT sa.buy_transaction_id, COALESCE(SUM(sa.quantity), 0)
            FROM sell_allocations sa
            INNER JOIN transactions s ON s.id = sa.sell_transaction_id
            WHERE s.account_id = ?1
              AND s.security_id = ?2
              AND s.type = 'SELL'
              AND s.id <> ?3
            GROUP BY sa.buy_transaction_id
        ",
        )
        .map_err(|err| format!("讀取既有 allocation 失敗: {err}"))?;
    let alloc_rows = alloc_stmt
        .query_map(params![account_id, security_id, skip_id], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, f64>(1)?))
        })
        .map_err(|err| format!("解析既有 allocation 失敗: {err}"))?;

    for row in alloc_rows {
        let (buy_id, allocated_qty) =
            row.map_err(|err| format!("解析既有 allocation 內容失敗: {err}"))?;
        if let Some(lot) = lots.get_mut(&buy_id) {
            lot.remaining_qty -= allocated_qty;
        }
    }

    Ok(lots)
}

fn validate_manual_allocations(
    lots: &HashMap<i64, BuyLotState>,
    account_id: i64,
    security_id: i64,
    sell_quantity: f64,
    allocations: &[ManualAllocationPayload],
) -> Result<(), String> {
    if allocations.is_empty() {
        return Err("手動 lot 分配不可為空，請改用自動 FIFO 或填入分配數量。".to_string());
    }

    if sell_quantity <= 0.0 {
        return Err("賣出股數必須大於 0，才能使用手動 lot 分配。".to_string());
    }

    let mut grouped: HashMap<i64, f64> = HashMap::new();
    for item in allocations {
        if item.quantity <= 0.0 {
            return Err(format!(
                "buyTransactionId={} 的手動分配數量必須大於 0。",
                item.buy_transaction_id
            ));
        }
        let lot = lots.get(&item.buy_transaction_id).ok_or_else(|| {
            format!(
                "buyTransactionId={} 不存在或不屬於目前帳戶/股票。",
                item.buy_transaction_id
            )
        })?;
        if lot.account_id != account_id || lot.security_id != security_id {
            return Err(format!(
                "buyTransactionId={} 不屬於目前帳戶/股票，無法分配。",
                item.buy_transaction_id
            ));
        }
        grouped
            .entry(item.buy_transaction_id)
            .and_modify(|sum| *sum += item.quantity)
            .or_insert(item.quantity);
    }

    let total_allocated: f64 = grouped.values().sum();
    if (total_allocated - sell_quantity).abs() > 0.000001 {
        return Err(format!(
            "手動分配總量 ({total_allocated}) 必須等於賣出股數 ({sell_quantity})。"
        ));
    }

    for (buy_id, qty) in grouped {
        let remaining = lots
            .get(&buy_id)
            .map(|lot| lot.remaining_qty.max(0.0))
            .unwrap_or(0.0);
        if qty - remaining > 0.000001 {
            return Err(format!(
                "buyTransactionId={buy_id} 分配數量 ({qty}) 超過可用 lot ({remaining})。"
            ));
        }
    }

    Ok(())
}

fn write_manual_allocations_tx(
    tx: &Transaction,
    sell_transaction_id: i64,
    allocations: &[ManualAllocationPayload],
) -> Result<(), String> {
    let mut grouped: HashMap<i64, f64> = HashMap::new();
    for item in allocations {
        grouped
            .entry(item.buy_transaction_id)
            .and_modify(|sum| *sum += item.quantity)
            .or_insert(item.quantity);
    }

    for (buy_id, qty) in grouped {
        tx.execute(
            "
            INSERT INTO sell_allocations (sell_transaction_id, buy_transaction_id, quantity)
            VALUES (?1, ?2, ?3)
        ",
            params![sell_transaction_id, buy_id, qty],
        )
        .map_err(|err| format!("寫入手動 allocation 失敗: {err}"))?;
    }

    Ok(())
}

fn rebuild_scopes_tx(tx: &Transaction, scopes: &[(i64, i64)]) -> Result<(), String> {
    let mut dedup = BTreeSet::new();
    for (account_id, security_id) in scopes {
        dedup.insert((*account_id, *security_id));
    }

    for (account_id, security_id) in dedup {
        let mut stmt = tx
            .prepare(
                "
                SELECT id, type, quantity
                FROM transactions
                WHERE account_id = ?1 AND security_id = ?2
                  AND type IN ('BUY', 'SELL', 'STOCK_DIVIDEND')
                ORDER BY trade_date ASC, created_at ASC, id ASC
            ",
            )
            .map_err(|err| format!("重建前查詢交易失敗: {err}"))?;

        let rows_iter = stmt
            .query_map(params![account_id, security_id], |row| {
                Ok(TxRow {
                    id: row.get(0)?,
                    tx_type: row.get(1)?,
                    quantity: row.get(2)?,
                })
            })
            .map_err(|err| format!("重建前讀取交易失敗: {err}"))?;

        let mut rows: Vec<TxRow> = Vec::new();
        for row in rows_iter {
            rows.push(row.map_err(|err| format!("交易資料解析失敗: {err}"))?);
        }

        let allocations = build_allocations(&rows)?;

        tx.execute(
            "
            DELETE FROM sell_allocations
            WHERE sell_transaction_id IN (
              SELECT id FROM transactions
              WHERE account_id = ?1 AND security_id = ?2 AND type = 'SELL'
            )
        ",
            params![account_id, security_id],
        )
        .map_err(|err| format!("刪除舊 allocation 失敗: {err}"))?;

        for (sell_id, buy_id, qty) in allocations {
            tx.execute(
                "
                INSERT INTO sell_allocations (sell_transaction_id, buy_transaction_id, quantity)
                VALUES (?1, ?2, ?3)
            ",
                params![sell_id, buy_id, qty],
            )
            .map_err(|err| format!("寫入 allocation 失敗: {err}"))?;
        }
    }

    Ok(())
}

fn get_account_currency_tx(tx: &Transaction, account_id: i64) -> Result<String, String> {
    tx.query_row(
        "SELECT currency FROM accounts WHERE id = ?1",
        params![account_id],
        |row| row.get(0),
    )
    .map_err(|err| format!("讀取帳戶幣別失敗: {err}"))
}

fn push_if_non_zero(target: &mut Vec<CashFlowMapping>, cash_type: &str, amount: f64, note: &str) {
    if amount.abs() <= 0.0000001 {
        return;
    }
    target.push(CashFlowMapping {
        cash_type: cash_type.to_string(),
        amount,
        note: note.to_string(),
    });
}

fn build_cash_flow_mappings_for_transaction(
    tx_type: &str,
    quantity: f64,
    price: f64,
    fee: f64,
    tax: f64,
    interest: f64,
    amount: f64,
) -> Vec<CashFlowMapping> {
    let gross_amount = round_money(quantity * price);
    let mut rows: Vec<CashFlowMapping> = Vec::new();
    match tx_type {
        "BUY" => {
            let settlement = round_money(-(gross_amount + fee + tax + interest));
            push_if_non_zero(
                &mut rows,
                "BUY_SETTLEMENT",
                settlement,
                "自動對應：買進交割",
            );
            if fee > 0.0 {
                push_if_non_zero(
                    &mut rows,
                    "FEE_OUT",
                    round_money(-fee),
                    "自動對應：買進手續費",
                );
            }
            if tax > 0.0 {
                push_if_non_zero(
                    &mut rows,
                    "TAX_OUT",
                    round_money(-tax),
                    "自動對應：買進交易稅",
                );
            }
            if interest > 0.0 {
                push_if_non_zero(
                    &mut rows,
                    "INTEREST_OUT",
                    round_money(-interest),
                    "自動對應：買進利息",
                );
            } else if interest < 0.0 {
                push_if_non_zero(
                    &mut rows,
                    "INTEREST_IN",
                    round_money(interest.abs()),
                    "自動對應：買進利息回沖",
                );
            }
        }
        "SELL" => {
            let settlement = round_money(gross_amount - fee - tax - interest);
            push_if_non_zero(
                &mut rows,
                "SELL_SETTLEMENT",
                settlement,
                "自動對應：賣出交割",
            );
            if fee > 0.0 {
                push_if_non_zero(
                    &mut rows,
                    "FEE_OUT",
                    round_money(-fee),
                    "自動對應：賣出手續費",
                );
            }
            if tax > 0.0 {
                push_if_non_zero(
                    &mut rows,
                    "TAX_OUT",
                    round_money(-tax),
                    "自動對應：賣出交易稅",
                );
            }
            if interest > 0.0 {
                push_if_non_zero(
                    &mut rows,
                    "INTEREST_OUT",
                    round_money(-interest),
                    "自動對應：賣出利息",
                );
            } else if interest < 0.0 {
                push_if_non_zero(
                    &mut rows,
                    "INTEREST_IN",
                    round_money(interest.abs()),
                    "自動對應：賣出利息回沖",
                );
            }
        }
        "CASH_DIVIDEND" => {
            push_if_non_zero(
                &mut rows,
                "CASH_DIVIDEND_IN",
                round_money(amount),
                "自動對應：現金股利",
            );
        }
        "FEE_ADJUSTMENT" => {
            push_if_non_zero(
                &mut rows,
                "MANUAL_ADJUSTMENT",
                round_money(amount),
                "自動對應：費用調整",
            );
        }
        _ => {}
    }
    rows
}

fn sync_cash_flow_for_transaction_tx(
    tx: &Transaction,
    transaction_id: i64,
    account_id: i64,
    tx_type: &str,
    trade_date: &str,
    quantity: f64,
    price: f64,
    fee: f64,
    tax: f64,
    interest: f64,
    amount: f64,
) -> Result<(), String> {
    tx.execute(
        "DELETE FROM cash_transactions WHERE related_transaction_id = ?1",
        params![transaction_id],
    )
    .map_err(|err| format!("清理關聯現金流水失敗: {err}"))?;

    let rows = build_cash_flow_mappings_for_transaction(
        tx_type, quantity, price, fee, tax, interest, amount,
    );
    if rows.is_empty() {
        return Ok(());
    }
    let currency = get_account_currency_tx(tx, account_id)?;
    for row in rows {
        let normalized = normalize_cash_amount(&row.cash_type, row.amount);
        tx.execute(
            "
            INSERT INTO cash_transactions
              (account_id, trade_date, type, amount, currency, note, related_transaction_id)
            VALUES
              (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        ",
            params![
                account_id,
                trade_date,
                row.cash_type,
                normalized,
                currency,
                row.note,
                transaction_id
            ],
        )
        .map_err(|err| format!("同步交易現金流水失敗: {err}"))?;
    }
    Ok(())
}

#[allow(non_snake_case)]
#[tauri::command]
fn run_seed_if_needed(app: AppHandle, seedVersion: String) -> Result<bool, String> {
    let mut conn = open_conn(&app)?;
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| format!("開始 seed 交易失敗: {err}"))?;

    let existing: Option<String> = tx
        .query_row(
            "SELECT value FROM app_settings WHERE key = 'seed_version'",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(|err| format!("讀取 seed_version 失敗: {err}"))?;

    if existing.as_deref() == Some(seedVersion.as_str()) {
        tx.commit().map_err(|err| format!("seed 提交失敗: {err}"))?;
        return Ok(false);
    }

    tx.execute("DELETE FROM cash_transactions", [])
        .map_err(|err| format!("清除 cash_transactions 失敗: {err}"))?;
    tx.execute("DELETE FROM auto_dividend_events", [])
        .map_err(|err| format!("清除 auto_dividend_events 失敗: {err}"))?;
    tx.execute("DELETE FROM sell_allocations", [])
        .map_err(|err| format!("清除 sell_allocations 失敗: {err}"))?;
    tx.execute("DELETE FROM transactions", [])
        .map_err(|err| format!("清除 transactions 失敗: {err}"))?;
    tx.execute("DELETE FROM price_snapshots", [])
        .map_err(|err| format!("清除 price_snapshots 失敗: {err}"))?;
    tx.execute("DELETE FROM securities", [])
        .map_err(|err| format!("清除 securities 失敗: {err}"))?;
    tx.execute("DELETE FROM accounts", [])
        .map_err(|err| format!("清除 accounts 失敗: {err}"))?;

    tx.execute(
        "INSERT INTO accounts (name, broker, currency) VALUES (?1, ?2, ?3)",
        params!["長期投資帳戶", "永豐金證券", "TWD"],
    )
    .map_err(|err| format!("seed 寫入帳戶失敗: {err}"))?;
    tx.execute(
        "INSERT INTO accounts (name, broker, currency) VALUES (?1, ?2, ?3)",
        params!["高股息帳戶", "富邦證券", "TWD"],
    )
    .map_err(|err| format!("seed 寫入帳戶失敗: {err}"))?;

    tx.execute(
        "INSERT INTO securities (symbol, name, market, currency) VALUES (?1, ?2, ?3, ?4)",
        params!["2330", "台積電", "TWSE", "TWD"],
    )
    .map_err(|err| format!("seed 寫入股票失敗: {err}"))?;
    tx.execute(
        "INSERT INTO securities (symbol, name, market, currency) VALUES (?1, ?2, ?3, ?4)",
        params!["0056", "元大高股息", "TWSE", "TWD"],
    )
    .map_err(|err| format!("seed 寫入股票失敗: {err}"))?;

    let mut insert_tx = tx
        .prepare(
            "
        INSERT INTO transactions
          (account_id, security_id, type, trade_date, quantity, price, fee, tax, interest, amount, note)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
    ",
        )
        .map_err(|err| format!("seed 準備交易寫入失敗: {err}"))?;

    insert_tx
        .execute(params![
            1,
            1,
            "BUY",
            "2026-01-15",
            10.0,
            615.0,
            20.0,
            0.0,
            0.0,
            -6170.0,
            "逢低分批"
        ])
        .map_err(|err| format!("seed 寫入交易失敗: {err}"))?;
    insert_tx
        .execute(params![
            1,
            2,
            "BUY",
            "2026-01-18",
            200.0,
            35.4,
            20.0,
            0.0,
            0.0,
            -7100.0,
            "建立股息部位"
        ])
        .map_err(|err| format!("seed 寫入交易失敗: {err}"))?;
    insert_tx
        .execute(params![
            2,
            2,
            "CASH_DIVIDEND",
            "2026-02-20",
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            480.0,
            "股息入帳"
        ])
        .map_err(|err| format!("seed 寫入交易失敗: {err}"))?;
    insert_tx
        .execute(params![
            1,
            1,
            "SELL",
            "2026-03-01",
            4.0,
            955.0,
            20.0,
            11.0,
            0.0,
            3789.0,
            "部位調節"
        ])
        .map_err(|err| format!("seed 寫入交易失敗: {err}"))?;
    insert_tx
        .execute(params![
            1,
            2,
            "STOCK_DIVIDEND",
            "2026-03-10",
            5.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            "配股"
        ])
        .map_err(|err| format!("seed 寫入交易失敗: {err}"))?;
    insert_tx
        .execute(params![
            1,
            2,
            "FEE_ADJUSTMENT",
            "2026-03-11",
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            -60.0,
            "保管費"
        ])
        .map_err(|err| format!("seed 寫入交易失敗: {err}"))?;
    insert_tx
        .execute(params![
            1,
            Option::<i64>::None,
            "FEE_ADJUSTMENT",
            "2026-03-12",
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            -30.0,
            "帳戶管理費"
        ])
        .map_err(|err| format!("seed 寫入交易失敗: {err}"))?;
    drop(insert_tx);

    tx.execute(
        "INSERT INTO price_snapshots (security_id, price_date, close_price, currency, source) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![1, "2026-03-18", 932.0, "TWD", "manual"],
    )
    .map_err(|err| format!("seed 寫入價格失敗: {err}"))?;
    tx.execute(
        "INSERT INTO price_snapshots (security_id, price_date, close_price, currency, source) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![1, "2026-03-19", 938.0, "TWD", "manual"],
    )
    .map_err(|err| format!("seed 寫入價格失敗: {err}"))?;
    tx.execute(
        "INSERT INTO price_snapshots (security_id, price_date, close_price, currency, source) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![2, "2026-03-18", 36.2, "TWD", "manual"],
    )
    .map_err(|err| format!("seed 寫入價格失敗: {err}"))?;
    tx.execute(
        "INSERT INTO price_snapshots (security_id, price_date, close_price, currency, source) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![2, "2026-03-19", 36.35, "TWD", "manual"],
    )
    .map_err(|err| format!("seed 寫入價格失敗: {err}"))?;
    tx.execute(
        "
        INSERT INTO app_settings (key, value)
        VALUES ('base_currency', 'TWD')
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP
    ",
        [],
    )
    .map_err(|err| format!("seed 寫入設定失敗: {err}"))?;
    tx.execute(
        "
        INSERT INTO app_settings (key, value)
        VALUES ('seed_version', ?1)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP
    ",
        params![seedVersion],
    )
    .map_err(|err| format!("seed 寫入版本失敗: {err}"))?;

    rebuild_scopes_tx(&tx, &[(1, 1), (1, 2)])?;
    tx.commit().map_err(|err| format!("seed 提交失敗: {err}"))?;
    Ok(true)
}

fn save_transaction_inner(
    app: AppHandle,
    payload: TransactionPayload,
) -> Result<SaveTransactionResult, String> {
    let mut conn = open_conn(&app)?;
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| format!("開始交易失敗: {err}"))?;

    let tx_type = normalize_tx_type(&payload.tx_type);
    ensure_supported_tx_type(&tx_type)?;
    if tx_type != "SELL" && payload.manual_allocations.is_some() {
        return Err("manual_allocations 只允許用於 SELL 交易。".to_string());
    }

    let mut security_id = payload.security_id;
    if tx_type == "FEE_ADJUSTMENT" && security_id.is_none() {
        security_id = None;
    }
    if (tx_type == "BUY" || tx_type == "SELL" || tx_type == "STOCK_DIVIDEND")
        && security_id.is_none()
    {
        return Err("買賣與股票股利交易必須指定股票".to_string());
    }

    let previous: Option<(i64, Option<i64>, String)> = if let Some(id) = payload.id {
        tx.query_row(
            "
            SELECT account_id, security_id, type
            FROM transactions
            WHERE id = ?1
        ",
            params![id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(|err| format!("讀取舊交易失敗: {err}"))?
    } else {
        None
    };

    let quantity = payload.quantity.unwrap_or(0.0);
    let price = payload.price.unwrap_or(0.0);
    let fee = payload.fee.unwrap_or(0.0);
    let tax = payload.tax.unwrap_or(0.0);
    let interest = payload.interest.unwrap_or(0.0);
    let input_amount = payload.amount.unwrap_or(0.0);
    let amount = if tx_type == "BUY" || tx_type == "SELL" {
        round_money(quantity * price)
    } else {
        input_amount
    };
    let note = payload.note.unwrap_or_default();
    let manual_allocations_opt = payload.manual_allocations.as_ref().cloned();

    let saved_id = if let Some(id) = payload.id {
        tx.execute(
            "
            UPDATE transactions
            SET account_id = ?1, security_id = ?2, type = ?3, trade_date = ?4,
                quantity = ?5, price = ?6, fee = ?7, tax = ?8, interest = ?9, amount = ?10, note = ?11
            WHERE id = ?12
        ",
            params![
                payload.account_id,
                security_id,
                tx_type,
                payload.trade_date,
                quantity,
                price,
                fee,
                tax,
                interest,
                amount,
                note,
                id
            ],
        )
        .map_err(|err| format!("更新交易失敗: {err}"))?;
        id
    } else {
        tx.execute(
            "
            INSERT INTO transactions
              (account_id, security_id, type, trade_date, quantity, price, fee, tax, interest, amount, note)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
        ",
            params![
                payload.account_id,
                security_id,
                tx_type,
                payload.trade_date,
                quantity,
                price,
                fee,
                tax,
                interest,
                amount,
                note
            ],
        )
        .map_err(|err| format!("新增交易失敗: {err}"))?;
        tx.last_insert_rowid()
    };

    sync_cash_flow_for_transaction_tx(
        &tx,
        saved_id,
        payload.account_id,
        &tx_type,
        &payload.trade_date,
        quantity,
        price,
        fee,
        tax,
        interest,
        amount,
    )?;

    let mut scopes: Vec<(i64, i64)> = Vec::new();
    if let Some(current_sid) = security_id {
        if affects_lots(&tx_type) {
            scopes.push((payload.account_id, current_sid));
        }
    }
    if let Some((prev_account_id, prev_security_id, prev_type)) = previous {
        if let Some(prev_sid) = prev_security_id {
            if affects_lots(&prev_type) {
                scopes.push((prev_account_id, prev_sid));
            }
        }
    }

    let manual_mode = tx_type == "SELL" && manual_allocations_opt.is_some();
    if manual_mode {
        let manual_allocations = manual_allocations_opt.unwrap_or_default();
        let sid = security_id.ok_or_else(|| "SELL 交易必須指定股票。".to_string())?;
        let lots = build_buy_lot_state_map_tx(&tx, payload.account_id, sid, Some(saved_id))?;
        validate_manual_allocations(
            &lots,
            payload.account_id,
            sid,
            quantity,
            &manual_allocations,
        )?;
        tx.execute(
            "DELETE FROM sell_allocations WHERE sell_transaction_id = ?1",
            params![saved_id],
        )
        .map_err(|err| format!("清除舊手動 allocation 失敗: {err}"))?;
        write_manual_allocations_tx(&tx, saved_id, &manual_allocations)?;

        let scoped_without_current = scopes
            .into_iter()
            .filter(|(account_id, security_id_value)| {
                !(*account_id == payload.account_id && *security_id_value == sid)
            })
            .collect::<Vec<_>>();
        rebuild_scopes_tx(&tx, &scoped_without_current)?;
    } else {
        rebuild_scopes_tx(&tx, &scopes)?;
    }

    tx.commit().map_err(|err| format!("交易提交失敗: {err}"))?;
    Ok(SaveTransactionResult { id: saved_id })
}

#[tauri::command]
fn save_transaction(
    app: AppHandle,
    payload: TransactionPayload,
) -> Result<SaveTransactionResult, String> {
    save_transaction_inner(app, payload)
}

#[allow(non_snake_case)]
#[tauri::command]
fn delete_transaction_and_rebuild(app: AppHandle, transactionId: i64) -> Result<(), String> {
    let mut conn = open_conn(&app)?;
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| format!("開始刪除交易失敗: {err}"))?;

    let previous: Option<(i64, Option<i64>, String)> = tx
        .query_row(
            "
            SELECT account_id, security_id, type
            FROM transactions
            WHERE id = ?1
        ",
            params![transactionId],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(|err| format!("讀取交易失敗: {err}"))?;

    tx.execute(
        "DELETE FROM cash_transactions WHERE related_transaction_id = ?1",
        params![transactionId],
    )
    .map_err(|err| format!("刪除關聯現金流水失敗: {err}"))?;
    tx.execute(
        "DELETE FROM sell_allocations WHERE sell_transaction_id = ?1 OR buy_transaction_id = ?1",
        params![transactionId],
    )
    .map_err(|err| format!("刪除 allocation 失敗: {err}"))?;
    tx.execute(
        "DELETE FROM transactions WHERE id = ?1",
        params![transactionId],
    )
    .map_err(|err| format!("刪除交易失敗: {err}"))?;

    let mut scopes: Vec<(i64, i64)> = Vec::new();
    if let Some((account_id, security_id, tx_type)) = previous {
        if let Some(sid) = security_id {
            if affects_lots(&tx_type) {
                scopes.push((account_id, sid));
            }
        }
    }
    rebuild_scopes_tx(&tx, &scopes)?;

    tx.commit()
        .map_err(|err| format!("刪除交易提交失敗: {err}"))?;
    Ok(())
}

#[tauri::command]
fn import_transactions_csv(
    app: AppHandle,
    rows: Vec<TransactionPayload>,
) -> Result<ImportTransactionsResult, String> {
    let mut conn = open_conn(&app)?;
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| format!("開始匯入交易失敗: {err}"))?;

    let mut scopes: Vec<(i64, i64)> = Vec::new();
    let mut inserted = 0usize;

    for row in rows {
        let tx_type = normalize_tx_type(&row.tx_type);
        ensure_supported_tx_type(&tx_type)?;
        let security_id = if tx_type == "FEE_ADJUSTMENT" {
            row.security_id
        } else {
            row.security_id
        };
        if (tx_type == "BUY" || tx_type == "SELL" || tx_type == "STOCK_DIVIDEND")
            && security_id.is_none()
        {
            return Err(format!("{tx_type} 交易必須有 security_id"));
        }

        let quantity = row.quantity.unwrap_or(0.0);
        let price = row.price.unwrap_or(0.0);
        let fee = row.fee.unwrap_or(0.0);
        let tax = row.tax.unwrap_or(0.0);
        let interest = row.interest.unwrap_or(0.0);
        let amount = if tx_type == "BUY" || tx_type == "SELL" {
            round_money(quantity * price)
        } else {
            row.amount.unwrap_or(0.0)
        };

        tx.execute(
            "
            INSERT INTO transactions
              (account_id, security_id, type, trade_date, quantity, price, fee, tax, interest, amount, note)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
        ",
            params![
                row.account_id,
                security_id,
                tx_type,
                row.trade_date,
                quantity,
                price,
                fee,
                tax,
                interest,
                amount,
                row.note.unwrap_or_default(),
            ],
        )
        .map_err(|err| format!("匯入交易寫入失敗: {err}"))?;
        let inserted_id = tx.last_insert_rowid();
        sync_cash_flow_for_transaction_tx(
            &tx,
            inserted_id,
            row.account_id,
            &tx_type,
            &row.trade_date,
            quantity,
            price,
            fee,
            tax,
            interest,
            amount,
        )?;
        inserted += 1;

        if let Some(sid) = security_id {
            if affects_lots(&tx_type) {
                scopes.push((row.account_id, sid));
            }
        }
    }

    rebuild_scopes_tx(&tx, &scopes)?;
    tx.commit()
        .map_err(|err| format!("匯入交易提交失敗: {err}"))?;

    Ok(ImportTransactionsResult { inserted })
}

#[tauri::command]
fn upsert_cash_transaction(app: AppHandle, payload: CashTransactionPayload) -> Result<i64, String> {
    let mut conn = open_conn(&app)?;
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| format!("開始現金流水寫入失敗: {err}"))?;

    if payload.related_transaction_id.is_some() {
        return Err("related_transaction_id 為系統保留欄位，無法手動設定。".to_string());
    }

    let cash_type = normalize_cash_type(&payload.cash_type);
    ensure_supported_cash_type(&cash_type)?;

    let trade_date = normalize_required_text(payload.trade_date, "日期")?;
    let _account_currency = get_account_currency_tx(&tx, payload.account_id)?;
    let amount = normalize_cash_amount(&cash_type, payload.amount);
    let currency = normalize_required_text(payload.currency, "幣別")?;
    let note = normalize_optional_text(payload.note);

    if let Some(id) = payload.id {
        let linked_tx: Option<Option<i64>> = tx
            .query_row(
                "SELECT related_transaction_id FROM cash_transactions WHERE id = ?1",
                params![id],
                |row| row.get::<_, Option<i64>>(0),
            )
            .optional()
            .map_err(|err| format!("讀取現金流水失敗: {err}"))?;
        let Some(existing_linked_tx) = linked_tx else {
            return Err("找不到要更新的現金流水。".to_string());
        };
        if existing_linked_tx.is_some() {
            return Err("此筆現金流水由交易自動產生，請至交易紀錄修改。".to_string());
        }

        tx.execute(
            "
            UPDATE cash_transactions
            SET account_id = ?1, trade_date = ?2, type = ?3, amount = ?4, currency = ?5, note = ?6
            WHERE id = ?7
        ",
            params![
                payload.account_id,
                trade_date,
                cash_type,
                amount,
                currency,
                note,
                id
            ],
        )
        .map_err(|err| format!("更新現金流水失敗: {err}"))?;

        tx.commit()
            .map_err(|err| format!("更新現金流水提交失敗: {err}"))?;
        return Ok(id);
    }

    tx.execute(
        "
        INSERT INTO cash_transactions
          (account_id, trade_date, type, amount, currency, note, related_transaction_id)
        VALUES
          (?1, ?2, ?3, ?4, ?5, ?6, NULL)
    ",
        params![
            payload.account_id,
            trade_date,
            cash_type,
            amount,
            currency,
            note
        ],
    )
    .map_err(|err| format!("新增現金流水失敗: {err}"))?;
    let saved_id = tx.last_insert_rowid();

    tx.commit()
        .map_err(|err| format!("新增現金流水提交失敗: {err}"))?;
    Ok(saved_id)
}

#[allow(non_snake_case)]
#[tauri::command]
fn delete_cash_transaction(app: AppHandle, cashTransactionId: i64) -> Result<(), String> {
    let mut conn = open_conn(&app)?;
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| format!("開始刪除現金流水失敗: {err}"))?;

    let linked_tx: Option<Option<i64>> = tx
        .query_row(
            "SELECT related_transaction_id FROM cash_transactions WHERE id = ?1",
            params![cashTransactionId],
            |row| row.get::<_, Option<i64>>(0),
        )
        .optional()
        .map_err(|err| format!("讀取現金流水失敗: {err}"))?;
    let Some(existing_linked_tx) = linked_tx else {
        return Err("找不到要刪除的現金流水。".to_string());
    };
    if existing_linked_tx.is_some() {
        return Err("此筆現金流水由交易自動產生，請至交易紀錄刪除或修改。".to_string());
    }

    tx.execute(
        "DELETE FROM cash_transactions WHERE id = ?1",
        params![cashTransactionId],
    )
    .map_err(|err| format!("刪除現金流水失敗: {err}"))?;
    tx.commit()
        .map_err(|err| format!("刪除現金流水提交失敗: {err}"))?;
    Ok(())
}

#[tauri::command]
fn upsert_price(app: AppHandle, payload: PricePayload) -> Result<(), String> {
    let mut conn = open_conn(&app)?;
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| format!("開始價格寫入失敗: {err}"))?;

    if let Some(id) = payload.id {
        tx.execute(
            "
            UPDATE price_snapshots
            SET security_id = ?1, price_date = ?2, close_price = ?3, currency = ?4, source = ?5
            WHERE id = ?6
        ",
            params![
                payload.security_id,
                payload.price_date,
                payload.close_price,
                payload.currency,
                payload.source,
                id
            ],
        )
        .map_err(|err| format!("更新價格失敗: {err}"))?;
    } else {
        tx.execute(
            "
            INSERT INTO price_snapshots (security_id, price_date, close_price, currency, source)
            VALUES (?1, ?2, ?3, ?4, ?5)
        ",
            params![
                payload.security_id,
                payload.price_date,
                payload.close_price,
                payload.currency,
                payload.source
            ],
        )
        .map_err(|err| format!("新增價格失敗: {err}"))?;
    }

    tx.commit()
        .map_err(|err| format!("價格寫入提交失敗: {err}"))?;
    Ok(())
}

#[tauri::command]
fn import_prices_csv(
    app: AppHandle,
    rows: Vec<PriceImportPayload>,
) -> Result<ImportTransactionsResult, String> {
    let mut conn = open_conn(&app)?;
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| format!("開始價格匯入失敗: {err}"))?;

    let mut inserted = 0usize;
    for row in rows {
        tx.execute(
            "
            INSERT INTO price_snapshots (security_id, price_date, close_price, currency, source)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(security_id, price_date)
            DO UPDATE SET
              close_price = excluded.close_price,
              currency = excluded.currency,
              source = excluded.source
        ",
            params![
                row.security_id,
                row.price_date,
                row.close_price,
                row.currency,
                row.source
            ],
        )
        .map_err(|err| format!("寫入價格資料失敗: {err}"))?;
        inserted += 1;
    }

    tx.commit()
        .map_err(|err| format!("價格匯入提交失敗: {err}"))?;
    Ok(ImportTransactionsResult { inserted })
}

fn import_tdcc_intermediate_tx(
    conn: &mut Connection,
    payload: TdccIntermediateImportPayload,
) -> Result<TdccIntermediateImportResult, String> {
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| format!("開始中介匯入失敗: {err}"))?;

    let run_key = format!(
        "tdcc-{}",
        Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_else(|| Utc::now().timestamp_micros() * 1000)
    );
    tx.execute(
        "
        INSERT INTO tdcc_import_runs (run_key, summary_json)
        VALUES (?1, ?2)
    ",
        params![&run_key, payload.import_summary_json],
    )
    .map_err(|err| format!("寫入匯入 run 失敗: {err}"))?;

    clear_bootstrap_formal_rows_tx(&tx)?;

    tx.execute("DELETE FROM tdcc_holdings_seed", [])
        .map_err(|err| format!("清除舊 holdings seed 失敗: {err}"))?;
    tx.execute("DELETE FROM tdcc_transaction_drafts", [])
        .map_err(|err| format!("清除舊 transaction draft 失敗: {err}"))?;
    tx.execute("DELETE FROM tdcc_skipped_events", [])
        .map_err(|err| format!("清除舊 skipped events 失敗: {err}"))?;

    let mut accounts_seeded = 0usize;
    let mut accounts_reused = 0usize;
    let mut mappings_upserted = 0usize;
    let mut account_cache: HashMap<String, i64> = HashMap::new();

    for account in payload.accounts {
        let account_name = normalize_required_text(account.account_name, "account_name")?;
        let broker_name = normalize_required_text(account.broker_name, "broker_name")?;
        let account_no = normalize_required_text(account.account_no, "account_no")?;
        let source = normalize_required_text(account.source, "source")?;
        let source_note = normalize_optional_text(account.source_note);

        let resolved =
            resolve_tdcc_account_id_tx(&tx, &account_no, &broker_name, &account_name, &source)?;
        let account_id = if let Some(id) = resolved {
            accounts_reused += 1;
            id
        } else {
            tx.execute(
                "
                INSERT INTO accounts
                  (name, broker, currency, bank_name, bank_branch, bank_account_last4, settlement_account_note, is_primary, note, is_archived)
                VALUES
                  (?1, ?2, 'TWD', NULL, NULL, NULL, NULL, 0, ?3, 0)
            ",
                params![
                    account_name,
                    broker_name,
                    source_note
                        .clone()
                        .unwrap_or_else(|| "bootstrap 匯入建立".to_string())
                ],
            )
            .map_err(|err| format!("建立匯入帳戶失敗: {err}"))?;
            accounts_seeded += 1;
            tx.last_insert_rowid()
        };

        tx.execute(
            "
            INSERT INTO tdcc_account_mappings
              (account_id, broker_name, account_name, account_no, source, source_note, updated_at)
            VALUES
              (?1, ?2, ?3, ?4, ?5, ?6, CURRENT_TIMESTAMP)
            ON CONFLICT(account_no, source)
            DO UPDATE SET
              account_id = excluded.account_id,
              broker_name = excluded.broker_name,
              account_name = excluded.account_name,
              source_note = excluded.source_note,
              updated_at = CURRENT_TIMESTAMP
        ",
            params![
                account_id,
                broker_name,
                account_name,
                account_no,
                source,
                source_note
            ],
        )
        .map_err(|err| format!("寫入匯入帳戶映射失敗: {err}"))?;
        mappings_upserted += 1;
        account_cache.insert(account_no, account_id);
    }

    let mut warnings: Vec<String> = Vec::new();
    let mut holding_targets: HashMap<(i64, i64), BootstrapHoldingTarget> = HashMap::new();
    let mut holdings_seeded = 0usize;
    for holding in payload.holdings {
        let account_no = normalize_required_text(holding.account_no, "holdings.account_no")?;
        let broker_name = normalize_required_text(holding.broker_name, "holdings.broker_name")?;
        let account_name = normalize_required_text(holding.account_name, "holdings.account_name")?;
        let source = normalize_required_text(holding.source, "holdings.source")?;
        let source_pdf = normalize_optional_text(holding.source_pdf);
        let snapshot_date =
            normalize_required_text(holding.snapshot_date, "holdings.snapshot_date")?;
        let symbol = normalize_required_text(holding.symbol, "holdings.symbol")?
            .trim()
            .to_uppercase();
        let name = normalize_required_text(holding.name, "holdings.name")?;

        let account_id = if let Some(id) = account_cache.get(&account_no) {
            *id
        } else if let Some(id) =
            resolve_tdcc_account_id_tx(&tx, &account_no, &broker_name, &account_name, &source)?
        {
            account_cache.insert(account_no.clone(), id);
            id
        } else {
            return Err(format!(
                "holdings seed 找不到帳戶對應：{broker_name} / {account_name} / {account_no}"
            ));
        };

        if holding.quantity < 0.0 {
            return Err(format!(
                "holdings seed 數量不可為負數：{symbol} / {}",
                holding.quantity
            ));
        }

        let security_id = ensure_security_for_seed_tx(&tx, &symbol, &name)?;
        tx.execute(
            "
            INSERT INTO tdcc_holdings_seed
              (run_key, account_id, security_id, quantity, snapshot_date, close_price, market_value, source, source_pdf)
            VALUES
              (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
        ",
            params![
                &run_key,
                account_id,
                security_id,
                holding.quantity,
                snapshot_date,
                holding.close_price,
                holding.market_value,
                source,
                source_pdf
            ],
        )
        .map_err(|err| format!("寫入 holdings seed 失敗: {err}"))?;
        if let Some(close_price) = holding.close_price {
            upsert_bootstrap_price_snapshot_tx(
                &tx,
                &run_key,
                security_id,
                &snapshot_date,
                close_price,
            )?;
        }
        let key = (account_id, security_id);
        if let Some(existing) = holding_targets.get_mut(&key) {
            existing.quantity += holding.quantity;
            if snapshot_date >= existing.snapshot_date {
                existing.snapshot_date = snapshot_date.clone();
                existing.close_price = holding.close_price.or(existing.close_price);
            } else if existing.close_price.is_none() {
                existing.close_price = holding.close_price;
            }
        } else {
            holding_targets.insert(
                key,
                BootstrapHoldingTarget {
                    quantity: holding.quantity,
                    snapshot_date: snapshot_date.clone(),
                    close_price: holding.close_price,
                },
            );
        }
        holdings_seeded += 1;
    }

    let mut draft_imported = 0usize;
    let mut draft_order = 0usize;
    let mut draft_events_by_scope: HashMap<(i64, i64), Vec<BootstrapDraftEvent>> = HashMap::new();
    for draft in payload.drafts {
        let account_no = normalize_required_text(draft.account_no, "draft.account_no")?;
        let broker_name = normalize_required_text(draft.broker_name, "draft.broker_name")?;
        let account_name = normalize_required_text(draft.account_name, "draft.account_name")?;
        let source = normalize_required_text(draft.source, "draft.source")?;
        let trade_date = normalize_required_text(draft.trade_date, "draft.trade_date")?;
        let side = normalize_required_text(draft.side, "draft.side")?.to_uppercase();
        if side != "BUY" && side != "SELL" {
            return Err(format!("transaction_draft 只允許 BUY/SELL，收到 {side}"));
        }
        if draft.quantity <= 0.0 {
            return Err(format!(
                "transaction_draft 數量必須大於 0：{} / {}",
                draft.symbol, draft.quantity
            ));
        }
        let symbol = normalize_required_text(draft.symbol, "draft.symbol")?
            .trim()
            .to_uppercase();
        let name = normalize_required_text(draft.name, "draft.name")?;
        let account_id = if let Some(id) = account_cache.get(&account_no) {
            *id
        } else if let Some(id) =
            resolve_tdcc_account_id_tx(&tx, &account_no, &broker_name, &account_name, &source)?
        {
            account_cache.insert(account_no.clone(), id);
            id
        } else {
            return Err(format!(
                "transaction_draft 找不到帳戶對應：{broker_name} / {account_name} / {account_no}"
            ));
        };
        let security_id = ensure_security_for_seed_tx(&tx, &symbol, &name)?;
        let source_memo = normalize_optional_text(draft.source_memo);
        let source_pdf = normalize_optional_text(draft.source_pdf);
        tx.execute(
            "
            INSERT INTO tdcc_transaction_drafts
              (run_key, account_id, security_id, trade_date, side, quantity, running_balance_from_pdf, source_memo, source_pdf, page_no, source)
            VALUES
              (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
        ",
            params![
                &run_key,
                account_id,
                security_id,
                trade_date,
                side,
                draft.quantity,
                draft.running_balance_from_pdf,
                source_memo.clone(),
                source_pdf.clone(),
                draft.page_no,
                source
            ],
        )
        .map_err(|err| format!("寫入 transaction draft 失敗: {err}"))?;
        draft_order += 1;
        draft_events_by_scope
            .entry((account_id, security_id))
            .or_default()
            .push(BootstrapDraftEvent {
                trade_date: trade_date.clone(),
                side: side.clone(),
                quantity: draft.quantity,
                running_balance_from_pdf: draft.running_balance_from_pdf,
                source_memo,
                source_pdf,
                page_no: draft.page_no,
                source,
                order: draft_order,
            });
        draft_imported += 1;
    }

    let mut skipped_logged = 0usize;
    let mut reason_counter: HashMap<String, usize> = HashMap::new();
    for skipped in payload.skipped_events {
        let account_no = normalize_required_text(skipped.account_no, "skipped.account_no")?;
        let broker_name = normalize_required_text(skipped.broker_name, "skipped.broker_name")?;
        let account_name = normalize_required_text(skipped.account_name, "skipped.account_name")?;
        let source = normalize_required_text(skipped.source, "skipped.source")?;
        let skip_reason = normalize_required_text(skipped.skip_reason, "skipped.skip_reason")?;
        let account_id = if let Some(id) = account_cache.get(&account_no) {
            Some(*id)
        } else {
            resolve_tdcc_account_id_tx(&tx, &account_no, &broker_name, &account_name, &source)?
        };
        tx.execute(
            "
            INSERT INTO tdcc_skipped_events
              (run_key, account_id, broker_name, account_name, account_no, trade_date, symbol, name, memo, withdrawal_qty, deposit_qty, running_balance_from_pdf, skip_reason, source_pdf, page_no, source)
            VALUES
              (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)
        ",
            params![
                &run_key,
                account_id,
                broker_name,
                account_name,
                account_no,
                normalize_optional_text(skipped.trade_date),
                normalize_optional_text(skipped.symbol).map(|s| s.to_uppercase()),
                normalize_optional_text(skipped.name),
                normalize_optional_text(skipped.memo),
                skipped.withdrawal_qty,
                skipped.deposit_qty,
                skipped.running_balance_from_pdf,
                skip_reason.clone(),
                normalize_optional_text(skipped.source_pdf),
                skipped.page_no,
                source
            ],
        )
        .map_err(|err| format!("寫入 skipped event 失敗: {err}"))?;
        skipped_logged += 1;
        *reason_counter.entry(skip_reason).or_insert(0) += 1;
    }

    let mut all_scopes = BTreeSet::new();
    all_scopes.extend(holding_targets.keys().copied());
    all_scopes.extend(draft_events_by_scope.keys().copied());

    let mut scopes_to_rebuild: Vec<(i64, i64)> = Vec::new();
    for (account_id, security_id) in all_scopes {
        let target = holding_targets.get(&(account_id, security_id));
        let target_quantity = target.map(|item| item.quantity).unwrap_or(0.0);
        let snapshot_date = target
            .map(|item| item.snapshot_date.clone())
            .unwrap_or_else(|| Utc::now().date_naive().format("%Y-%m-%d").to_string());
        let reference_price = target.and_then(|item| item.close_price).unwrap_or(0.0);

        let mut events = draft_events_by_scope
            .remove(&(account_id, security_id))
            .unwrap_or_default();
        events.sort_by(|a, b| {
            a.trade_date
                .cmp(&b.trade_date)
                .then_with(|| a.order.cmp(&b.order))
        });

        let mut running_qty = 0.0;
        let mut min_running_qty = 0.0;
        for event in &events {
            let delta = if event.side == "BUY" {
                event.quantity
            } else {
                -event.quantity
            };
            running_qty += delta;
            if running_qty < min_running_qty {
                min_running_qty = running_qty;
            }
        }

        let opening_quantity = if min_running_qty < 0.0 {
            -min_running_qty
        } else {
            0.0
        };
        if opening_quantity > 0.0000001 {
            let base_trade_date = events
                .first()
                .map(|item| item.trade_date.as_str())
                .unwrap_or(snapshot_date.as_str());
            let opening_date = day_before_or_same(base_trade_date);
            let note = format_bootstrap_trade_note(
                "期初庫存校正",
                "BOOTSTRAP_HOLDINGS_SEED",
                Some(&format!("target_quantity={target_quantity}")),
                None,
                None,
                None,
            );
            insert_bootstrap_formal_transaction_tx(
                &tx,
                &run_key,
                account_id,
                security_id,
                "BUY",
                &opening_date,
                opening_quantity,
                reference_price,
                &note,
            )?;
        }

        for event in &events {
            let note = format_bootstrap_trade_note(
                "匯入交易",
                &event.source,
                event.source_memo.as_deref(),
                event.source_pdf.as_deref(),
                event.page_no,
                event.running_balance_from_pdf,
            );
            insert_bootstrap_formal_transaction_tx(
                &tx,
                &run_key,
                account_id,
                security_id,
                &event.side,
                &event.trade_date,
                event.quantity,
                reference_price,
                &note,
            )?;
        }

        let net_draft_quantity = events.iter().fold(0.0, |sum, event| {
            if event.side == "BUY" {
                sum + event.quantity
            } else {
                sum - event.quantity
            }
        });
        let current_quantity = opening_quantity + net_draft_quantity;
        let align_delta = round_money(target_quantity - current_quantity);
        if align_delta.abs() > 0.0000001 {
            let (align_type, align_qty) = if align_delta > 0.0 {
                ("BUY", align_delta)
            } else {
                ("SELL", align_delta.abs())
            };
            let note = format_bootstrap_trade_note(
                "期末庫存對齊",
                "BOOTSTRAP_HOLDINGS_SEED",
                Some(&format!("target_quantity={target_quantity}")),
                None,
                None,
                None,
            );
            insert_bootstrap_formal_transaction_tx(
                &tx,
                &run_key,
                account_id,
                security_id,
                align_type,
                &snapshot_date,
                align_qty,
                reference_price,
                &note,
            )?;
        }

        scopes_to_rebuild.push((account_id, security_id));
    }

    rebuild_scopes_tx(&tx, &scopes_to_rebuild)?;

    let mut skipped_by_reason = reason_counter
        .into_iter()
        .map(|(reason, count)| TdccImportReasonCount { reason, count })
        .collect::<Vec<_>>();
    skipped_by_reason.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.reason.cmp(&b.reason)));

    warnings.push("這批資料已寫入正式交易資料，並保留中介檔追溯資料供核對與重匯。".to_string());

    tx.commit()
        .map_err(|err| format!("中介匯入提交失敗: {err}"))?;

    Ok(TdccIntermediateImportResult {
        run_key,
        accounts_seeded,
        accounts_reused,
        mappings_upserted,
        holdings_seeded,
        draft_imported,
        skipped_logged,
        skipped_by_reason,
        warnings,
    })
}

#[tauri::command]
fn import_tdcc_intermediate(
    app: AppHandle,
    payload: TdccIntermediateImportPayload,
) -> Result<TdccIntermediateImportResult, String> {
    let mut conn = open_conn(&app)?;
    import_tdcc_intermediate_tx(&mut conn, payload)
}

fn load_tdcc_payload_from_intermediate_tables(
    conn: &Connection,
) -> Result<TdccIntermediateImportPayload, String> {
    let mut mappings_stmt = conn
        .prepare(
            "
            SELECT account_id, broker_name, account_name, account_no, source, source_note
            FROM tdcc_account_mappings
            ORDER BY id ASC
        ",
        )
        .map_err(|err| format!("讀取匯入帳戶對照失敗: {err}"))?;
    let mapping_rows = mappings_stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, Option<String>>(5)?,
            ))
        })
        .map_err(|err| format!("解析匯入帳戶對照失敗: {err}"))?;

    let mut mappings_by_account_id: HashMap<i64, TdccAccountSeedPayload> = HashMap::new();
    for row in mapping_rows {
        let (account_id, broker_name, account_name, account_no, source, source_note) =
            row.map_err(|err| format!("解析匯入帳戶對照列失敗: {err}"))?;
        mappings_by_account_id.insert(
            account_id,
            TdccAccountSeedPayload {
                account_name,
                broker_name,
                account_no,
                source,
                source_note,
            },
        );
    }

    let accounts = mappings_by_account_id
        .values()
        .map(|item| TdccAccountSeedPayload {
            account_name: item.account_name.clone(),
            broker_name: item.broker_name.clone(),
            account_no: item.account_no.clone(),
            source: item.source.clone(),
            source_note: item.source_note.clone(),
        })
        .collect::<Vec<_>>();

    let mut holdings_stmt = conn
        .prepare(
            "
            SELECT h.account_id, s.symbol, s.name, h.quantity, h.snapshot_date, h.close_price, h.market_value, h.source, h.source_pdf
            FROM tdcc_holdings_seed h
            INNER JOIN securities s ON s.id = h.security_id
            ORDER BY h.id ASC
        ",
        )
        .map_err(|err| format!("讀取 holdings seed 失敗: {err}"))?;
    let holding_rows = holdings_stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, f64>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, Option<f64>>(5)?,
                row.get::<_, Option<f64>>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, Option<String>>(8)?,
            ))
        })
        .map_err(|err| format!("解析 holdings seed 失敗: {err}"))?;
    let mut holdings = Vec::new();
    for row in holding_rows {
        let (
            account_id,
            symbol,
            name,
            quantity,
            snapshot_date,
            close_price,
            market_value,
            source,
            source_pdf,
        ) = row.map_err(|err| format!("解析 holdings seed 列失敗: {err}"))?;
        let mapping = mappings_by_account_id
            .get(&account_id)
            .ok_or_else(|| format!("holdings seed 找不到帳戶對照: account_id={account_id}"))?;
        holdings.push(TdccHoldingSeedPayload {
            account_no: mapping.account_no.clone(),
            broker_name: mapping.broker_name.clone(),
            account_name: mapping.account_name.clone(),
            symbol,
            name,
            quantity,
            snapshot_date,
            close_price,
            market_value,
            source,
            source_pdf,
        });
    }

    let mut drafts_stmt = conn
        .prepare(
            "
            SELECT d.account_id, d.trade_date, s.symbol, s.name, d.side, d.quantity, d.running_balance_from_pdf, d.source_memo, d.source_pdf, d.page_no, d.source
            FROM tdcc_transaction_drafts d
            INNER JOIN securities s ON s.id = d.security_id
            ORDER BY d.trade_date ASC, d.id ASC
        ",
        )
        .map_err(|err| format!("讀取 transaction drafts 失敗: {err}"))?;
    let draft_rows = drafts_stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, f64>(5)?,
                row.get::<_, Option<f64>>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, Option<String>>(8)?,
                row.get::<_, Option<i64>>(9)?,
                row.get::<_, String>(10)?,
            ))
        })
        .map_err(|err| format!("解析 transaction drafts 失敗: {err}"))?;
    let mut drafts = Vec::new();
    for row in draft_rows {
        let (
            account_id,
            trade_date,
            symbol,
            name,
            side,
            quantity,
            running_balance_from_pdf,
            source_memo,
            source_pdf,
            page_no,
            source,
        ) = row.map_err(|err| format!("解析 transaction drafts 列失敗: {err}"))?;
        let mapping = mappings_by_account_id
            .get(&account_id)
            .ok_or_else(|| format!("transaction drafts 找不到帳戶對照: account_id={account_id}"))?;
        drafts.push(TdccTransactionDraftPayload {
            account_no: mapping.account_no.clone(),
            broker_name: mapping.broker_name.clone(),
            account_name: mapping.account_name.clone(),
            trade_date,
            symbol,
            name,
            side,
            quantity,
            running_balance_from_pdf,
            source_memo,
            source_pdf,
            page_no,
            source,
        });
    }

    let mut skipped_stmt = conn
        .prepare(
            "
            SELECT account_no, broker_name, account_name, trade_date, symbol, name, memo, withdrawal_qty, deposit_qty, running_balance_from_pdf, skip_reason, source_pdf, page_no, source
            FROM tdcc_skipped_events
            ORDER BY id ASC
        ",
        )
        .map_err(|err| format!("讀取 skipped events 失敗: {err}"))?;
    let skipped_rows = skipped_stmt
        .query_map([], |row| {
            Ok(TdccSkippedEventPayload {
                account_no: row.get(0)?,
                broker_name: row.get(1)?,
                account_name: row.get(2)?,
                trade_date: row.get(3)?,
                symbol: row.get(4)?,
                name: row.get(5)?,
                memo: row.get(6)?,
                withdrawal_qty: row.get(7)?,
                deposit_qty: row.get(8)?,
                running_balance_from_pdf: row.get(9)?,
                skip_reason: row.get(10)?,
                source_pdf: row.get(11)?,
                page_no: row.get(12)?,
                source: row.get(13)?,
            })
        })
        .map_err(|err| format!("解析 skipped events 失敗: {err}"))?;
    let mut skipped_events = Vec::new();
    for row in skipped_rows {
        skipped_events.push(row.map_err(|err| format!("解析 skipped events 列失敗: {err}"))?);
    }

    let import_summary_json: Option<String> = conn
        .query_row(
            "
            SELECT summary_json
            FROM tdcc_import_runs
            ORDER BY id DESC
            LIMIT 1
        ",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(|err| format!("讀取匯入摘要失敗: {err}"))?
        .flatten();

    Ok(TdccIntermediateImportPayload {
        accounts,
        holdings,
        drafts,
        skipped_events,
        import_summary_json,
    })
}

fn backfill_bootstrap_formal_from_intermediate_conn(
    conn: &mut Connection,
) -> Result<TdccBootstrapBackfillResult, String> {
    let accounts_count: i64 = conn
        .query_row("SELECT COUNT(1) FROM tdcc_account_mappings", [], |row| {
            row.get(0)
        })
        .map_err(|err| format!("讀取匯入帳戶對照數量失敗: {err}"))?;
    let holdings_count: i64 = conn
        .query_row("SELECT COUNT(1) FROM tdcc_holdings_seed", [], |row| {
            row.get(0)
        })
        .map_err(|err| format!("讀取 holdings seed 數量失敗: {err}"))?;
    let drafts_count: i64 = conn
        .query_row("SELECT COUNT(1) FROM tdcc_transaction_drafts", [], |row| {
            row.get(0)
        })
        .map_err(|err| format!("讀取 transaction draft 數量失敗: {err}"))?;
    let skipped_count: i64 = conn
        .query_row("SELECT COUNT(1) FROM tdcc_skipped_events", [], |row| {
            row.get(0)
        })
        .map_err(|err| format!("讀取 skipped events 數量失敗: {err}"))?;
    let imported_formal_count: i64 = conn
        .query_row(
            "SELECT COUNT(1) FROM tdcc_imported_formal_transactions",
            [],
            |row| row.get(0),
        )
        .map_err(|err| format!("讀取正式交易追蹤數量失敗: {err}"))?;

    if holdings_count == 0 && drafts_count == 0 {
        return Ok(TdccBootstrapBackfillResult {
            executed: false,
            reason: "no_bootstrap_seed".to_string(),
            accounts_count: accounts_count as usize,
            holdings_count: holdings_count as usize,
            drafts_count: drafts_count as usize,
            skipped_count: skipped_count as usize,
            imported_transactions: imported_formal_count as usize,
        });
    }

    if imported_formal_count > 0 {
        return Ok(TdccBootstrapBackfillResult {
            executed: false,
            reason: "already_backfilled".to_string(),
            accounts_count: accounts_count as usize,
            holdings_count: holdings_count as usize,
            drafts_count: drafts_count as usize,
            skipped_count: skipped_count as usize,
            imported_transactions: imported_formal_count as usize,
        });
    }

    let payload = load_tdcc_payload_from_intermediate_tables(conn)?;
    let result = import_tdcc_intermediate_tx(conn, payload)?;
    Ok(TdccBootstrapBackfillResult {
        executed: true,
        reason: "backfilled".to_string(),
        accounts_count: result.accounts_seeded + result.accounts_reused,
        holdings_count: result.holdings_seeded,
        drafts_count: result.draft_imported,
        skipped_count: result.skipped_logged,
        imported_transactions: result.draft_imported,
    })
}

#[tauri::command]
fn backfill_bootstrap_formal_from_intermediate(
    app: AppHandle,
) -> Result<TdccBootstrapBackfillResult, String> {
    let mut conn = open_conn(&app)?;
    backfill_bootstrap_formal_from_intermediate_conn(&mut conn)
}

fn try_backfill_bootstrap_formal_on_app_startup(app: &AppHandle) -> Result<(), String> {
    let mut conn = open_conn(app)?;
    let result = backfill_bootstrap_formal_from_intermediate_conn(&mut conn)?;
    println!(
        "[startup-bootstrap-backfill] executed={} reason={} accounts={} holdings={} drafts={} skipped={} imported={}",
        result.executed,
        result.reason,
        result.accounts_count,
        result.holdings_count,
        result.drafts_count,
        result.skipped_count,
        result.imported_transactions
    );
    Ok(())
}

#[tauri::command]
fn clear_tdcc_intermediate(app: AppHandle) -> Result<(), String> {
    let mut conn = open_conn(&app)?;
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| format!("開始清除匯入中介檔資料失敗: {err}"))?;

    clear_bootstrap_formal_rows_tx(&tx)?;

    tx.execute("DELETE FROM tdcc_holdings_seed", [])
        .map_err(|err| format!("清除 tdcc_holdings_seed 失敗: {err}"))?;
    tx.execute("DELETE FROM tdcc_transaction_drafts", [])
        .map_err(|err| format!("清除 tdcc_transaction_drafts 失敗: {err}"))?;
    tx.execute("DELETE FROM tdcc_skipped_events", [])
        .map_err(|err| format!("清除 tdcc_skipped_events 失敗: {err}"))?;
    tx.execute("DELETE FROM tdcc_import_runs", [])
        .map_err(|err| format!("清除 tdcc_import_runs 失敗: {err}"))?;
    tx.execute("DELETE FROM tdcc_account_mappings", [])
        .map_err(|err| format!("清除 tdcc_account_mappings 失敗: {err}"))?;

    tx.commit()
        .map_err(|err| format!("提交清除匯入中介檔資料失敗: {err}"))?;
    Ok(())
}

#[tauri::command]
fn create_account(app: AppHandle, payload: CreateAccountPayload) -> Result<i64, String> {
    let mut conn = open_conn(&app)?;
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| format!("開始建立帳戶交易失敗: {err}"))?;

    let name = normalize_required_text(payload.name, "帳戶名稱")?;
    let currency = normalize_required_text(payload.currency, "幣別")?;
    let broker = normalize_optional_text(payload.broker);
    let bank_name = normalize_optional_text(payload.bank_name);
    let bank_branch = normalize_optional_text(payload.bank_branch);
    let bank_account_last4 = normalize_optional_text(payload.bank_account_last4);
    let settlement_account_note = normalize_optional_text(payload.settlement_account_note);
    let note = normalize_optional_text(payload.note);
    let is_archived = payload.is_archived.unwrap_or(false);
    let is_primary = if is_archived {
        false
    } else {
        payload.is_primary.unwrap_or(false)
    };

    validate_bank_account_last4(&bank_account_last4)?;

    if is_primary {
        tx.execute("UPDATE accounts SET is_primary = 0", [])
            .map_err(|err| format!("重設主要帳戶失敗: {err}"))?;
    }

    tx.execute(
        "
        INSERT INTO accounts
          (name, broker, currency, bank_name, bank_branch, bank_account_last4, settlement_account_note, is_primary, note, is_archived)
        VALUES
          (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
    ",
        params![
            name,
            broker,
            currency,
            bank_name,
            bank_branch,
            bank_account_last4,
            settlement_account_note,
            if is_primary { 1 } else { 0 },
            note,
            if is_archived { 1 } else { 0 }
        ],
    )
    .map_err(|err| format!("建立帳戶失敗: {err}"))?;

    let account_id = tx.last_insert_rowid();
    tx.commit()
        .map_err(|err| format!("建立帳戶提交失敗: {err}"))?;
    Ok(account_id)
}

#[tauri::command]
fn update_account(app: AppHandle, payload: UpdateAccountPayload) -> Result<(), String> {
    let mut conn = open_conn(&app)?;
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| format!("開始更新帳戶交易失敗: {err}"))?;

    let exists: Option<i64> = tx
        .query_row(
            "SELECT id FROM accounts WHERE id = ?1",
            params![payload.id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|err| format!("檢查帳戶失敗: {err}"))?;
    if exists.is_none() {
        return Err("找不到要更新的帳戶。".to_string());
    }

    let name = normalize_required_text(payload.name, "帳戶名稱")?;
    let currency = normalize_required_text(payload.currency, "幣別")?;
    let broker = normalize_optional_text(payload.broker);
    let bank_name = normalize_optional_text(payload.bank_name);
    let bank_branch = normalize_optional_text(payload.bank_branch);
    let bank_account_last4 = normalize_optional_text(payload.bank_account_last4);
    let settlement_account_note = normalize_optional_text(payload.settlement_account_note);
    let note = normalize_optional_text(payload.note);
    let is_archived = payload.is_archived.unwrap_or(false);
    let is_primary = if is_archived {
        false
    } else {
        payload.is_primary.unwrap_or(false)
    };

    validate_bank_account_last4(&bank_account_last4)?;

    if is_primary {
        tx.execute(
            "UPDATE accounts SET is_primary = 0 WHERE id <> ?1",
            params![payload.id],
        )
        .map_err(|err| format!("重設主要帳戶失敗: {err}"))?;
    }

    tx.execute(
        "
        UPDATE accounts
        SET
          name = ?1,
          broker = ?2,
          currency = ?3,
          bank_name = ?4,
          bank_branch = ?5,
          bank_account_last4 = ?6,
          settlement_account_note = ?7,
          is_primary = ?8,
          note = ?9,
          is_archived = ?10
        WHERE id = ?11
    ",
        params![
            name,
            broker,
            currency,
            bank_name,
            bank_branch,
            bank_account_last4,
            settlement_account_note,
            if is_primary { 1 } else { 0 },
            note,
            if is_archived { 1 } else { 0 },
            payload.id
        ],
    )
    .map_err(|err| format!("更新帳戶失敗: {err}"))?;

    tx.commit()
        .map_err(|err| format!("更新帳戶提交失敗: {err}"))?;
    Ok(())
}

#[allow(non_snake_case)]
#[tauri::command]
fn set_account_archived(app: AppHandle, accountId: i64, isArchived: bool) -> Result<(), String> {
    let mut conn = open_conn(&app)?;
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| format!("開始封存帳戶交易失敗: {err}"))?;

    let updated = tx
        .execute(
            "
            UPDATE accounts
            SET
              is_archived = ?1,
              is_primary = CASE WHEN ?1 = 1 THEN 0 ELSE is_primary END
            WHERE id = ?2
        ",
            params![if isArchived { 1 } else { 0 }, accountId],
        )
        .map_err(|err| format!("更新帳戶封存狀態失敗: {err}"))?;
    if updated == 0 {
        return Err("找不到要封存的帳戶。".to_string());
    }

    tx.commit()
        .map_err(|err| format!("更新帳戶封存提交失敗: {err}"))?;
    Ok(())
}

#[allow(non_snake_case)]
#[tauri::command]
fn delete_account(app: AppHandle, accountId: i64) -> Result<DeleteAccountResult, String> {
    let mut conn = open_conn(&app)?;
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| format!("開始刪除帳戶交易失敗: {err}"))?;

    let exists: Option<i64> = tx
        .query_row(
            "SELECT id FROM accounts WHERE id = ?1",
            params![accountId],
            |row| row.get(0),
        )
        .optional()
        .map_err(|err| format!("檢查帳戶失敗: {err}"))?;
    if exists.is_none() {
        return Err("找不到要刪除的帳戶。".to_string());
    }

    let transaction_count: i64 = tx
        .query_row(
            "SELECT COUNT(1) FROM transactions WHERE account_id = ?1",
            params![accountId],
            |row| row.get(0),
        )
        .map_err(|err| format!("檢查帳戶交易筆數失敗: {err}"))?;

    if transaction_count > 0 {
        tx.commit()
            .map_err(|err| format!("回傳刪除阻擋結果提交失敗: {err}"))?;
        return Ok(DeleteAccountResult {
            deleted: false,
            blocked_by_transactions: true,
            transaction_count,
        });
    }

    tx.execute("DELETE FROM accounts WHERE id = ?1", params![accountId])
        .map_err(|err| format!("刪除帳戶失敗: {err}"))?;
    tx.commit()
        .map_err(|err| format!("刪除帳戶提交失敗: {err}"))?;

    Ok(DeleteAccountResult {
        deleted: true,
        blocked_by_transactions: false,
        transaction_count: 0,
    })
}

#[allow(non_snake_case)]
#[tauri::command]
fn delete_holding_transactions(
    app: AppHandle,
    accountId: i64,
    securityId: i64,
) -> Result<DeleteHoldingResult, String> {
    let mut conn = open_conn(&app)?;
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| format!("開始刪除持股交易失敗: {err}"))?;

    let transaction_count: i64 = tx
        .query_row(
            "SELECT COUNT(1) FROM transactions WHERE account_id = ?1 AND security_id = ?2",
            params![accountId, securityId],
            |row| row.get(0),
        )
        .map_err(|err| format!("檢查持股交易筆數失敗: {err}"))?;

    if transaction_count <= 0 {
        tx.commit()
            .map_err(|err| format!("回傳空刪除結果提交失敗: {err}"))?;
        return Ok(DeleteHoldingResult {
            deleted_transaction_count: 0,
        });
    }

    tx.execute(
        "
        DELETE FROM cash_transactions
        WHERE related_transaction_id IN (
          SELECT id FROM transactions WHERE account_id = ?1 AND security_id = ?2
        )
        ",
        params![accountId, securityId],
    )
    .map_err(|err| format!("刪除持股關聯帳戶往來明細失敗: {err}"))?;

    tx.execute(
        "
        DELETE FROM sell_allocations
        WHERE sell_transaction_id IN (
          SELECT id FROM transactions WHERE account_id = ?1 AND security_id = ?2
        )
        OR buy_transaction_id IN (
          SELECT id FROM transactions WHERE account_id = ?1 AND security_id = ?2
        )
        ",
        params![accountId, securityId],
    )
    .map_err(|err| format!("刪除持股 allocation 失敗: {err}"))?;

    tx.execute(
        "DELETE FROM holding_pledge_profiles WHERE account_id = ?1 AND security_id = ?2",
        params![accountId, securityId],
    )
    .map_err(|err| format!("刪除持股質押設定失敗: {err}"))?;

    tx.execute(
        "DELETE FROM transactions WHERE account_id = ?1 AND security_id = ?2",
        params![accountId, securityId],
    )
    .map_err(|err| format!("刪除持股交易失敗: {err}"))?;

    rebuild_scopes_tx(&tx, &[(accountId, securityId)])?;

    tx.commit()
        .map_err(|err| format!("刪除持股提交失敗: {err}"))?;

    Ok(DeleteHoldingResult {
        deleted_transaction_count: transaction_count,
    })
}

#[tauri::command]
fn backup_database(app: AppHandle) -> Result<String, String> {
    let path = current_db_path(&app)?;
    if !path.exists() {
        return Err("找不到資料庫檔案，請先建立資料後再備份。".to_string());
    }
    let snapshot_root = std::env::temp_dir().join(format!(
        "stock-vault-db-export-{}",
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    fs::create_dir_all(&snapshot_root).map_err(|err| format!("建立資料庫匯出暫存資料夾失敗: {err}"))?;
    let snapshot_path = snapshot_root.join("stock-vault.export.db");
    let result = (|| -> Result<String, String> {
        create_sqlite_snapshot(&path, &snapshot_path)?;
        let bytes = fs::read(&snapshot_path).map_err(|err| format!("讀取資料庫快照失敗: {err}"))?;
        Ok(general_purpose::STANDARD.encode(bytes))
    })();
    let _ = fs::remove_dir_all(&snapshot_root);
    result
}

fn current_app_bundle_path() -> Result<PathBuf, String> {
    let current_exe =
        std::env::current_exe().map_err(|err| format!("無法取得目前 App 路徑: {err}"))?;
    if let Some(macos_dir) = current_exe.parent() {
        if let Some(contents_dir) = macos_dir.parent() {
            if let Some(app_dir) = contents_dir.parent() {
                if app_dir.extension().map(|ext| ext == "app").unwrap_or(false) {
                    return Ok(app_dir.to_path_buf());
                }
            }
        }
    }

    let fallback = dirs_home_dir()
        .map(|home| home.join("Applications/stock-vault.app"))
        .filter(|path| path.exists());
    fallback.ok_or_else(|| "找不到目前可備份的 App bundle。".to_string())
}

fn dirs_home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

fn openclaw_snapshot_dir(app: &AppHandle) -> Result<PathBuf, String> {
    if let Some(portable_root) = portable_root_dir()? {
        let dir = portable_root.join("snapshots");
        fs::create_dir_all(&dir).map_err(|err| format!("建立 portable OpenClaw 輸出資料夾失敗: {err}"))?;
        return Ok(dir);
    }

    let app_data = app
        .path()
        .app_data_dir()
        .map_err(|err| format!("無法取得 AppData 目錄: {err}"))?;
    let dir = app_data.join("openclaw-snapshots");
    fs::create_dir_all(&dir).map_err(|err| format!("建立 OpenClaw 輸出資料夾失敗: {err}"))?;
    Ok(dir)
}

fn build_full_backup_info_text(
    created_at: &str,
    package_path: &str,
    stock_vault_db_path: &str,
    history_db_path: &str,
    app_bundle_path: &str,
    snapshot_dir: &str,
) -> String {
    [
        format!("format=stock-vault.full-backup.v1"),
        format!("created_at={created_at}"),
        format!("package_path={package_path}"),
        format!("app_version={}", env!("CARGO_PKG_VERSION")),
        format!("stock_vault_db_path={stock_vault_db_path}"),
        format!("history_db_path={history_db_path}"),
        format!("app_bundle_path={app_bundle_path}"),
        format!("openclaw_snapshot_dir={snapshot_dir}"),
    ]
    .join("\n")
}

fn backup_root_dir(app: &AppHandle) -> Result<PathBuf, String> {
    let dir = if let Some(portable_root) = portable_root_dir()? {
        portable_root.join("backups")
    } else {
        app.path()
            .app_data_dir()
            .map_err(|err| format!("無法取得備份資料夾: {err}"))?
            .join("backups")
    };
    fs::create_dir_all(&dir).map_err(|err| format!("建立備份資料夾失敗: {err}"))?;
    Ok(dir)
}

fn settings_backup_status_path(app: &AppHandle) -> Result<PathBuf, String> {
    Ok(backup_root_dir(app)?.join(SETTINGS_BACKUP_STATUS_FILE))
}

fn write_settings_backup_status(
    app: &AppHandle,
    metadata: &SettingsBackupStatusMetadata,
) -> Result<(), String> {
    let path = settings_backup_status_path(app)?;
    let content = serde_json::to_vec_pretty(metadata)
        .map_err(|err| format!("序列化最近備份資訊失敗: {err}"))?;
    fs::write(&path, content).map_err(|err| format!("寫入最近備份資訊失敗: {err}"))?;
    Ok(())
}

fn read_settings_backup_status(
    app: &AppHandle,
) -> Result<Option<SettingsBackupStatusMetadata>, String> {
    let path = settings_backup_status_path(app)?;
    if !path.exists() {
        return Ok(None);
    }
    let content = fs::read_to_string(&path).map_err(|err| format!("讀取最近備份資訊失敗: {err}"))?;
    let metadata: SettingsBackupStatusMetadata = serde_json::from_str(&content)
        .map_err(|err| format!("解析最近備份資訊失敗: {err}"))?;
    Ok(Some(metadata))
}

fn sqlite_sidecar_paths(path: &Path) -> Result<[PathBuf; 2], String> {
    let file_name = path
        .file_name()
        .ok_or_else(|| format!("無法取得 SQLite 檔名：{}", path.display()))?
        .to_string_lossy()
        .to_string();
    Ok([
        path.with_file_name(format!("{file_name}-wal")),
        path.with_file_name(format!("{file_name}-shm")),
    ])
}

fn remove_sqlite_sidecar_files(path: &Path) -> Result<(), String> {
    for sidecar in sqlite_sidecar_paths(path)? {
        if sidecar.exists() {
            fs::remove_file(&sidecar)
                .map_err(|err| format!("移除 SQLite sidecar 檔失敗（{}）: {err}", sidecar.display()))?;
        }
    }
    Ok(())
}

fn sqlite_literal(path: &Path) -> String {
    path.to_string_lossy().replace('\'', "''")
}

fn create_sqlite_snapshot(source_path: &Path, snapshot_path: &Path) -> Result<(), String> {
    if !source_path.exists() {
        return Err(format!("找不到 SQLite 資料庫：{}", source_path.display()));
    }
    if snapshot_path.exists() {
        fs::remove_file(snapshot_path)
            .map_err(|err| format!("移除舊的 SQLite 暫存快照失敗: {err}"))?;
    }

    let conn = Connection::open(source_path)
        .map_err(|err| format!("開啟 SQLite 資料庫以建立備份快照失敗: {err}"))?;
    conn.busy_timeout(Duration::from_millis(5000))
        .map_err(|err| format!("設定 SQLite 快照 busy timeout 失敗: {err}"))?;
    conn.execute_batch("PRAGMA busy_timeout = 5000; PRAGMA wal_checkpoint(TRUNCATE);")
        .map_err(|err| format!("執行 SQLite 快照前 checkpoint 失敗: {err}"))?;
    let sql = format!("VACUUM INTO '{}';", sqlite_literal(snapshot_path));
    conn.execute_batch(&sql)
        .map_err(|err| format!("建立 SQLite 快照失敗: {err}"))?;
    Ok(())
}

fn replace_sqlite_database_from_file(source_path: &Path, target_path: &Path) -> Result<(), String> {
    if !source_path.exists() {
        return Err(format!(
            "找不到待還原的 SQLite 快照：{}",
            source_path.display()
        ));
    }
    if let Some(parent) = target_path.parent() {
        fs::create_dir_all(parent).map_err(|err| format!("建立 SQLite 目標資料夾失敗: {err}"))?;
    }

    let temp_target = target_path.with_file_name(format!(
        "{}.restore.tmp",
        target_path
            .file_name()
            .ok_or_else(|| format!("無法取得 SQLite 目標檔名：{}", target_path.display()))?
            .to_string_lossy()
    ));
    if temp_target.exists() {
        let _ = fs::remove_file(&temp_target);
    }

    remove_sqlite_sidecar_files(target_path)?;
    fs::copy(source_path, &temp_target).map_err(|err| {
        format!(
            "複製 SQLite 暫存還原檔失敗（{} -> {}）: {err}",
            source_path.display(),
            temp_target.display()
        )
    })?;
    if target_path.exists() {
        fs::remove_file(target_path)
            .map_err(|err| format!("移除舊 SQLite 檔失敗（{}）: {err}", target_path.display()))?;
    }
    fs::rename(&temp_target, target_path)
        .map_err(|err| format!("覆寫 SQLite 檔失敗（{}）: {err}", target_path.display()))?;
    remove_sqlite_sidecar_files(target_path)?;
    Ok(())
}

fn append_bytes_to_tar<W: Write>(
    builder: &mut Builder<W>,
    entry_path: &str,
    bytes: &[u8],
) -> Result<(), String> {
    let mut header = tar::Header::new_gnu();
    header.set_size(bytes.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    builder
        .append_data(&mut header, entry_path, Cursor::new(bytes))
        .map_err(|err| format!("寫入 {entry_path} 失敗: {err}"))
}

fn read_table_count(conn: &Connection, table_name: &str) -> Result<i64, String> {
    let sql = format!("SELECT COUNT(*) FROM {table_name}");
    conn.query_row(&sql, [], |row| row.get(0))
        .map_err(|err| format!("讀取 {table_name} 筆數失敗: {err}"))
}

fn read_app_settings_snapshot(conn: &Connection) -> Result<Vec<Value>, String> {
    let mut stmt = conn
        .prepare("SELECT key, value, updated_at FROM app_settings ORDER BY key")
        .map_err(|err| format!("讀取 app_settings 失敗: {err}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|err| format!("查詢 app_settings 失敗: {err}"))?;
    let mut result = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|err| format!("讀取 app_settings 資料列失敗: {err}"))?
    {
        let key: String = row.get(0).map_err(|err| format!("讀取 setting key 失敗: {err}"))?;
        let value: String = row
            .get(1)
            .map_err(|err| format!("讀取 setting value 失敗: {err}"))?;
        let updated_at: String = row
            .get(2)
            .map_err(|err| format!("讀取 setting updated_at 失敗: {err}"))?;
        result.push(json!({
            "key": key,
            "value": value,
            "updatedAt": updated_at,
        }));
    }
    Ok(result)
}

fn ensure_sqlite_header(path: &Path) -> Result<(), String> {
    let bytes = fs::read(path).map_err(|err| format!("讀取 SQLite 檔案失敗: {err}"))?;
    if bytes.len() < 16 || &bytes[..16] != b"SQLite format 3\0" {
        return Err(format!("檔案不是有效的 SQLite 檔案：{}", path.display()));
    }
    Ok(())
}

fn read_sqlite_schema_version_number(conn: &Connection) -> Result<i64, String> {
    let has_migration_table: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '_sqlx_migrations'",
            [],
            |row| row.get(0),
        )
        .map_err(|err| format!("檢查 migration 紀錄表失敗: {err}"))?;
    if has_migration_table == 0 {
        return Err("資料庫缺少 _sqlx_migrations，無法確認 schema 版本。".to_string());
    }

    let version: Option<i64> = conn
        .query_row(
            "SELECT MAX(version) FROM _sqlx_migrations WHERE success = 1",
            [],
            |row| row.get(0),
        )
        .map_err(|err| format!("讀取 schema 版本失敗: {err}"))?;
    version.ok_or_else(|| "資料庫尚未記錄已套用的 schema 版本。".to_string())
}

fn normalize_archive_relative_path(path: &Path) -> Result<PathBuf, String> {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(segment) => normalized.push(segment),
            _ => {
                return Err(format!(
                    "備份包包含不安全路徑，已拒絕處理：{}",
                    path.display()
                ));
            }
        }
    }

    if normalized.as_os_str().is_empty() {
        return Err("備份包包含空路徑項目，已拒絕處理。".to_string());
    }
    Ok(normalized)
}

fn unpack_backup_archive(backup_path: &Path, destination_root: &Path) -> Result<(), String> {
    let file = File::open(backup_path).map_err(|err| format!("開啟備份檔失敗: {err}"))?;
    let decoder = GzDecoder::new(file);
    let mut archive = Archive::new(decoder);
    let entries = archive
        .entries()
        .map_err(|err| format!("讀取備份封裝內容失敗: {err}"))?;

    for entry_result in entries {
        let mut entry = entry_result.map_err(|err| format!("讀取備份封裝項目失敗: {err}"))?;
        let raw_path = entry
            .path()
            .map_err(|err| format!("讀取備份項目路徑失敗: {err}"))?;
        let relative_path = normalize_archive_relative_path(&raw_path)?;
        let target_path = destination_root.join(&relative_path);
        let entry_type = entry.header().entry_type();

        if entry_type.is_dir() {
            fs::create_dir_all(&target_path)
                .map_err(|err| format!("建立備份資料夾失敗（{}）: {err}", target_path.display()))?;
            continue;
        }
        if !entry_type.is_file() {
            return Err(format!(
                "備份包包含不支援的項目類型，已拒絕處理：{}",
                relative_path.display()
            ));
        }
        if let Some(parent) = target_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|err| format!("建立備份父資料夾失敗（{}）: {err}", parent.display()))?;
        }
        entry
            .unpack(&target_path)
            .map_err(|err| format!("解開備份項目失敗（{}）: {err}", relative_path.display()))?;
    }

    Ok(())
}

fn build_settings_backup_manifest(
    app: &AppHandle,
    created_at: &str,
    current_db_path: &Path,
    app_settings_snapshot: &[Value],
    snapshot_db_path: &Path,
) -> Result<SettingsBackupManifest, String> {
    let conn = Connection::open_with_flags(snapshot_db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|err| format!("開啟 SQLite 快照失敗: {err}"))?;
    let schema_version_number = read_sqlite_schema_version_number(&conn)?;
    let summary = SettingsBackupSummary {
        app_settings_count: app_settings_snapshot.len(),
        account_count: read_table_count(&conn, "accounts")?,
        security_count: read_table_count(&conn, "securities")?,
        transaction_count: read_table_count(&conn, "transactions")?,
        price_snapshot_count: read_table_count(&conn, "price_snapshots")?,
    };

    Ok(SettingsBackupManifest {
        format_version: SETTINGS_BACKUP_FORMAT_VERSION.to_string(),
        created_at: created_at.to_string(),
        app_version: app.package_info().version.to_string(),
        bundle_identifier: "com.riosbot.stock-vault".to_string(),
        schema_version: format!("stock-vault.sqlite.v{schema_version_number}"),
        schema_version_number,
        entries: SettingsBackupEntries {
            sqlite_db: SETTINGS_BACKUP_SQLITE_ENTRY.to_string(),
            app_settings_json: SETTINGS_BACKUP_APP_SETTINGS_ENTRY.to_string(),
        },
        includes: SettingsBackupIncludes {
            sqlite_db: true,
            app_settings_snapshot: true,
        },
        summary,
        source: SettingsBackupSource {
            current_db_path: current_db_path.to_string_lossy().to_string(),
            portable_mode: portable_root_dir()?.is_some(),
        },
    })
}

fn build_settings_backup_readme(manifest: &SettingsBackupManifest) -> String {
    [
        "Stock Vault 設定備份包".to_string(),
        "".to_string(),
        "用途：供 Settings / Backup 匯出與還原使用。".to_string(),
        format!("格式版本：{}", manifest.format_version),
        format!("建立時間：{}", manifest.created_at),
        format!("App 版本：{}", manifest.app_version),
        format!("SQLite schema：{}", manifest.schema_version),
        format!("主資料庫：{}", manifest.entries.sqlite_db),
        format!("設定快照：{}", manifest.entries.app_settings_json),
        format!("設定筆數：{}", manifest.summary.app_settings_count),
        format!("帳戶數：{}", manifest.summary.account_count),
        format!("股票主檔數：{}", manifest.summary.security_count),
        format!("交易筆數：{}", manifest.summary.transaction_count),
        format!("價格快照筆數：{}", manifest.summary.price_snapshot_count),
        "".to_string(),
        "若備份包 schema 與目前程式版本不一致，系統會阻擋還原。".to_string(),
    ]
    .join("\n")
}

fn inspect_settings_backup_package(
    _app: &AppHandle,
    backup_path: &Path,
) -> Result<Value, String> {
    if !backup_path.exists() {
        return Err(format!("找不到備份檔：{}", backup_path.display()));
    }

    let file_size_bytes = fs::metadata(backup_path)
        .map_err(|err| format!("讀取備份檔大小失敗: {err}"))?
        .len();
    let current_schema_version_number = current_sqlite_schema_version_number();
    let current_schema_version = current_sqlite_schema_version_label();

    let inspect_root = std::env::temp_dir().join(format!(
        "stock-vault-settings-backup-inspect-{}",
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    fs::create_dir_all(&inspect_root).map_err(|err| format!("建立備份檢查暫存資料夾失敗: {err}"))?;

    let inspection = (|| -> Result<Value, String> {
        unpack_backup_archive(backup_path, &inspect_root)?;

        let manifest_path = inspect_root.join(SETTINGS_BACKUP_MANIFEST_ENTRY);
        if !manifest_path.exists() {
            return Err(format!(
                "備份包缺少 {}，不是可辨識的 Stock Vault 設定備份包。",
                SETTINGS_BACKUP_MANIFEST_ENTRY
            ));
        }
        let manifest_text = fs::read_to_string(&manifest_path)
            .map_err(|err| format!("讀取備份 manifest 失敗: {err}"))?;
        let manifest: SettingsBackupManifest = serde_json::from_str(&manifest_text)
            .map_err(|err| format!("解析備份 manifest 失敗: {err}"))?;

        let mut blocking_reasons = Vec::new();
        let mut warnings = Vec::new();

        if manifest.format_version != SETTINGS_BACKUP_FORMAT_VERSION {
            blocking_reasons.push(format!(
                "備份格式不支援：{}。目前只支援 {}。",
                manifest.format_version, SETTINGS_BACKUP_FORMAT_VERSION
            ));
        }

        let sqlite_entry_path = inspect_root.join(&manifest.entries.sqlite_db);
        if !sqlite_entry_path.exists() {
            blocking_reasons.push(format!(
                "備份包缺少 {}，無法還原。",
                manifest.entries.sqlite_db
            ));
        }

        let app_settings_entry_path = inspect_root.join(&manifest.entries.app_settings_json);
        if !app_settings_entry_path.exists() {
            warnings.push(format!(
                "備份包缺少 {}，但主 SQLite 仍可檢查。",
                manifest.entries.app_settings_json
            ));
        }

        let mut detected_schema_version_number: Option<i64> = None;
        let mut detected_schema_version: Option<String> = None;
        let mut app_settings_count: Option<usize> = None;
        let mut app_setting_keys: Vec<String> = Vec::new();

        if sqlite_entry_path.exists() {
            ensure_sqlite_header(&sqlite_entry_path)?;
            let conn = Connection::open_with_flags(&sqlite_entry_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
                .map_err(|err| format!("開啟備份 SQLite 失敗: {err}"))?;
            let schema_version_number = read_sqlite_schema_version_number(&conn)?;
            detected_schema_version_number = Some(schema_version_number);
            detected_schema_version = Some(format!("stock-vault.sqlite.v{schema_version_number}"));

            if schema_version_number != manifest.schema_version_number {
                blocking_reasons.push(format!(
                    "備份 manifest 宣告 schema 為 {}，但 SQLite 內容實際為 stock-vault.sqlite.v{}，資料不一致。",
                    manifest.schema_version, schema_version_number
                ));
            }

            if schema_version_number != current_schema_version_number {
                blocking_reasons.push(format!(
                    "備份 schema 為 stock-vault.sqlite.v{}，目前程式需要 {}，已阻擋還原。",
                    schema_version_number, current_schema_version
                ));
            }

            let settings_snapshot = read_app_settings_snapshot(&conn)?;
            app_setting_keys = settings_snapshot
                .iter()
                .filter_map(|item| item.get("key").and_then(|value| value.as_str()))
                .map(str::to_string)
                .collect();
            app_settings_count = Some(settings_snapshot.len());
        }

        let restorable = blocking_reasons.is_empty();
        Ok(json!({
            "backupPath": backup_path.to_string_lossy().to_string(),
            "fileName": backup_path.file_name().map(|name| name.to_string_lossy().to_string()).unwrap_or_else(|| backup_path.to_string_lossy().to_string()),
            "fileExists": true,
            "fileSizeBytes": file_size_bytes,
            "formatVersion": manifest.format_version,
            "createdAt": manifest.created_at,
            "appVersion": manifest.app_version,
            "schemaVersion": detected_schema_version.clone().unwrap_or(manifest.schema_version.clone()),
            "schemaVersionNumber": detected_schema_version_number.unwrap_or(manifest.schema_version_number),
            "currentSchemaVersion": current_schema_version,
            "currentSchemaVersionNumber": current_schema_version_number,
            "schemaCompatible": detected_schema_version_number == Some(current_schema_version_number),
            "restorable": restorable,
            "blockingReasons": blocking_reasons,
            "warnings": warnings,
            "includes": {
                "sqliteDb": manifest.includes.sqlite_db,
                "appSettingsSnapshot": manifest.includes.app_settings_snapshot,
            },
            "appSettingsCount": app_settings_count.unwrap_or(manifest.summary.app_settings_count),
            "appSettingKeys": app_setting_keys,
            "summary": {
                "accountCount": manifest.summary.account_count,
                "securityCount": manifest.summary.security_count,
                "transactionCount": manifest.summary.transaction_count,
                "priceSnapshotCount": manifest.summary.price_snapshot_count,
            }
        }))
    })();

    let _ = fs::remove_dir_all(&inspect_root);
    inspection
}

#[tauri::command]
fn get_settings_backup_status(app: AppHandle) -> Result<Value, String> {
    let backup_dir = backup_root_dir(&app)?;
    let last_backup = read_settings_backup_status(&app)?.map(|metadata| {
        json!({
            "backupPath": metadata.backup_path,
            "createdAt": metadata.created_at,
            "fileSizeBytes": metadata.file_size_bytes,
            "formatVersion": metadata.format_version,
            "schemaVersion": metadata.schema_version,
            "schemaVersionNumber": metadata.schema_version_number,
            "fileExists": PathBuf::from(&metadata.backup_path).exists(),
        })
    });

    Ok(json!({
        "defaultDirectory": backup_dir.to_string_lossy().to_string(),
        "currentSchemaVersion": current_sqlite_schema_version_label(),
        "currentSchemaVersionNumber": current_sqlite_schema_version_number(),
        "lastBackup": last_backup,
    }))
}

#[tauri::command]
fn inspect_settings_backup(app: AppHandle, backup_path: String) -> Result<Value, String> {
    let backup_path_buf = PathBuf::from(backup_path.trim());
    inspect_settings_backup_package(&app, &backup_path_buf)
}

#[tauri::command]
fn create_settings_backup(app: AppHandle, backup_path: String) -> Result<Value, String> {
    let backup_path_buf = PathBuf::from(backup_path.trim());
    if backup_path_buf.as_os_str().is_empty() {
        return Err("備份路徑不可為空。".to_string());
    }
    let parent = backup_path_buf
        .parent()
        .ok_or_else(|| "備份路徑缺少父資料夾。".to_string())?;
    fs::create_dir_all(parent).map_err(|err| format!("建立備份資料夾失敗: {err}"))?;

    let current_db = current_db_path(&app)?;
    if !current_db.exists() {
        return Err("找不到 stock-vault.db，請先建立資料後再備份。".to_string());
    }

    let created_at = Utc::now().to_rfc3339();
    let snapshot_root = std::env::temp_dir().join(format!(
        "stock-vault-settings-backup-create-{}",
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    fs::create_dir_all(&snapshot_root).map_err(|err| format!("建立備份暫存資料夾失敗: {err}"))?;
    let snapshot_db_path = snapshot_root.join("stock-vault.snapshot.db");

    let result = (|| -> Result<Value, String> {
        create_sqlite_snapshot(&current_db, &snapshot_db_path)?;
        let snapshot_conn = Connection::open_with_flags(&snapshot_db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|err| format!("開啟備份快照失敗: {err}"))?;
        let app_settings_snapshot = read_app_settings_snapshot(&snapshot_conn)?;
        let manifest = build_settings_backup_manifest(
            &app,
            &created_at,
            &current_db,
            &app_settings_snapshot,
            &snapshot_db_path,
        )?;
        let manifest_bytes = serde_json::to_vec_pretty(&manifest)
            .map_err(|err| format!("序列化備份 manifest 失敗: {err}"))?;
        let settings_bytes = serde_json::to_vec_pretty(&app_settings_snapshot)
            .map_err(|err| format!("序列化 app_settings 快照失敗: {err}"))?;
        let readme_text = build_settings_backup_readme(&manifest);

        let temp_backup_path = backup_path_buf.with_extension("tmp");
        if temp_backup_path.exists() {
            let _ = fs::remove_file(&temp_backup_path);
        }
        let file = File::create(&temp_backup_path).map_err(|err| format!("建立備份檔失敗: {err}"))?;
        let encoder = GzEncoder::new(file, Compression::default());
        let mut builder = Builder::new(encoder);

        append_bytes_to_tar(&mut builder, SETTINGS_BACKUP_MANIFEST_ENTRY, &manifest_bytes)?;
        append_bytes_to_tar(
            &mut builder,
            SETTINGS_BACKUP_README_ENTRY,
            readme_text.as_bytes(),
        )?;
        append_bytes_to_tar(
            &mut builder,
            SETTINGS_BACKUP_APP_SETTINGS_ENTRY,
            &settings_bytes,
        )?;
        builder
            .append_path_with_name(&snapshot_db_path, SETTINGS_BACKUP_SQLITE_ENTRY)
            .map_err(|err| format!("寫入 {} 失敗: {err}", SETTINGS_BACKUP_SQLITE_ENTRY))?;

        builder
            .finish()
            .map_err(|err| format!("完成備份封裝失敗: {err}"))?;
        let encoder = builder
            .into_inner()
            .map_err(|err| format!("整理備份壓縮檔失敗: {err}"))?;
        encoder
            .finish()
            .map_err(|err| format!("完成 gzip 壓縮失敗: {err}"))?;

        if backup_path_buf.exists() {
            fs::remove_file(&backup_path_buf).map_err(|err| format!("移除舊備份檔失敗: {err}"))?;
        }
        fs::rename(&temp_backup_path, &backup_path_buf)
            .map_err(|err| format!("寫入備份檔失敗: {err}"))?;

        let package_size_bytes = fs::metadata(&backup_path_buf)
            .map_err(|err| format!("讀取備份檔大小失敗: {err}"))?
            .len();
        let metadata = SettingsBackupStatusMetadata {
            backup_path: backup_path_buf.to_string_lossy().to_string(),
            created_at: created_at.clone(),
            file_size_bytes: package_size_bytes,
            format_version: manifest.format_version.clone(),
            schema_version: manifest.schema_version.clone(),
            schema_version_number: manifest.schema_version_number,
        };
        write_settings_backup_status(&app, &metadata)?;

        Ok(json!({
            "backupPath": metadata.backup_path,
            "createdAt": metadata.created_at,
            "fileSizeBytes": metadata.file_size_bytes,
            "formatVersion": metadata.format_version,
            "schemaVersion": metadata.schema_version,
            "schemaVersionNumber": metadata.schema_version_number,
            "appSettingsCount": manifest.summary.app_settings_count,
        }))
    })();

    let _ = fs::remove_dir_all(&snapshot_root);
    result
}

#[tauri::command]
fn restore_settings_backup(app: AppHandle, backup_path: String) -> Result<Value, String> {
    let backup_path_buf = PathBuf::from(backup_path.trim());
    let inspection = inspect_settings_backup_package(&app, &backup_path_buf)?;
    let restorable = inspection
        .get("restorable")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    if !restorable {
        let blocking_reasons = inspection
            .get("blockingReasons")
            .and_then(|value| value.as_array())
            .map(|items| {
                items
                    .iter()
                    .filter_map(|value| value.as_str())
                    .collect::<Vec<_>>()
                    .join("；")
            })
            .unwrap_or_else(|| "備份檢查未通過。".to_string());
        return Err(format!("已阻擋還原：{blocking_reasons}"));
    }

    let restore_root = std::env::temp_dir().join(format!(
        "stock-vault-settings-backup-restore-{}",
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    fs::create_dir_all(&restore_root).map_err(|err| format!("建立還原暫存資料夾失敗: {err}"))?;

    let result = (|| -> Result<Value, String> {
        unpack_backup_archive(&backup_path_buf, &restore_root)?;
        let sqlite_path = restore_root.join(SETTINGS_BACKUP_SQLITE_ENTRY);
        ensure_sqlite_header(&sqlite_path)?;

        for target in db_candidates(&app)? {
            replace_sqlite_database_from_file(&sqlite_path, &target)?;
        }

        Ok(json!({
            "backupPath": backup_path_buf.to_string_lossy().to_string(),
            "restoredMainDb": true,
            "schemaVersion": inspection.get("schemaVersion").cloned().unwrap_or(Value::Null),
            "currentSchemaVersion": inspection.get("currentSchemaVersion").cloned().unwrap_or(Value::Null),
        }))
    })();

    let _ = fs::remove_dir_all(&restore_root);
    result
}

#[tauri::command]
fn create_full_backup(app: AppHandle, backup_path: String) -> Result<Value, String> {
    let backup_path_buf = PathBuf::from(backup_path.trim());
    if backup_path_buf.as_os_str().is_empty() {
        return Err("備份路徑不可為空。".to_string());
    }
    let parent = backup_path_buf
        .parent()
        .ok_or_else(|| "備份路徑缺少父資料夾。".to_string())?;
    fs::create_dir_all(parent).map_err(|err| format!("建立備份資料夾失敗: {err}"))?;

    let created_at = Utc::now().to_rfc3339();
    let main_db_path = current_db_path(&app)?;
    if !main_db_path.exists() {
        return Err("找不到 stock-vault.db，請先建立資料後再備份。".to_string());
    }
    let history_db_path = history_db_path(&app)?;
    let app_bundle_path = current_app_bundle_path()?;
    let snapshot_dir = openclaw_snapshot_dir(&app)?;

    let temp_path = backup_path_buf.with_extension("tmp");
    if temp_path.exists() {
        let _ = fs::remove_file(&temp_path);
    }

    let snapshot_root = std::env::temp_dir().join(format!(
        "stock-vault-full-backup-create-{}",
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    fs::create_dir_all(&snapshot_root).map_err(|err| format!("建立完整備份暫存資料夾失敗: {err}"))?;
    let main_db_snapshot_path = snapshot_root.join("stock-vault.full-backup.db");

    let result = (|| -> Result<Value, String> {
        create_sqlite_snapshot(&main_db_path, &main_db_snapshot_path)?;

        let file = File::create(&temp_path).map_err(|err| format!("建立備份檔失敗: {err}"))?;
        let encoder = GzEncoder::new(file, Compression::default());
        let mut builder = Builder::new(encoder);

        let manifest = json!({
            "formatVersion": "stock-vault.full-backup.v1",
            "createdAt": created_at,
            "appVersion": env!("CARGO_PKG_VERSION"),
            "bundleIdentifier": "com.riosbot.stock-vault",
            "schemaVersion": current_sqlite_schema_version_label(),
            "schemaVersionNumber": current_sqlite_schema_version_number(),
            "includes": {
                "appBundle": app_bundle_path.exists(),
                "stockVaultDb": true,
                "stockHistoryDb": history_db_path.exists(),
                "openClawSnapshots": snapshot_dir.exists(),
            },
            "sourcePaths": {
                "appBundlePath": app_bundle_path,
                "stockVaultDbPath": main_db_path,
                "historyDbPath": history_db_path,
                "openClawSnapshotDir": snapshot_dir,
            }
        });
        let manifest_text = serde_json::to_string_pretty(&manifest)
            .map_err(|err| format!("建立備份 manifest 失敗: {err}"))?;
        let manifest_bytes = manifest_text.as_bytes();
        let mut manifest_header = tar::Header::new_gnu();
        manifest_header.set_size(manifest_bytes.len() as u64);
        manifest_header.set_mode(0o644);
        manifest_header.set_cksum();
        builder
            .append_data(
                &mut manifest_header,
                "backup-info.json",
                Cursor::new(manifest_bytes),
            )
            .map_err(|err| format!("寫入 backup-info.json 失敗: {err}"))?;

        let info_text = build_full_backup_info_text(
            &created_at,
            &backup_path_buf.to_string_lossy(),
            &main_db_path.to_string_lossy(),
            &history_db_path.to_string_lossy(),
            &app_bundle_path.to_string_lossy(),
            &snapshot_dir.to_string_lossy(),
        );
        let info_bytes = info_text.as_bytes();
        let mut info_header = tar::Header::new_gnu();
        info_header.set_size(info_bytes.len() as u64);
        info_header.set_mode(0o644);
        info_header.set_cksum();
        builder
            .append_data(&mut info_header, "backup-info.txt", Cursor::new(info_bytes))
            .map_err(|err| format!("寫入 backup-info.txt 失敗: {err}"))?;

        builder
            .append_path_with_name(&main_db_snapshot_path, "data/stock-vault.db")
            .map_err(|err| format!("寫入 stock-vault.db 失敗: {err}"))?;
        if history_db_path.exists() {
            builder
                .append_path_with_name(&history_db_path, "data/stock-history.db")
                .map_err(|err| format!("寫入 stock-history.db 失敗: {err}"))?;
        }
        if snapshot_dir.exists() {
            builder
                .append_dir_all("data/openclaw-snapshots", &snapshot_dir)
                .map_err(|err| format!("寫入 openclaw snapshots 失敗: {err}"))?;
        }
        if app_bundle_path.exists() {
            builder
                .append_dir_all("app/stock-vault.app", &app_bundle_path)
                .map_err(|err| format!("寫入 App bundle 失敗: {err}"))?;
        }

        builder
            .finish()
            .map_err(|err| format!("完成備份封裝失敗: {err}"))?;
        let encoder = builder
            .into_inner()
            .map_err(|err| format!("整理備份壓縮檔失敗: {err}"))?;
        encoder
            .finish()
            .map_err(|err| format!("完成 gzip 壓縮失敗: {err}"))?;

        if backup_path_buf.exists() {
            fs::remove_file(&backup_path_buf).map_err(|err| format!("移除舊備份檔失敗: {err}"))?;
        }
        fs::rename(&temp_path, &backup_path_buf)
            .map_err(|err| format!("寫入備份檔失敗: {err}"))?;

        let package_size = fs::metadata(&backup_path_buf)
            .map_err(|err| format!("讀取備份檔大小失敗: {err}"))?
            .len();
        Ok(json!({
            "backupPath": backup_path_buf.to_string_lossy().to_string(),
            "createdAt": created_at,
            "formatVersion": "stock-vault.full-backup.v1",
            "packageSizeBytes": package_size,
            "includes": {
                "appBundle": app_bundle_path.exists(),
                "stockVaultDb": true,
                "stockHistoryDb": history_db_path.exists(),
                "openClawSnapshots": snapshot_dir.exists(),
            }
        }))
    })();

    let _ = fs::remove_dir_all(&snapshot_root);
    result
}

#[tauri::command]
fn restore_full_backup(app: AppHandle, backup_path: String) -> Result<Value, String> {
    let backup_path_buf = PathBuf::from(backup_path.trim());
    if !backup_path_buf.exists() {
        return Err(format!("找不到備份檔：{}", backup_path_buf.display()));
    }

    let restore_root = std::env::temp_dir().join(format!(
        "stock-vault-restore-{}",
        Utc::now().format("%Y%m%d%H%M%S")
    ));
    fs::create_dir_all(&restore_root).map_err(|err| format!("建立還原暫存資料夾失敗: {err}"))?;

    let file = File::open(&backup_path_buf).map_err(|err| format!("開啟備份檔失敗: {err}"))?;
    let decoder = GzDecoder::new(file);
    let mut archive = Archive::new(decoder);
    archive
        .unpack(&restore_root)
        .map_err(|err| format!("解開備份檔失敗: {err}"))?;

    let manifest_path = restore_root.join("backup-info.json");
    let manifest_text = fs::read_to_string(&manifest_path)
        .map_err(|err| format!("讀取 backup-info.json 失敗: {err}"))?;
    let manifest: Value = serde_json::from_str(&manifest_text)
        .map_err(|err| format!("解析 backup-info.json 失敗: {err}"))?;
    let format_version = manifest
        .get("formatVersion")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    if format_version != "stock-vault.full-backup.v1" {
        return Err(format!("不支援的完整備份格式：{format_version}"));
    }

    let restored_main_db = restore_root.join("data/stock-vault.db");
    if !restored_main_db.exists() {
        return Err("備份包缺少 data/stock-vault.db，無法還原。".to_string());
    }
    let restored_history_db = restore_root.join("data/stock-history.db");
    let restored_snapshots_dir = restore_root.join("data/openclaw-snapshots");
    let restored_app_bundle = restore_root.join("app/stock-vault.app");

    ensure_sqlite_header(&restored_main_db)?;
    for target in db_candidates(&app)? {
        replace_sqlite_database_from_file(&restored_main_db, &target)?;
    }

    let mut restored_history = false;
    if restored_history_db.exists() {
        let target = history_db_path(&app)?;
        ensure_sqlite_header(&restored_history_db)?;
        replace_sqlite_database_from_file(&restored_history_db, &target)
            .map_err(|err| format!("還原 stock-history.db 失敗: {err}"))?;
        restored_history = true;
    }

    let snapshot_target = openclaw_snapshot_dir(&app)?;
    let mut restored_snapshots = false;
    if restored_snapshots_dir.exists() {
        if snapshot_target.exists() {
            let _ = fs::remove_dir_all(&snapshot_target);
        }
        if let Some(parent) = snapshot_target.parent() {
            fs::create_dir_all(parent)
                .map_err(|err| format!("建立 snapshot 父資料夾失敗: {err}"))?;
        }
        fs::rename(&restored_snapshots_dir, &snapshot_target)
            .or_else(|_| {
                fs::create_dir_all(&snapshot_target)?;
                for entry in fs::read_dir(&restored_snapshots_dir)? {
                    let entry = entry?;
                    let to = snapshot_target.join(entry.file_name());
                    if entry.path().is_dir() {
                        fs::create_dir_all(&to)?;
                    } else {
                        fs::copy(entry.path(), to)?;
                    }
                }
                Ok(())
            })
            .map_err(|err: std::io::Error| format!("還原 openclaw snapshots 失敗: {err}"))?;
        restored_snapshots = true;
    }

    Ok(json!({
        "backupPath": backup_path_buf.to_string_lossy().to_string(),
        "formatVersion": format_version,
        "restoredMainDb": true,
        "restoredHistoryDb": restored_history,
        "restoredOpenClawSnapshots": restored_snapshots,
        "appBundleIncluded": restored_app_bundle.exists(),
        "appBundleRestoreMode": if restored_app_bundle.exists() { "package-included-use-external-script" } else { "not-included" },
    }))
}

#[tauri::command]
fn write_text_to_path(path: String, content: String) -> Result<(), String> {
    fs::write(&path, content).map_err(|err| format!("寫入檔案失敗: {err}"))?;
    Ok(())
}

#[tauri::command]
fn atomic_write_text_to_path(path: String, content: String) -> Result<(), String> {
    let temp_path = format!("{path}.tmp");
    fs::write(&temp_path, content).map_err(|err| format!("寫入暫存檔失敗: {err}"))?;
    if fs::metadata(&path).is_ok() {
        fs::remove_file(&path).map_err(|err| format!("移除舊檔失敗: {err}"))?;
    }
    fs::rename(&temp_path, &path).map_err(|err| format!("原子替換檔案失敗: {err}"))?;
    Ok(())
}

#[tauri::command]
fn read_text_from_path(path: String) -> Result<String, String> {
    fs::read_to_string(&path).map_err(|err| format!("讀取檔案失敗: {err}"))
}

#[tauri::command]
fn read_file_base64(path: String) -> Result<String, String> {
    let bytes = fs::read(&path).map_err(|err| format!("讀取檔案失敗: {err}"))?;
    Ok(general_purpose::STANDARD.encode(bytes))
}

#[allow(non_snake_case)]
#[tauri::command]
fn write_base64_to_path(path: String, dataBase64: String) -> Result<(), String> {
    let bytes = general_purpose::STANDARD
        .decode(dataBase64)
        .map_err(|err| format!("檔案資料格式錯誤: {err}"))?;
    fs::write(&path, bytes).map_err(|err| format!("寫入檔案失敗: {err}"))?;
    Ok(())
}

#[tauri::command]
fn get_openclaw_snapshot_dir(app: AppHandle) -> Result<String, String> {
    let dir = openclaw_snapshot_dir(&app)?;
    Ok(dir.to_string_lossy().to_string())
}

#[tauri::command]
fn get_runtime_storage_info(app: AppHandle) -> Result<Value, String> {
    let portable_root = portable_root_dir()?;
    let db_candidates = db_candidates(&app)?
        .into_iter()
        .map(|path| path.to_string_lossy().to_string())
        .collect::<Vec<_>>();
    let current_db = current_db_path(&app)?;
    let history_db = history_db_path(&app)?;
    let snapshot_dir = openclaw_snapshot_dir(&app)?;

    let mut startup_warnings: Vec<String> = Vec::new();
    #[cfg(target_os = "windows")]
    {
        startup_warnings.push("Windows 版需確認系統已安裝 WebView2 Runtime。".to_string());
    }

    Ok(json!({
        "platform": std::env::consts::OS,
        "portableMode": portable_root.is_some(),
        "portableRoot": portable_root.map(|path| path.to_string_lossy().to_string()),
        "dbCandidates": db_candidates,
        "currentDbPath": current_db.to_string_lossy().to_string(),
        "historyDbPath": history_db.to_string_lossy().to_string(),
        "openClawSnapshotDir": snapshot_dir.to_string_lossy().to_string(),
        "appVersion": app.package_info().version.to_string(),
        "startupWarnings": startup_warnings,
    }))
}

#[tauri::command]
fn get_app_version(app: AppHandle) -> Result<String, String> {
    Ok(app.package_info().version.to_string())
}

#[tauri::command]
fn open_folder_in_finder(path: String) -> Result<(), String> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err("路徑不可為空。".to_string());
    }

    let mut target = PathBuf::from(trimmed);
    if !target.exists() {
        return Err(format!("找不到路徑：{trimmed}"));
    }
    if target.is_file() {
        target = target
            .parent()
            .ok_or_else(|| "無法取得檔案所在資料夾。".to_string())?
            .to_path_buf();
    }

    #[cfg(target_os = "macos")]
    let status = Command::new("open")
        .arg(&target)
        .status()
        .map_err(|err| format!("呼叫系統開啟資料夾失敗: {err}"))?;

    #[cfg(target_os = "windows")]
    let status = Command::new("explorer")
        .arg(&target)
        .status()
        .map_err(|err| format!("呼叫 Windows Explorer 開啟資料夾失敗: {err}"))?;

    #[cfg(all(not(target_os = "macos"), not(target_os = "windows")))]
    let status = Command::new("xdg-open")
        .arg(&target)
        .status()
        .map_err(|err| format!("呼叫系統開啟資料夾失敗: {err}"))?;

    if !status.success() {
        return Err(format!(
            "開啟資料夾失敗，系統狀態碼：{}",
            status
                .code()
                .map(|code| code.to_string())
                .unwrap_or_else(|| "unknown".to_string())
        ));
    }

    Ok(())
}

fn response_preview(text: &str, max_chars: usize) -> String {
    text.chars().take(max_chars).collect()
}

fn parse_row_value<'a>(row: &'a Value, keys: &[&str]) -> Option<&'a str> {
    for key in keys {
        if let Some(value) = row.get(*key).and_then(Value::as_str) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Some(trimmed);
            }
        }
    }
    None
}

fn etf_category_override(symbol: &str) -> Option<&'static str> {
    match symbol {
        "00631L" => Some("槓桿型ETF"),
        "00662" => Some("海外科技ETF"),
        "00878" => Some("高股息ETF"),
        "00919" => Some("高股息ETF"),
        "00952" => Some("科技ETF"),
        "00757" => Some("海外科技ETF"),
        "00981A" => Some("主動型台股ETF"),
        "00985A" => Some("主動型台股ETF"),
        _ => None,
    }
}

fn tw_industry_code_to_label(code: &str) -> Option<&'static str> {
    match code {
        "01" => Some("水泥工業"),
        "02" => Some("食品工業"),
        "03" => Some("塑膠工業"),
        "04" => Some("紡織纖維"),
        "05" => Some("電機機械"),
        "06" => Some("電器電纜"),
        "08" => Some("玻璃陶瓷"),
        "09" => Some("造紙工業"),
        "10" => Some("鋼鐵工業"),
        "11" => Some("橡膠工業"),
        "12" => Some("汽車工業"),
        "14" => Some("建材營造"),
        "15" => Some("航運業"),
        "16" => Some("觀光餐旅"),
        "17" => Some("金融保險"),
        "18" => Some("貿易百貨"),
        "19" => Some("綜合"),
        "20" => Some("其他"),
        "21" => Some("化學工業"),
        "22" => Some("生技醫療業"),
        "23" => Some("油電燃氣業"),
        "24" => Some("半導體業"),
        "25" => Some("電腦及週邊設備業"),
        "26" => Some("光電業"),
        "27" => Some("通信網路業"),
        "28" => Some("電子零組件業"),
        "29" => Some("電子通路業"),
        "30" => Some("資訊服務業"),
        "31" => Some("其他電子業"),
        "32" => Some("文化創意業"),
        "33" => Some("農業科技業"),
        "34" => Some("電子商務"),
        "35" => Some("綠能環保"),
        "36" => Some("數位雲端"),
        "37" => Some("運動休閒"),
        "38" => Some("居家生活"),
        "80" => Some("管理股票"),
        _ => None,
    }
}

fn normalize_security_category(symbol: &str, _market: &str, raw: Option<&str>) -> Option<String> {
    if let Some(override_value) = etf_category_override(symbol) {
        return Some(override_value.to_string());
    }
    let cleaned = raw
        .map(|value| value.trim().replace('\u{3000}', " "))
        .unwrap_or_default();
    if cleaned.is_empty() {
        return None;
    }
    if cleaned.len() == 2 && cleaned.chars().all(|ch| ch.is_ascii_digit()) {
        if let Some(label) = tw_industry_code_to_label(&cleaned) {
            return Some(label.to_string());
        }
    }
    Some(cleaned)
}

fn lookup_from_source(
    client: &Client,
    url: &str,
    symbol: &str,
    market: &str,
    code_keys: &[&str],
    name_keys: &[&str],
    category_keys: &[&str],
) -> Result<(Option<TwStockMasterRecordPayload>, TwStockLookupDiagnostic), String> {
    let mut diagnostic = TwStockLookupDiagnostic {
        queried: true,
        url: url.to_string(),
        http_status: None,
        response_preview: String::new(),
        parsed_rows: 0,
        matched: false,
        error: None,
    };

    let response = client.get(url).send().map_err(|err| {
        diagnostic.error = Some(format!("網路失敗: {err}"));
        format!("NETWORK_FAILURE::{err}")
    })?;
    diagnostic.http_status = Some(response.status().as_u16());
    let status = response.status();
    let text = response
        .text()
        .map_err(|err| format!("PARSE_FAILURE::讀取回應失敗: {err}"))?;
    diagnostic.response_preview = response_preview(&text, 300);

    if !status.is_success() {
        diagnostic.error = Some(format!("HTTP {}", status.as_u16()));
        return Ok((None, diagnostic));
    }

    let parsed: Value = serde_json::from_str(&text)
        .map_err(|err| format!("PARSE_FAILURE::JSON 解析失敗: {err}"))?;
    let rows = parsed
        .as_array()
        .ok_or_else(|| "SOURCE_FORMAT_ERROR::來源資料不是陣列".to_string())?;
    diagnostic.parsed_rows = rows.len();

    for row in rows {
        let code = parse_row_value(row, code_keys).unwrap_or("").to_uppercase();
        if code != symbol {
            continue;
        }
        let name = parse_row_value(row, name_keys)
            .ok_or_else(|| "SOURCE_FORMAT_ERROR::來源缺少公司名稱欄位".to_string())?;
        diagnostic.matched = true;
        return Ok((
            Some(TwStockMasterRecordPayload {
                symbol: symbol.to_string(),
                name: name.to_string(),
                market: market.to_string(),
                currency: "TWD".to_string(),
                board_lot_size: 1000,
                category: normalize_security_category(
                    symbol,
                    market,
                    parse_row_value(row, category_keys),
                ),
            }),
            diagnostic,
        ));
    }

    Ok((None, diagnostic))
}

#[tauri::command]
fn lookup_tw_stock_master(symbol: String) -> Result<TwStockLookupResult, String> {
    let normalized_symbol = symbol.trim().to_uppercase();
    if normalized_symbol.is_empty() {
        return Ok(TwStockLookupResult {
            ok: false,
            reason_type: Some("NOT_FOUND".to_string()),
            reason_message: Some("股票代號不可空白。".to_string()),
            record: None,
            diagnostics: vec![],
        });
    }

    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|err| format!("建立查詢 client 失敗: {err}"))?;

    let twse_url = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L";
    let twse_quotes_url = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL";
    let tpex_url = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O";
    let mut diagnostics: Vec<TwStockLookupDiagnostic> = Vec::new();

    match lookup_from_source(
        &client,
        twse_url,
        &normalized_symbol,
        "TWSE",
        &["公司代號"],
        &["公司簡稱", "公司名稱"],
        &["產業別"],
    ) {
        Ok((record, diag)) => {
            diagnostics.push(diag);
            if record.is_some() {
                return Ok(TwStockLookupResult {
                    ok: true,
                    reason_type: None,
                    reason_message: None,
                    record,
                    diagnostics,
                });
            }
        }
        Err(error) => {
            return Ok(TwStockLookupResult {
                ok: false,
                reason_type: Some(
                    if error.starts_with("NETWORK_FAILURE::") {
                        "NETWORK_FAILURE"
                    } else if error.starts_with("SOURCE_FORMAT_ERROR::") {
                        "SOURCE_FORMAT_ERROR"
                    } else {
                        "PARSE_FAILURE"
                    }
                    .to_string(),
                ),
                reason_message: Some(error),
                record: None,
                diagnostics,
            });
        }
    }

    match lookup_from_source(
        &client,
        twse_quotes_url,
        &normalized_symbol,
        "TWSE",
        &["Code"],
        &["Name"],
        &[],
    ) {
        Ok((record, diag)) => {
            diagnostics.push(diag);
            if record.is_some() {
                return Ok(TwStockLookupResult {
                    ok: true,
                    reason_type: None,
                    reason_message: None,
                    record,
                    diagnostics,
                });
            }
        }
        Err(error) => {
            return Ok(TwStockLookupResult {
                ok: false,
                reason_type: Some(
                    if error.starts_with("NETWORK_FAILURE::") {
                        "NETWORK_FAILURE"
                    } else if error.starts_with("SOURCE_FORMAT_ERROR::") {
                        "SOURCE_FORMAT_ERROR"
                    } else {
                        "PARSE_FAILURE"
                    }
                    .to_string(),
                ),
                reason_message: Some(error),
                record: None,
                diagnostics,
            });
        }
    }

    match lookup_from_source(
        &client,
        tpex_url,
        &normalized_symbol,
        "TPEX",
        &["SecuritiesCompanyCode"],
        &["CompanyAbbreviation", "CompanyName"],
        &["SecuritiesIndustryCode"],
    ) {
        Ok((record, diag)) => {
            diagnostics.push(diag);
            if record.is_some() {
                return Ok(TwStockLookupResult {
                    ok: true,
                    reason_type: None,
                    reason_message: None,
                    record,
                    diagnostics,
                });
            }
        }
        Err(error) => {
            return Ok(TwStockLookupResult {
                ok: false,
                reason_type: Some(
                    if error.starts_with("NETWORK_FAILURE::") {
                        "NETWORK_FAILURE"
                    } else if error.starts_with("SOURCE_FORMAT_ERROR::") {
                        "SOURCE_FORMAT_ERROR"
                    } else {
                        "PARSE_FAILURE"
                    }
                    .to_string(),
                ),
                reason_message: Some(error),
                record: None,
                diagnostics,
            });
        }
    }

    if diagnostics
        .iter()
        .any(|diag| diag.error.as_deref().unwrap_or("").starts_with("HTTP"))
    {
        let status_summary = diagnostics
            .iter()
            .filter_map(|diag| diag.http_status)
            .map(|code| code.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        return Ok(TwStockLookupResult {
            ok: false,
            reason_type: Some("NETWORK_FAILURE".to_string()),
            reason_message: Some(format!(
                "來源連線狀態異常（HTTP: {}）",
                if status_summary.is_empty() {
                    "unknown".to_string()
                } else {
                    status_summary
                }
            )),
            record: None,
            diagnostics,
        });
    }

    Ok(TwStockLookupResult {
        ok: false,
        reason_type: Some("NOT_FOUND".to_string()),
        reason_message: Some(format!("查無股票代號 {normalized_symbol}")),
        record: None,
        diagnostics,
    })
}

fn upsert_app_setting_tx(tx: &Transaction, key: &str, value: &str) -> Result<(), String> {
    tx.execute(
        "
        INSERT INTO app_settings (key, value)
        VALUES (?1, ?2)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP
    ",
        params![key, value],
    )
    .map_err(|err| format!("更新設定失敗（{key}）: {err}"))?;
    Ok(())
}

fn parse_numeric(raw: &str) -> Option<f64> {
    let cleaned = raw.trim().replace(',', "");
    if cleaned.is_empty() || cleaned == "--" {
        return None;
    }
    cleaned.parse::<f64>().ok()
}

fn parse_change(raw: &str) -> Option<f64> {
    let cleaned = raw.trim().replace(',', "");
    if cleaned.is_empty() || cleaned == "--" {
        return None;
    }
    let filtered: String = cleaned
        .chars()
        .filter(|ch| ch.is_ascii_digit() || *ch == '.' || *ch == '-' || *ch == '+')
        .collect();
    if filtered.is_empty() || filtered == "+" || filtered == "-" {
        return None;
    }
    filtered.parse::<f64>().ok()
}

fn roc_date_to_iso(raw: &str) -> Option<String> {
    let value: String = raw.chars().filter(|ch| ch.is_ascii_digit()).collect();
    if value.len() == 7 {
        let roc_year = value[0..3].parse::<i32>().ok()?;
        let year = roc_year + 1911;
        let month = value[3..5].parse::<u32>().ok()?;
        let day = value[5..7].parse::<u32>().ok()?;
        let date = NaiveDate::from_ymd_opt(year, month, day)?;
        return Some(date.format("%Y-%m-%d").to_string());
    }
    if value.len() == 8 {
        let year = value[0..4].parse::<i32>().ok()?;
        let month = value[4..6].parse::<u32>().ok()?;
        let day = value[6..8].parse::<u32>().ok()?;
        let date = NaiveDate::from_ymd_opt(year, month, day)?;
        return Some(date.format("%Y-%m-%d").to_string());
    }
    None
}

fn previous_trading_date(iso_date: &str) -> Option<String> {
    let mut date = NaiveDate::parse_from_str(iso_date, "%Y-%m-%d").ok()?;
    loop {
        date = date.checked_sub_signed(ChronoDuration::days(1))?;
        let weekday = date.weekday();
        if weekday != Weekday::Sat && weekday != Weekday::Sun {
            return Some(date.format("%Y-%m-%d").to_string());
        }
    }
}

fn next_trading_date(iso_date: &str) -> Option<String> {
    let mut date = NaiveDate::parse_from_str(iso_date, "%Y-%m-%d").ok()?;
    loop {
        date = date.checked_add_signed(ChronoDuration::days(1))?;
        let weekday = date.weekday();
        if weekday != Weekday::Sat && weekday != Weekday::Sun {
            return Some(date.format("%Y-%m-%d").to_string());
        }
    }
}

fn iso_to_yyyymmdd(iso_date: &str) -> Option<String> {
    let date = NaiveDate::parse_from_str(iso_date, "%Y-%m-%d").ok()?;
    Some(date.format("%Y%m%d").to_string())
}

fn wait_for_request_slot(last_request_at: &mut Option<Instant>, min_interval: Duration) {
    if let Some(last) = last_request_at {
        let elapsed = last.elapsed();
        if elapsed < min_interval {
            std::thread::sleep(min_interval - elapsed);
        }
    }
    *last_request_at = Some(Instant::now());
}

fn first_calendar_day_of_month(iso_date: &str) -> Option<String> {
    let date = NaiveDate::parse_from_str(iso_date, "%Y-%m-%d").ok()?;
    let first = NaiveDate::from_ymd_opt(date.year(), date.month(), 1)?;
    Some(first.format("%Y-%m-%d").to_string())
}

fn first_calendar_day_of_year(iso_date: &str) -> Option<String> {
    let date = NaiveDate::parse_from_str(iso_date, "%Y-%m-%d").ok()?;
    let first = NaiveDate::from_ymd_opt(date.year(), 1, 1)?;
    Some(first.format("%Y-%m-%d").to_string())
}

fn is_twse_market(market: &str) -> bool {
    let upper = market.trim().to_uppercase();
    upper.contains("TWSE") || upper == "TSE"
}

fn is_tpex_market(market: &str) -> bool {
    let upper = market.trim().to_uppercase();
    upper.contains("TPEX") || upper.contains("OTC")
}

#[allow(dead_code)]
fn parse_tw_quote_map(
    body: &str,
    code_key: &str,
    close_key: &str,
    change_key: &str,
    date_key: &str,
) -> Result<HashMap<String, TwMarketQuote>, String> {
    let parsed: Value =
        serde_json::from_str(body).map_err(|err| format!("來源 JSON 解析失敗: {err}"))?;
    let rows = parsed
        .as_array()
        .ok_or_else(|| "來源格式異常：非陣列資料".to_string())?;
    let mut map = HashMap::new();
    for row in rows {
        let code = row
            .get(code_key)
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .trim()
            .to_uppercase();
        if code.is_empty() {
            continue;
        }
        let date_raw = row
            .get(date_key)
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .trim();
        let trade_date = match roc_date_to_iso(date_raw) {
            Some(value) => value,
            None => continue,
        };
        let close_raw = row
            .get(close_key)
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .trim();
        let close_price = match parse_numeric(close_raw) {
            Some(value) => value,
            None => continue,
        };
        let change_raw = row
            .get(change_key)
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .trim();
        let previous_close_price = parse_change(change_raw).map(|change| close_price - change);
        map.insert(
            code,
            TwMarketQuote {
                trade_date,
                close_price,
                previous_close_price,
            },
        );
    }
    Ok(map)
}

fn month_start_yyyymm01(date: NaiveDate) -> String {
    format!("{:04}{:02}01", date.year(), date.month())
}

fn prev_month_start_yyyymm01(date: NaiveDate) -> String {
    let first = NaiveDate::from_ymd_opt(date.year(), date.month(), 1).unwrap_or(date);
    let prev = first
        .checked_sub_signed(ChronoDuration::days(1))
        .unwrap_or(first);
    month_start_yyyymm01(prev)
}

fn parse_month_quotes_from_twse_stock_day(body: &str) -> Result<Vec<(String, f64)>, String> {
    let parsed: Value =
        serde_json::from_str(body).map_err(|err| format!("TWSE JSON 解析失敗: {err}"))?;
    let stat = parsed
        .get("stat")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    if !stat.is_empty() && stat != "OK" {
        return Ok(Vec::new());
    }
    let rows = parsed
        .get("data")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();

    let mut result = Vec::new();
    for row in rows {
        let values = match row.as_array() {
            Some(arr) => arr,
            None => continue,
        };
        let trade_date_raw = values
            .get(0)
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .trim();
        let close_raw = values
            .get(6)
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .trim();
        let Some(trade_date) = roc_date_to_iso(trade_date_raw) else {
            continue;
        };
        let Some(close_price) = parse_numeric(close_raw) else {
            continue;
        };
        result.push((trade_date, close_price));
    }
    result.sort_by(|lhs, rhs| lhs.0.cmp(&rhs.0));
    Ok(result)
}

fn parse_month_quotes_from_tpex_daily(body: &str) -> Result<Vec<(String, f64)>, String> {
    let parsed: Value =
        serde_json::from_str(body).map_err(|err| format!("TPEX JSON 解析失敗: {err}"))?;
    let rows = parsed
        .get("aaData")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();

    let mut result = Vec::new();
    for row in rows {
        let values = match row.as_array() {
            Some(arr) => arr,
            None => continue,
        };
        let trade_date_raw = values
            .get(0)
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .trim();
        let close_raw = values
            .get(2)
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .trim();
        let Some(trade_date) = roc_date_to_iso(trade_date_raw) else {
            continue;
        };
        let Some(close_price) = parse_numeric(close_raw) else {
            continue;
        };
        result.push((trade_date, close_price));
    }
    result.sort_by(|lhs, rhs| lhs.0.cmp(&rhs.0));
    Ok(result)
}

fn fetch_twse_month_quotes(
    client: &Client,
    symbol: &str,
    month_start: &str,
    last_request_at: &mut Option<Instant>,
    min_interval: Duration,
) -> Result<Vec<(String, f64)>, String> {
    let url = format!(
        "https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={month_start}&stockNo={symbol}"
    );
    wait_for_request_slot(last_request_at, min_interval);
    let response = client
        .get(url)
        .send()
        .map_err(|err| format!("TWSE 連線失敗: {err}"))?;
    let status = response.status();
    let body = response
        .text()
        .map_err(|err| format!("TWSE 回應讀取失敗: {err}"))?;
    if !status.is_success() {
        return Err(format!(
            "TWSE 來源 HTTP {}: {}",
            status.as_u16(),
            response_preview(&body, 120)
        ));
    }
    parse_month_quotes_from_twse_stock_day(&body)
}

fn fetch_twse_mis_quotes(
    client: &Client,
    symbols: &[String],
    last_request_at: &mut Option<Instant>,
    min_interval: Duration,
) -> Result<HashMap<String, TwMarketQuote>, String> {
    if symbols.is_empty() {
        return Ok(HashMap::new());
    }
    let mut result = HashMap::new();
    let chunk_size = 20usize;
    for chunk in symbols.chunks(chunk_size) {
        let ex_ch = chunk
            .iter()
            .map(|symbol| format!("tse_{symbol}.tw"))
            .collect::<Vec<_>>()
            .join("|");
        let url = format!(
            "https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={ex_ch}&json=1&delay=0"
        );
        wait_for_request_slot(last_request_at, min_interval);
        let response = client
            .get(url)
            .send()
            .map_err(|err| format!("TWSE MIS 連線失敗: {err}"))?;
        let status = response.status();
        let body = response
            .text()
            .map_err(|err| format!("TWSE MIS 讀取失敗: {err}"))?;
        if !status.is_success() {
            return Err(format!(
                "TWSE MIS HTTP {}: {}",
                status.as_u16(),
                response_preview(&body, 120)
            ));
        }
        let parsed: Value =
            serde_json::from_str(&body).map_err(|err| format!("TWSE MIS JSON 解析失敗: {err}"))?;
        let rows = parsed
            .get("msgArray")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();
        for row in rows {
            let symbol = row
                .get("c")
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .trim()
                .to_uppercase();
            if symbol.is_empty() || !chunk.iter().any(|target| target == &symbol) {
                continue;
            }
            let trade_date_raw = row
                .get("d")
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .trim();
            let Some(trade_date) = roc_date_to_iso(trade_date_raw) else {
                continue;
            };
            let latest_raw = row
                .get("z")
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .trim();
            let previous_raw = row
                .get("y")
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .trim();
            let previous_close_price = parse_numeric(previous_raw);
            let close_price = parse_numeric(latest_raw).or(previous_close_price);
            let Some(close_price) = close_price else {
                continue;
            };
            result.insert(
                symbol,
                TwMarketQuote {
                    trade_date,
                    close_price,
                    previous_close_price,
                },
            );
        }
    }
    Ok(result)
}

fn should_replace_tw_quote(current: &TwMarketQuote, candidate: &TwMarketQuote) -> bool {
    if candidate.trade_date > current.trade_date {
        return true;
    }
    if candidate.trade_date < current.trade_date {
        return false;
    }
    matches!(
        (
            current.previous_close_price.is_some(),
            candidate.previous_close_price.is_some(),
        ),
        (false, true)
    )
}

fn fetch_twse_daily_all_quotes(
    client: &Client,
    symbols: &[String],
    last_request_at: &mut Option<Instant>,
    min_interval: Duration,
) -> Result<HashMap<String, TwMarketQuote>, String> {
    let url = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL";
    let openapi_result = retry_with_backoff(3, Duration::from_secs(2), |_attempt| {
        wait_for_request_slot(last_request_at, min_interval);
        let response = client
            .get(url)
            .header("User-Agent", "stock-vault/1.0")
            .send()
            .map_err(|err| format!("TWSE 開放資料連線失敗: {err}"))?;
        let status = response.status();
        let body = response
            .text()
            .map_err(|err| format!("TWSE 開放資料讀取失敗: {err}"))?;
        if !status.is_success() {
            return Err(format!(
                "TWSE 開放資料 HTTP {}: {}",
                status.as_u16(),
                response_preview(&body, 120)
            ));
        }
        parse_tw_quote_map(&body, "Code", "ClosingPrice", "Change", "Date")
    });
    let mis_result = fetch_twse_mis_quotes(client, symbols, last_request_at, min_interval);

    match (openapi_result, mis_result) {
        (Ok(mut openapi_map), Ok(mis_map)) => {
            for (symbol, quote) in mis_map {
                match openapi_map.get(&symbol) {
                    Some(existing) if !should_replace_tw_quote(existing, &quote) => {}
                    _ => {
                        openapi_map.insert(symbol, quote);
                    }
                }
            }
            Ok(openapi_map)
        }
        (Ok(openapi_map), Err(_)) => Ok(openapi_map),
        (Err(_), Ok(mis_map)) => Ok(mis_map),
        (Err(primary_err), Err(fallback_err)) => Err(format!(
            "TWSE 開放資料連線失敗: {primary_err}；MIS 備援也失敗: {fallback_err}"
        )),
    }
}

fn fetch_tpex_month_quotes(
    client: &Client,
    symbol: &str,
    month_start: &str,
    last_request_at: &mut Option<Instant>,
    min_interval: Duration,
) -> Result<Vec<(String, f64)>, String> {
    let year = month_start
        .get(0..4)
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(0);
    let month = month_start.get(4..6).unwrap_or("01");
    let roc_year = year.saturating_sub(1911);
    let url = format!(
        "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw&d={roc_year}/{month}&stkno={symbol}"
    );
    wait_for_request_slot(last_request_at, min_interval);
    let response = client
        .get(url)
        .header("referer", "https://www.tpex.org.tw/")
        .send()
        .map_err(|err| format!("TPEX 連線失敗: {err}"))?;
    let status = response.status();
    let body = response
        .text()
        .map_err(|err| format!("TPEX 回應讀取失敗: {err}"))?;
    if !status.is_success() {
        return Err(format!(
            "TPEX 來源 HTTP {}: {}",
            status.as_u16(),
            response_preview(&body, 120)
        ));
    }
    parse_month_quotes_from_tpex_daily(&body)
}

fn build_latest_quote_from_month_rows(rows: &[(String, f64)]) -> Option<TwMarketQuote> {
    let latest = rows.last()?;
    let previous_close_price = if rows.len() >= 2 {
        Some(rows[rows.len() - 2].1)
    } else {
        None
    };
    Some(TwMarketQuote {
        trade_date: latest.0.clone(),
        close_price: latest.1,
        previous_close_price,
    })
}

fn fetch_twse_symbol_quote(
    client: &Client,
    symbol: &str,
    last_request_at: &mut Option<Instant>,
    min_interval: Duration,
) -> Result<Option<TwMarketQuote>, String> {
    let today = chrono::Local::now().date_naive();
    let month_start = month_start_yyyymm01(today);
    let prev_month_start = prev_month_start_yyyymm01(today);

    let mut current_rows =
        fetch_twse_month_quotes(client, symbol, &month_start, last_request_at, min_interval)?;
    if current_rows.is_empty() {
        return Ok(None);
    }
    if current_rows.len() == 1 {
        let prev_rows = fetch_twse_month_quotes(
            client,
            symbol,
            &prev_month_start,
            last_request_at,
            min_interval,
        )?;
        if let Some(prev) = prev_rows.last() {
            current_rows.insert(0, prev.clone());
        }
    }
    Ok(build_latest_quote_from_month_rows(&current_rows))
}

fn fetch_tpex_symbol_quote(
    client: &Client,
    symbol: &str,
    last_request_at: &mut Option<Instant>,
    min_interval: Duration,
) -> Result<Option<TwMarketQuote>, String> {
    let today = chrono::Local::now().date_naive();
    let month_start = month_start_yyyymm01(today);
    let prev_month_start = prev_month_start_yyyymm01(today);

    let mut current_rows =
        fetch_tpex_month_quotes(client, symbol, &month_start, last_request_at, min_interval)?;
    if current_rows.is_empty() {
        return Ok(None);
    }
    if current_rows.len() == 1 {
        let prev_rows = fetch_tpex_month_quotes(
            client,
            symbol,
            &prev_month_start,
            last_request_at,
            min_interval,
        )?;
        if let Some(prev) = prev_rows.last() {
            current_rows.insert(0, prev.clone());
        }
    }
    Ok(build_latest_quote_from_month_rows(&current_rows))
}

fn extract_iso_date_prefix(value: &str) -> Option<String> {
    let prefix = value.get(0..10)?;
    NaiveDate::parse_from_str(prefix, "%Y-%m-%d")
        .ok()
        .map(|date| date.format("%Y-%m-%d").to_string())
}

fn iso_to_tpex_roc_date(iso_date: &str) -> Option<String> {
    let date = NaiveDate::parse_from_str(iso_date, "%Y-%m-%d").ok()?;
    let roc_year = date.year() - 1911;
    Some(format!(
        "{roc_year:03}/{:02}/{:02}",
        date.month(),
        date.day()
    ))
}

fn read_app_setting(conn: &Connection, key: &str) -> Result<Option<String>, String> {
    conn.query_row(
        "SELECT value FROM app_settings WHERE key = ?1",
        params![key],
        |row| row.get(0),
    )
    .optional()
    .map_err(|err| format!("讀取設定失敗（{key}）: {err}"))
}

fn parse_optional_i64_setting(raw: Option<String>) -> i64 {
    raw.and_then(|value| value.trim().parse::<i64>().ok())
        .unwrap_or(0)
}

fn previous_calendar_day_iso(iso_date: &str) -> Option<String> {
    let date = NaiveDate::parse_from_str(iso_date, "%Y-%m-%d").ok()?;
    let prev = date.checked_sub_signed(ChronoDuration::days(1))?;
    Some(prev.format("%Y-%m-%d").to_string())
}

fn compute_tpex_fail_streak(
    last_fail_date: Option<String>,
    last_streak: i64,
    current_date: &str,
    is_failed: bool,
) -> (i64, Option<String>) {
    if !is_failed {
        return (0, None);
    }

    if last_fail_date.as_deref() == Some(current_date) {
        return (last_streak.max(1), Some(current_date.to_string()));
    }

    let expected_prev = previous_calendar_day_iso(current_date);
    let is_consecutive = match (last_fail_date.as_deref(), expected_prev.as_deref()) {
        (Some(last), Some(prev)) => last == prev,
        _ => false,
    };
    if is_consecutive {
        (last_streak.max(1) + 1, Some(current_date.to_string()))
    } else {
        (1, Some(current_date.to_string()))
    }
}

fn retry_with_backoff<T, F>(max_attempts: usize, delay: Duration, mut task: F) -> Result<T, String>
where
    F: FnMut(usize) -> Result<T, String>,
{
    let mut last_error = String::new();
    for attempt in 1..=max_attempts.max(1) {
        match task(attempt) {
            Ok(value) => return Ok(value),
            Err(err) => {
                last_error = err;
                if attempt < max_attempts {
                    std::thread::sleep(delay);
                }
            }
        }
    }
    Err(last_error)
}

fn parse_twse_dividend_events(body: &str) -> Result<Vec<TwDividendMarketEvent>, String> {
    let parsed: Value =
        serde_json::from_str(body).map_err(|err| format!("TWSE 除權息 JSON 解析失敗: {err}"))?;
    let stat = parsed
        .get("stat")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    if !stat.is_empty() && stat != "OK" {
        return Ok(Vec::new());
    }

    let mut events = Vec::new();
    let rows = parsed
        .get("data")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();

    for row in rows {
        let values = match row.as_array() {
            Some(arr) => arr,
            None => continue,
        };
        let ex_dividend_date = values
            .get(0)
            .and_then(|value| value.as_str())
            .and_then(roc_date_to_iso);
        let symbol = values
            .get(1)
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .trim()
            .to_uppercase();
        let kind = values
            .get(3)
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .trim();
        let bonus_ratio = values
            .get(4)
            .and_then(|value| value.as_str())
            .and_then(parse_numeric);
        let cash_dividend_per_share = values
            .get(7)
            .and_then(|value| value.as_str())
            .and_then(parse_numeric);

        let Some(ex_dividend_date) = ex_dividend_date else {
            continue;
        };
        if symbol.is_empty() {
            continue;
        }

        let stock_dividend_per_thousand = bonus_ratio
            .filter(|value| *value > 0.0)
            .map(|value| round_money(value * 1000.0));

        let includes_cash = kind.contains('息') || cash_dividend_per_share.unwrap_or(0.0) > 0.0;
        let includes_stock =
            kind.contains('權') || stock_dividend_per_thousand.unwrap_or(0.0) > 0.0;

        if includes_cash && cash_dividend_per_share.unwrap_or(0.0) > 0.0 {
            events.push(TwDividendMarketEvent {
                symbol: symbol.clone(),
                ex_dividend_date: ex_dividend_date.clone(),
                cash_dividend_per_share,
                stock_dividend_per_thousand: None,
                record_date: None,
                payment_date: None,
            });
        }

        if includes_stock && stock_dividend_per_thousand.unwrap_or(0.0) > 0.0 {
            events.push(TwDividendMarketEvent {
                symbol,
                ex_dividend_date,
                cash_dividend_per_share: None,
                stock_dividend_per_thousand,
                record_date: None,
                payment_date: None,
            });
        }
    }

    Ok(events)
}

fn fetch_twse_dividend_events_range_by_endpoint(
    client: &Client,
    endpoint: &str,
    start_date: &str,
    end_date: &str,
    last_request_at: &mut Option<Instant>,
    min_interval: Duration,
) -> Result<Vec<TwDividendMarketEvent>, String> {
    let start_yyyymmdd = iso_to_yyyymmdd(start_date)
        .ok_or_else(|| format!("無法解析 TWSE 股利起始日期：{start_date}"))?;
    let end_yyyymmdd = iso_to_yyyymmdd(end_date)
        .ok_or_else(|| format!("無法解析 TWSE 股利結束日期：{end_date}"))?;

    let url = format!(
        "https://www.twse.com.tw/exchangeReport/{endpoint}?response=json&strDate={start_yyyymmdd}&endDate={end_yyyymmdd}"
    );
    retry_with_backoff(3, Duration::from_secs(2), |_attempt| {
        wait_for_request_slot(last_request_at, min_interval);
        let response = client
            .get(url.clone())
            .send()
            .map_err(|err| format!("TWSE {endpoint} 連線失敗: {err}"))?;
        let status = response.status();
        let body = response
            .text()
            .map_err(|err| format!("TWSE {endpoint} 讀取失敗: {err}"))?;
        if !status.is_success() {
            return Err(format!(
                "TWSE {endpoint} HTTP {}: {}",
                status.as_u16(),
                response_preview(&body, 120)
            ));
        }
        parse_twse_dividend_events(&body)
    })
}

fn fetch_twse_dividend_events_range(
    client: &Client,
    start_date: &str,
    end_date: &str,
    last_request_at: &mut Option<Instant>,
    min_interval: Duration,
) -> Result<Vec<TwDividendMarketEvent>, String> {
    let mut all_events = Vec::new();
    let mut errors = Vec::new();

    for endpoint in ["TWT48U"] {
        match fetch_twse_dividend_events_range_by_endpoint(
            client,
            endpoint,
            start_date,
            end_date,
            last_request_at,
            min_interval,
        ) {
            Ok(mut events) => all_events.append(&mut events),
            Err(err) => errors.push(err),
        }
    }

    if all_events.is_empty() && !errors.is_empty() {
        return Err(errors.join("；"));
    }

    let mut deduped = Vec::new();
    let mut seen = BTreeSet::new();
    for event in all_events {
        let cash_key = event
            .cash_dividend_per_share
            .map(|value| format!("{value:.8}"))
            .unwrap_or_default();
        let stock_key = event
            .stock_dividend_per_thousand
            .map(|value| format!("{value:.8}"))
            .unwrap_or_default();
        let key = format!(
            "{}|{}|{}|{}",
            event.symbol, event.ex_dividend_date, cash_key, stock_key
        );
        if seen.insert(key) {
            deduped.push(event);
        }
    }

    Ok(deduped)
}

fn parse_twse_etf_dividend_events(
    body: &str,
    requested_symbol: &str,
) -> Result<Vec<TwDividendMarketEvent>, String> {
    let parsed: Value = serde_json::from_str(body)
        .map_err(|err| format!("TWSE ETF 股利 JSON 解析失敗（{requested_symbol}）: {err}"))?;
    let stat = parsed
        .get("stat")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    if !stat.is_empty() && stat != "OK" {
        return Ok(Vec::new());
    }

    let mut events = Vec::new();
    let upper_symbol = requested_symbol.trim().to_uppercase();

    if let Some(rows) = parsed.get("data").and_then(|value| value.as_array()) {
        if let Some(fields) = parsed.get("fields").and_then(|value| value.as_array()) {
            let mut field_idx = HashMap::new();
            for (idx, field) in fields.iter().enumerate() {
                let key = field
                    .as_str()
                    .unwrap_or("")
                    .trim()
                    .replace(' ', "")
                    .to_uppercase();
                field_idx.insert(key, idx);
            }
            let symbol_idx = field_idx
                .iter()
                .find(|(key, _)| key.contains("代號"))
                .map(|(_, idx)| *idx);
            let ex_date_idx = field_idx
                .iter()
                .find(|(key, _)| key.contains("除息交易日") || key.contains("除息日"))
                .map(|(_, idx)| *idx);
            let cash_idx = field_idx
                .iter()
                .find(|(key, _)| {
                    key.contains("現金股利")
                        || key.contains("收益分配金額")
                        || key.contains("每受益權益單位")
                        || key.contains("每1受益權益單位")
                        || key.contains("每單位")
                        || key.contains("配息")
                })
                .map(|(_, idx)| *idx);
            let record_date_idx = field_idx
                .iter()
                .find(|(key, _)| key.contains("收益分配基準日") || key.contains("基準日"))
                .map(|(_, idx)| *idx);
            let payment_date_idx = field_idx
                .iter()
                .find(|(key, _)| key.contains("發放日") || key.contains("給付日"))
                .map(|(_, idx)| *idx);

            for row in rows {
                let values = match row.as_array() {
                    Some(arr) => arr,
                    None => continue,
                };
                let symbol = symbol_idx
                    .and_then(|idx| values.get(idx))
                    .and_then(|value| value.as_str())
                    .map(|value| value.trim().to_uppercase())
                    .filter(|value| !value.is_empty())
                    .unwrap_or_else(|| upper_symbol.clone());
                if symbol != upper_symbol {
                    continue;
                }

                let ex_date = ex_date_idx
                    .and_then(|idx| values.get(idx))
                    .and_then(|value| value.as_str())
                    .and_then(roc_date_to_iso);
                let Some(ex_dividend_date) = ex_date else {
                    continue;
                };
                let cash_dividend_per_share = cash_idx
                    .and_then(|idx| values.get(idx))
                    .and_then(|value| value.as_str())
                    .and_then(parse_numeric)
                    .filter(|value| *value > 0.0);
                if cash_dividend_per_share.is_none() {
                    continue;
                }
                let record_date = record_date_idx
                    .and_then(|idx| values.get(idx))
                    .and_then(|value| value.as_str())
                    .and_then(roc_date_to_iso);
                let payment_date = payment_date_idx
                    .and_then(|idx| values.get(idx))
                    .and_then(|value| value.as_str())
                    .and_then(roc_date_to_iso);

                events.push(TwDividendMarketEvent {
                    symbol,
                    ex_dividend_date,
                    cash_dividend_per_share,
                    stock_dividend_per_thousand: None,
                    record_date,
                    payment_date,
                });
            }
            return Ok(events);
        }

        for row in rows {
            let row_obj = match row.as_object() {
                Some(value) => value,
                None => continue,
            };
            let mut symbol = upper_symbol.clone();
            let mut ex_date: Option<String> = None;
            let mut cash_per_share: Option<f64> = None;
            let mut record_date: Option<String> = None;
            let mut payment_date: Option<String> = None;
            for (key, value) in row_obj {
                let normalized_key = key.trim().replace(' ', "");
                let value_raw = value.as_str().unwrap_or("").trim();
                if value_raw.is_empty() {
                    continue;
                }
                if normalized_key.contains("代號") {
                    symbol = value_raw.to_uppercase();
                } else if normalized_key.contains("除息交易日") || normalized_key.contains("除息日")
                {
                    ex_date = roc_date_to_iso(value_raw);
                } else if normalized_key.contains("現金股利")
                    || normalized_key.contains("收益分配金額")
                    || normalized_key.contains("每受益權益單位")
                    || normalized_key.contains("每1受益權益單位")
                    || normalized_key.contains("每單位")
                    || normalized_key.contains("配息")
                {
                    cash_per_share = parse_numeric(value_raw);
                } else if normalized_key.contains("收益分配基準日")
                    || normalized_key.contains("基準日")
                {
                    record_date = roc_date_to_iso(value_raw);
                } else if normalized_key.contains("發放日") || normalized_key.contains("給付日")
                {
                    payment_date = roc_date_to_iso(value_raw);
                }
            }

            if symbol != upper_symbol {
                continue;
            }
            let Some(ex_dividend_date) = ex_date else {
                continue;
            };
            let cash_dividend_per_share = cash_per_share.filter(|value| *value > 0.0);
            if cash_dividend_per_share.is_none() {
                continue;
            }
            events.push(TwDividendMarketEvent {
                symbol,
                ex_dividend_date,
                cash_dividend_per_share,
                stock_dividend_per_thousand: None,
                record_date,
                payment_date,
            });
        }
    }

    Ok(events)
}

fn fetch_twse_etf_dividend_events_for_symbol(
    client: &Client,
    symbol: &str,
    last_request_at: &mut Option<Instant>,
    min_interval: Duration,
) -> Result<Vec<TwDividendMarketEvent>, String> {
    let url = format!("https://www.twse.com.tw/rwd/zh/ETF/etfDiv?response=json&stkNo={symbol}");
    retry_with_backoff(3, Duration::from_secs(2), |_attempt| {
        wait_for_request_slot(last_request_at, min_interval);
        let response = client
            .get(url.clone())
            .send()
            .map_err(|err| format!("TWSE ETF 股利來源連線失敗（{symbol}）: {err}"))?;
        let status = response.status();
        let body = response
            .text()
            .map_err(|err| format!("TWSE ETF 股利來源讀取失敗（{symbol}）: {err}"))?;
        if !status.is_success() {
            return Err(format!(
                "TWSE ETF 股利來源 HTTP {}（{symbol}）: {}",
                status.as_u16(),
                response_preview(&body, 120)
            ));
        }
        parse_twse_etf_dividend_events(&body, symbol)
    })
}

fn fetch_twse_etf_dividend_events(
    client: &Client,
    targets: &[TwPriceSyncTarget],
    last_request_at: &mut Option<Instant>,
    min_interval: Duration,
) -> Result<Vec<TwDividendMarketEvent>, String> {
    let mut events = Vec::new();
    let mut errors = Vec::new();
    let mut etf_symbols = BTreeSet::new();
    for target in targets {
        if is_twse_market(&target.market) && target.symbol.trim().starts_with("00") {
            etf_symbols.insert(target.symbol.trim().to_uppercase());
        }
    }
    for symbol in etf_symbols {
        match fetch_twse_etf_dividend_events_for_symbol(
            client,
            &symbol,
            last_request_at,
            min_interval,
        ) {
            Ok(mut symbol_events) => events.append(&mut symbol_events),
            Err(err) => errors.push(err),
        }
    }

    if events.is_empty() && !errors.is_empty() {
        return Err(errors.join("；"));
    }
    Ok(events)
}

fn parse_tpex_dividend_events(body: &str) -> Result<Vec<TwDividendMarketEvent>, String> {
    let parsed: Value =
        serde_json::from_str(body).map_err(|err| format!("TPEX 除權息 JSON 解析失敗: {err}"))?;
    let rows = parsed
        .get("aaData")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();

    let mut events = Vec::new();
    for row in rows {
        let values = match row.as_array() {
            Some(arr) => arr,
            None => continue,
        };
        let ex_dividend_date = values
            .get(0)
            .and_then(|value| value.as_str())
            .and_then(roc_date_to_iso);
        let symbol = values
            .get(1)
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .trim()
            .to_uppercase();
        let kind = values
            .get(3)
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .trim();
        let stock_ratio = values
            .get(4)
            .and_then(|value| value.as_str())
            .and_then(parse_numeric);
        let cash_dividend_per_share = values
            .get(7)
            .and_then(|value| value.as_str())
            .and_then(parse_numeric);

        let Some(ex_dividend_date) = ex_dividend_date else {
            continue;
        };
        if symbol.is_empty() {
            continue;
        }

        let stock_dividend_per_thousand = stock_ratio
            .filter(|value| *value > 0.0)
            .map(|value| round_money(value * 1000.0));

        let includes_cash = kind.contains('息') || cash_dividend_per_share.unwrap_or(0.0) > 0.0;
        let includes_stock =
            kind.contains('權') || stock_dividend_per_thousand.unwrap_or(0.0) > 0.0;

        if includes_cash && cash_dividend_per_share.unwrap_or(0.0) > 0.0 {
            events.push(TwDividendMarketEvent {
                symbol: symbol.clone(),
                ex_dividend_date: ex_dividend_date.clone(),
                cash_dividend_per_share,
                stock_dividend_per_thousand: None,
                record_date: None,
                payment_date: None,
            });
        }
        if includes_stock && stock_dividend_per_thousand.unwrap_or(0.0) > 0.0 {
            events.push(TwDividendMarketEvent {
                symbol,
                ex_dividend_date,
                cash_dividend_per_share: None,
                stock_dividend_per_thousand,
                record_date: None,
                payment_date: None,
            });
        }
    }

    Ok(events)
}

fn fetch_tpex_dividend_events_by_day(
    client: &Client,
    iso_date: &str,
    last_request_at: &mut Option<Instant>,
    min_interval: Duration,
) -> Result<Vec<TwDividendMarketEvent>, String> {
    let roc_date = iso_to_tpex_roc_date(iso_date)
        .ok_or_else(|| format!("無法解析 TPEX 股利查詢日期：{iso_date}"))?;
    let url = format!(
        "https://www.tpex.org.tw/web/stock/exright/dailyquo/result.php?l=zh-tw&d={roc_date}"
    );
    retry_with_backoff(3, Duration::from_secs(2), |_attempt| {
        wait_for_request_slot(last_request_at, min_interval);
        let response = client
            .get(url.clone())
            .header("referer", "https://www.tpex.org.tw/")
            .send()
            .map_err(|err| format!("TPEX 股利資料連線失敗: {err}"))?;
        let status = response.status();
        let body = response
            .text()
            .map_err(|err| format!("TPEX 股利資料讀取失敗: {err}"))?;
        if !status.is_success() {
            return Err(format!(
                "TPEX 股利資料 HTTP {}: {}",
                status.as_u16(),
                response_preview(&body, 120)
            ));
        }
        parse_tpex_dividend_events(&body)
    })
}

fn fetch_tpex_dividend_events_range(
    client: &Client,
    start_date: &str,
    end_date: &str,
    last_request_at: &mut Option<Instant>,
    min_interval: Duration,
) -> Result<Vec<TwDividendMarketEvent>, String> {
    let start = NaiveDate::parse_from_str(start_date, "%Y-%m-%d")
        .map_err(|err| format!("無法解析 TPEX 股利起始日期: {err}"))?;
    let end = NaiveDate::parse_from_str(end_date, "%Y-%m-%d")
        .map_err(|err| format!("無法解析 TPEX 股利結束日期: {err}"))?;
    if start > end {
        return Ok(Vec::new());
    }

    let mut result = Vec::new();
    let mut cursor = start;
    while cursor <= end {
        let iso = cursor.format("%Y-%m-%d").to_string();
        let mut rows =
            fetch_tpex_dividend_events_by_day(client, &iso, last_request_at, min_interval)?;
        result.append(&mut rows);
        cursor = match cursor.checked_add_signed(ChronoDuration::days(1)) {
            Some(value) => value,
            None => break,
        };
    }
    Ok(result)
}

fn load_earliest_trade_date_for_targets(
    conn: &Connection,
    targets: &[TwPriceSyncTarget],
) -> Result<Option<String>, String> {
    if targets.is_empty() {
        return Ok(None);
    }
    let mut stmt = conn
        .prepare(
            "
            SELECT MIN(trade_date)
            FROM transactions
            WHERE security_id = ?1
              AND type IN ('BUY', 'SELL', 'STOCK_DIVIDEND')
        ",
        )
        .map_err(|err| format!("準備讀取最早交易日期失敗: {err}"))?;

    let mut earliest: Option<String> = None;
    for target in targets {
        let candidate: Option<String> = stmt
            .query_row(params![target.security_id], |row| row.get(0))
            .map_err(|err| format!("讀取最早交易日期失敗（{}）: {err}", target.symbol))?;
        if let Some(date) = candidate {
            if earliest
                .as_ref()
                .map(|existing| date < *existing)
                .unwrap_or(true)
            {
                earliest = Some(date);
            }
        }
    }
    Ok(earliest)
}

fn compute_dividend_sync_start_date(
    earliest_trade_date: &str,
    end_date: &str,
    minimum_lookback_days: i64,
) -> Result<String, String> {
    let end_date_value = NaiveDate::parse_from_str(end_date, "%Y-%m-%d")
        .map_err(|err| format!("解析股利同步結束日期失敗: {err}"))?;
    let lookback_start = end_date_value
        .checked_sub_signed(ChronoDuration::days(minimum_lookback_days))
        .unwrap_or(end_date_value)
        .format("%Y-%m-%d")
        .to_string();
    Ok(if earliest_trade_date > lookback_start.as_str() {
        earliest_trade_date.to_string()
    } else {
        lookback_start
    })
}

fn load_account_holdings_on_ex_date(
    conn: &Connection,
    security_id: i64,
    ex_date: &str,
) -> Result<Vec<(i64, f64)>, String> {
    let mut stmt = conn
        .prepare(
            "
            SELECT
              account_id,
              SUM(
                CASE
                  WHEN type IN ('BUY', 'STOCK_DIVIDEND') THEN quantity
                  WHEN type = 'SELL' THEN -quantity
                  ELSE 0
                END
              ) AS holding_qty
            FROM transactions
            WHERE security_id = ?1
              AND trade_date <= ?2
              AND type IN ('BUY', 'SELL', 'STOCK_DIVIDEND')
            GROUP BY account_id
            HAVING holding_qty > 0
        ",
        )
        .map_err(|err| format!("準備讀取除息日持股數失敗: {err}"))?;
    let rows = stmt
        .query_map(params![security_id, ex_date], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, f64>(1)?))
        })
        .map_err(|err| format!("查詢除息日持股數失敗: {err}"))?;
    let mut result = Vec::new();
    for row in rows {
        result.push(row.map_err(|err| format!("讀取除息日持股數資料列失敗: {err}"))?);
    }
    Ok(result)
}

fn exists_manual_dividend_transaction(
    conn: &Connection,
    account_id: i64,
    security_id: i64,
    tx_type: &str,
    ex_date: &str,
) -> Result<bool, String> {
    let value: Option<i64> = conn
        .query_row(
            "
            SELECT id
            FROM transactions
            WHERE account_id = ?1
              AND security_id = ?2
              AND type = ?3
              AND trade_date = ?4
            LIMIT 1
        ",
            params![account_id, security_id, tx_type, ex_date],
            |row| row.get(0),
        )
        .optional()
        .map_err(|err| format!("檢查手動股利是否存在失敗: {err}"))?;
    Ok(value.is_some())
}

fn exists_auto_dividend_event(
    conn: &Connection,
    account_id: i64,
    security_id: i64,
    dividend_type: &str,
    ex_date: &str,
) -> Result<bool, String> {
    let value: Option<i64> = conn
        .query_row(
            "
            SELECT id
            FROM auto_dividend_events
            WHERE account_id = ?1
              AND security_id = ?2
              AND dividend_type = ?3
              AND ex_dividend_date = ?4
            LIMIT 1
        ",
            params![account_id, security_id, dividend_type, ex_date],
            |row| row.get(0),
        )
        .optional()
        .map_err(|err| format!("檢查自動股利是否存在失敗: {err}"))?;
    Ok(value.is_some())
}

fn build_auto_dividend_upsert_rows(
    conn: &Connection,
    targets: &[TwPriceSyncTarget],
    events: &[TwDividendMarketEvent],
    fetched_at: &str,
    source: &str,
) -> Result<(Vec<AutoDividendUpsertRow>, AutoDividendBuildStats), String> {
    let mut stats = AutoDividendBuildStats::default();
    let mut rows = Vec::new();
    let mut target_by_symbol: HashMap<String, Vec<TwPriceSyncTarget>> = HashMap::new();
    for target in targets {
        target_by_symbol
            .entry(target.symbol.trim().to_uppercase())
            .or_default()
            .push(target.clone());
    }

    for event in events {
        let Some(symbol_targets) = target_by_symbol.get(&event.symbol) else {
            continue;
        };
        for target in symbol_targets {
            stats.matched_events += 1;
            let holdings = load_account_holdings_on_ex_date(
                conn,
                target.security_id,
                &event.ex_dividend_date,
            )?;
            if holdings.is_empty() {
                stats.skipped_non_holding_rows += 1;
                continue;
            }

            for (account_id, holding_qty) in holdings {
                if holding_qty <= 0.0 {
                    stats.skipped_non_holding_rows += 1;
                    continue;
                }

                if let Some(per_share) = event.cash_dividend_per_share.filter(|value| *value > 0.0)
                {
                    if exists_manual_dividend_transaction(
                        conn,
                        account_id,
                        target.security_id,
                        "CASH_DIVIDEND",
                        &event.ex_dividend_date,
                    )? {
                        stats.skipped_manual_rows += 1;
                    } else if exists_auto_dividend_event(
                        conn,
                        account_id,
                        target.security_id,
                        "CASH",
                        &event.ex_dividend_date,
                    )? {
                        stats.skipped_existing_rows += 1;
                    } else {
                        rows.push(AutoDividendUpsertRow {
                            account_id,
                            security_id: target.security_id,
                            dividend_type: "CASH".to_string(),
                            ex_dividend_date: event.ex_dividend_date.clone(),
                            record_date: event.record_date.clone(),
                            payment_date: event.payment_date.clone(),
                            cash_dividend_per_share: Some(per_share),
                            total_cash_dividend: Some(round_money(per_share * holding_qty)),
                            stock_dividend_per_thousand: None,
                            total_stock_dividend: None,
                            holding_quantity_at_ex_date: round_money(holding_qty),
                            source: source.to_string(),
                            fetched_at: fetched_at.to_string(),
                        });
                        stats.cash_rows += 1;
                    }
                }

                if let Some(per_thousand) = event
                    .stock_dividend_per_thousand
                    .filter(|value| *value > 0.0)
                {
                    if exists_manual_dividend_transaction(
                        conn,
                        account_id,
                        target.security_id,
                        "STOCK_DIVIDEND",
                        &event.ex_dividend_date,
                    )? {
                        stats.skipped_manual_rows += 1;
                    } else if exists_auto_dividend_event(
                        conn,
                        account_id,
                        target.security_id,
                        "STOCK",
                        &event.ex_dividend_date,
                    )? {
                        stats.skipped_existing_rows += 1;
                    } else {
                        rows.push(AutoDividendUpsertRow {
                            account_id,
                            security_id: target.security_id,
                            dividend_type: "STOCK".to_string(),
                            ex_dividend_date: event.ex_dividend_date.clone(),
                            record_date: event.record_date.clone(),
                            payment_date: event.payment_date.clone(),
                            cash_dividend_per_share: None,
                            total_cash_dividend: None,
                            stock_dividend_per_thousand: Some(per_thousand),
                            total_stock_dividend: Some(round_money(
                                per_thousand * holding_qty / 1000.0,
                            )),
                            holding_quantity_at_ex_date: round_money(holding_qty),
                            source: source.to_string(),
                            fetched_at: fetched_at.to_string(),
                        });
                        stats.stock_rows += 1;
                    }
                }
            }
        }
    }

    Ok((rows, stats))
}

struct PreparedAutoDividendSync {
    rows: Vec<AutoDividendUpsertRow>,
    snapshot: TwDividendSyncSnapshot,
    tpex_fail_streak: i64,
    tpex_fail_last_date: Option<String>,
}

fn collect_market_dividend_events(
    client: &Client,
    targets: &[TwPriceSyncTarget],
    start_date: &str,
    end_date: &str,
    last_request_at: &mut Option<Instant>,
    min_interval: Duration,
) -> (Vec<TwDividendMarketEvent>, Vec<String>, bool, bool) {
    let mut warnings = Vec::new();
    let mut events = Vec::new();
    let mut twse_failed = false;
    let mut tpex_failed = false;
    let has_tpex_target = targets.iter().any(|target| is_tpex_market(&target.market));

    match fetch_twse_dividend_events_range(
        client,
        start_date,
        end_date,
        last_request_at,
        min_interval,
    ) {
        Ok(mut twse_events) => events.append(&mut twse_events),
        Err(err) => {
            twse_failed = true;
            warnings.push(format!("TWSE 除權息來源暫時不可用：{err}"));
        }
    }

    match fetch_twse_etf_dividend_events(client, targets, last_request_at, min_interval) {
        Ok(mut etf_events) => events.append(&mut etf_events),
        Err(err) => {
            warnings.push(format!(
                "TWSE ETF 除息來源暫時不可用（已保留其他來源結果）：{err}"
            ));
        }
    }

    if has_tpex_target {
        match fetch_tpex_dividend_events_range(
            client,
            start_date,
            end_date,
            last_request_at,
            min_interval,
        ) {
            Ok(mut tpex_events) => events.append(&mut tpex_events),
            Err(err) => {
                tpex_failed = true;
                warnings.push(format!(
                    "TPEX 除權息來源暫時不可用（已保留 TWSE 結果）：{err}"
                ));
            }
        }
    }

    (events, warnings, twse_failed, tpex_failed)
}

fn build_auto_dividend_sync_prepared(
    conn: &Connection,
    client: &Client,
    targets: &[TwPriceSyncTarget],
    start_date: &str,
    end_date: &str,
    fetched_at: &str,
    mode: &str,
    source: &str,
    last_request_at: &mut Option<Instant>,
    min_interval: Duration,
) -> Result<PreparedAutoDividendSync, String> {
    let (events, mut warnings, twse_failed, tpex_failed) = collect_market_dividend_events(
        client,
        targets,
        start_date,
        end_date,
        last_request_at,
        min_interval,
    );
    let has_tpex_target = targets.iter().any(|target| is_tpex_market(&target.market));
    let status = if events.is_empty() && twse_failed && (!has_tpex_target || tpex_failed) {
        "failed".to_string()
    } else if twse_failed || tpex_failed {
        "partial".to_string()
    } else {
        "success".to_string()
    };

    let last_tpex_fail_date = read_app_setting(conn, "tw_dividend_sync_tpex_last_fail_date")?;
    let last_tpex_fail_streak =
        parse_optional_i64_setting(read_app_setting(conn, "tw_dividend_sync_tpex_fail_streak")?);
    let (tpex_fail_streak, tpex_fail_last_date) = compute_tpex_fail_streak(
        last_tpex_fail_date,
        last_tpex_fail_streak,
        end_date,
        has_tpex_target && tpex_failed,
    );
    if has_tpex_target && tpex_fail_streak >= 3 {
        warnings.push(format!(
            "TPEX 除權息來源已連續 {tpex_fail_streak} 天抓取失敗，請稍後重試或改以手動覆核。"
        ));
    }

    let (rows, stats) =
        build_auto_dividend_upsert_rows(conn, targets, &events, fetched_at, source)?;
    let mut security_name_by_id = HashMap::new();
    let mut security_symbol_by_id = HashMap::new();
    for target in targets {
        security_name_by_id.insert(target.security_id, target.name.clone());
        security_symbol_by_id.insert(target.security_id, target.symbol.clone());
    }
    let detected_items = rows
        .iter()
        .take(12)
        .map(|row| TwDividendDetectedItem {
            symbol: security_symbol_by_id
                .get(&row.security_id)
                .cloned()
                .unwrap_or_else(|| row.security_id.to_string()),
            name: security_name_by_id
                .get(&row.security_id)
                .cloned()
                .unwrap_or_else(|| row.security_id.to_string()),
            ex_dividend_date: row.ex_dividend_date.clone(),
            dividend_type: row.dividend_type.clone(),
            cash_dividend_per_share: row.cash_dividend_per_share,
            stock_dividend_per_thousand: row.stock_dividend_per_thousand,
            estimated_total: row.total_cash_dividend.or(row.total_stock_dividend),
        })
        .collect::<Vec<_>>();
    let upserted_rows = rows.len();
    let warning_count = warnings.len();
    let summary = format!(
        "股利{}：區間 {start_date} ~ {end_date}，匹配事件 {} 筆，寫入 {} 筆（現金 {}、股票 {}），略過手動 {} 筆、已存在 {} 筆、無持股 {} 筆{}。",
        if mode == "backfill" { "歷史回補" } else { "自動同步" },
        stats.matched_events,
        upserted_rows,
        stats.cash_rows,
        stats.stock_rows,
        stats.skipped_manual_rows,
        stats.skipped_existing_rows,
        stats.skipped_non_holding_rows,
        if warning_count > 0 {
            format!("，另有 {} 則警示", warning_count)
        } else {
            String::new()
        }
    );

    Ok(PreparedAutoDividendSync {
        rows,
        snapshot: TwDividendSyncSnapshot {
            synced_at: fetched_at.to_string(),
            status,
            source: "TWSE/TPEX 除權息".to_string(),
            mode: mode.to_string(),
            start_date: Some(start_date.to_string()),
            end_date: Some(end_date.to_string()),
            upserted_rows,
            cash_upserted_rows: stats.cash_rows,
            stock_upserted_rows: stats.stock_rows,
            matched_events: stats.matched_events,
            skipped_manual_rows: stats.skipped_manual_rows,
            skipped_non_holding_rows: stats.skipped_non_holding_rows,
            skipped_existing_rows: stats.skipped_existing_rows,
            tpex_failed: has_tpex_target && tpex_failed,
            tpex_fail_streak,
            warning_count,
            warnings,
            detected_items,
            summary,
        },
        tpex_fail_streak,
        tpex_fail_last_date,
    })
}

fn prepare_auto_dividend_sync(
    app: &AppHandle,
    client: &Client,
    targets: &[TwPriceSyncTarget],
    sync_end_date: Option<&str>,
    force_run: bool,
    last_request_at: &mut Option<Instant>,
    min_interval: Duration,
) -> Result<PreparedAutoDividendSync, String> {
    let synced_at = chrono::Local::now().to_rfc3339();
    let fallback_end = chrono::Local::now()
        .date_naive()
        .format("%Y-%m-%d")
        .to_string();
    let end_date = sync_end_date.unwrap_or(fallback_end.as_str()).to_string();

    if targets.is_empty() {
        return Ok(PreparedAutoDividendSync {
            rows: Vec::new(),
            snapshot: TwDividendSyncSnapshot {
                synced_at,
                status: "success".to_string(),
                source: "TWSE/TPEX 除權息".to_string(),
                mode: "daily".to_string(),
                start_date: None,
                end_date: Some(end_date),
                upserted_rows: 0,
                cash_upserted_rows: 0,
                stock_upserted_rows: 0,
                matched_events: 0,
                skipped_manual_rows: 0,
                skipped_non_holding_rows: 0,
                skipped_existing_rows: 0,
                tpex_failed: false,
                tpex_fail_streak: 0,
                warning_count: 0,
                warnings: Vec::new(),
                detected_items: Vec::new(),
                summary: "目前沒有持股標的，略過股利自動同步。".to_string(),
            },
            tpex_fail_streak: 0,
            tpex_fail_last_date: None,
        });
    }

    let conn = open_conn(app)?;
    let last_sync_at = read_app_setting(&conn, "tw_dividend_sync_last_at")?;
    let last_sync_date = last_sync_at.as_deref().and_then(extract_iso_date_prefix);
    if !force_run && last_sync_date.as_deref() == Some(end_date.as_str()) {
        return Ok(PreparedAutoDividendSync {
            rows: Vec::new(),
            snapshot: TwDividendSyncSnapshot {
                synced_at,
                status: "success".to_string(),
                source: "TWSE/TPEX 除權息".to_string(),
                mode: "daily".to_string(),
                start_date: None,
                end_date: Some(end_date),
                upserted_rows: 0,
                cash_upserted_rows: 0,
                stock_upserted_rows: 0,
                matched_events: 0,
                skipped_manual_rows: 0,
                skipped_non_holding_rows: 0,
                skipped_existing_rows: 0,
                tpex_failed: false,
                tpex_fail_streak: parse_optional_i64_setting(read_app_setting(
                    &conn,
                    "tw_dividend_sync_tpex_fail_streak",
                )?),
                warning_count: 0,
                warnings: Vec::new(),
                detected_items: Vec::new(),
                summary: "今天已同步過股利資料，略過重抓。".to_string(),
            },
            tpex_fail_streak: parse_optional_i64_setting(read_app_setting(
                &conn,
                "tw_dividend_sync_tpex_fail_streak",
            )?),
            tpex_fail_last_date: read_app_setting(&conn, "tw_dividend_sync_tpex_last_fail_date")?,
        });
    }

    let Some(earliest_trade_date) = load_earliest_trade_date_for_targets(&conn, targets)? else {
        return Ok(PreparedAutoDividendSync {
            rows: Vec::new(),
            snapshot: TwDividendSyncSnapshot {
                synced_at,
                status: "success".to_string(),
                source: "TWSE/TPEX 除權息".to_string(),
                mode: "daily".to_string(),
                start_date: None,
                end_date: Some(end_date),
                upserted_rows: 0,
                cash_upserted_rows: 0,
                stock_upserted_rows: 0,
                matched_events: 0,
                skipped_manual_rows: 0,
                skipped_non_holding_rows: 0,
                skipped_existing_rows: 0,
                tpex_failed: false,
                tpex_fail_streak: 0,
                warning_count: 0,
                warnings: Vec::new(),
                detected_items: Vec::new(),
                summary: "找不到可用交易期間，略過股利自動同步。".to_string(),
            },
            tpex_fail_streak: 0,
            tpex_fail_last_date: None,
        });
    };

    let start_date = compute_dividend_sync_start_date(&earliest_trade_date, &end_date, 90)?;

    if start_date > end_date {
        return Ok(PreparedAutoDividendSync {
            rows: Vec::new(),
            snapshot: TwDividendSyncSnapshot {
                synced_at,
                status: "success".to_string(),
                source: "TWSE/TPEX 除權息".to_string(),
                mode: "daily".to_string(),
                start_date: Some(start_date),
                end_date: Some(end_date),
                upserted_rows: 0,
                cash_upserted_rows: 0,
                stock_upserted_rows: 0,
                matched_events: 0,
                skipped_manual_rows: 0,
                skipped_non_holding_rows: 0,
                skipped_existing_rows: 0,
                tpex_failed: false,
                tpex_fail_streak: 0,
                warning_count: 0,
                warnings: Vec::new(),
                detected_items: Vec::new(),
                summary: "目前沒有新增日期區間需要同步股利。".to_string(),
            },
            tpex_fail_streak: 0,
            tpex_fail_last_date: None,
        });
    }

    build_auto_dividend_sync_prepared(
        &conn,
        client,
        targets,
        &start_date,
        &end_date,
        &synced_at,
        "daily",
        "auto-fetched",
        last_request_at,
        min_interval,
    )
}

#[tauri::command]
fn backfill_auto_dividends_last_12_months(
    app: AppHandle,
) -> Result<TwDividendSyncSnapshot, String> {
    let conn = open_conn(&app)?;
    let targets = load_price_sync_targets(&conn, None)?;
    let Some(earliest_trade_date) = load_earliest_trade_date_for_targets(&conn, &targets)? else {
        return Ok(TwDividendSyncSnapshot {
            synced_at: chrono::Local::now().to_rfc3339(),
            status: "success".to_string(),
            source: "TWSE/TPEX 除權息".to_string(),
            mode: "backfill".to_string(),
            start_date: None,
            end_date: None,
            upserted_rows: 0,
            cash_upserted_rows: 0,
            stock_upserted_rows: 0,
            matched_events: 0,
            skipped_manual_rows: 0,
            skipped_non_holding_rows: 0,
            skipped_existing_rows: 0,
            tpex_failed: false,
            tpex_fail_streak: 0,
            warning_count: 0,
            warnings: Vec::new(),
            detected_items: Vec::new(),
            summary: "目前沒有可回補的持股交易期間。".to_string(),
        });
    };
    let today = chrono::Local::now().date_naive();
    let cap_start = today
        .checked_sub_signed(ChronoDuration::days(365))
        .unwrap_or(today)
        .format("%Y-%m-%d")
        .to_string();
    let start_date = if earliest_trade_date < cap_start {
        cap_start
    } else {
        earliest_trade_date
    };
    let end_date = today.format("%Y-%m-%d").to_string();
    drop(conn);

    let client = Client::builder()
        .timeout(Duration::from_secs(12))
        .build()
        .map_err(|err| format!("建立股利回補 client 失敗: {err}"))?;
    let mut last_request_at: Option<Instant> = None;
    let min_interval = Duration::from_millis(250);
    let synced_at = chrono::Local::now().to_rfc3339();

    let conn = open_conn(&app)?;
    let prepared = build_auto_dividend_sync_prepared(
        &conn,
        &client,
        &targets,
        &start_date,
        &end_date,
        &synced_at,
        "backfill",
        "auto-fetched-backfill",
        &mut last_request_at,
        min_interval,
    )?;
    drop(conn);

    let mut write_conn = open_conn(&app)?;
    let tx = write_conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| format!("開始股利回補交易失敗: {err}"))?;
    upsert_auto_dividend_events_tx(&tx, &prepared.rows)?;
    upsert_app_setting_tx(
        &tx,
        "tw_dividend_sync_last_at",
        &prepared.snapshot.synced_at,
    )?;
    upsert_app_setting_tx(
        &tx,
        "tw_dividend_sync_last_status",
        &prepared.snapshot.status,
    )?;
    upsert_app_setting_tx(&tx, "tw_dividend_sync_status", &prepared.snapshot.status)?;
    upsert_app_setting_tx(
        &tx,
        "tw_dividend_sync_last_source",
        &prepared.snapshot.source,
    )?;
    upsert_app_setting_tx(
        &tx,
        "tw_dividend_sync_last_summary",
        &prepared.snapshot.summary,
    )?;
    upsert_app_setting_tx(
        &tx,
        "tw_dividend_sync_tpex_fail_streak",
        &prepared.tpex_fail_streak.to_string(),
    )?;
    if let Some(last_date) = &prepared.tpex_fail_last_date {
        upsert_app_setting_tx(&tx, "tw_dividend_sync_tpex_last_fail_date", last_date)?;
    } else {
        tx.execute(
            "DELETE FROM app_settings WHERE key = 'tw_dividend_sync_tpex_last_fail_date'",
            [],
        )
        .map_err(|err| format!("清除 TPEX 失敗日期失敗: {err}"))?;
    }
    tx.commit()
        .map_err(|err| format!("股利回補提交失敗: {err}"))?;

    Ok(prepared.snapshot)
}

fn upsert_auto_dividend_events_tx(
    tx: &Transaction,
    rows: &[AutoDividendUpsertRow],
) -> Result<(), String> {
    for row in rows {
        tx.execute(
            "
            INSERT INTO auto_dividend_events (
              account_id,
              security_id,
              dividend_type,
              ex_dividend_date,
              record_date,
              payment_date,
              cash_dividend_per_share,
              total_cash_dividend,
              stock_dividend_per_thousand,
              total_stock_dividend,
              holding_quantity_at_ex_date,
              source,
              fetched_at,
              updated_at
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, CURRENT_TIMESTAMP)
            ON CONFLICT(account_id, security_id, dividend_type, ex_dividend_date, source)
            DO UPDATE SET
              record_date = excluded.record_date,
              payment_date = excluded.payment_date,
              cash_dividend_per_share = excluded.cash_dividend_per_share,
              total_cash_dividend = excluded.total_cash_dividend,
              stock_dividend_per_thousand = excluded.stock_dividend_per_thousand,
              total_stock_dividend = excluded.total_stock_dividend,
              holding_quantity_at_ex_date = excluded.holding_quantity_at_ex_date,
              fetched_at = excluded.fetched_at,
              updated_at = CURRENT_TIMESTAMP
        ",
            params![
                row.account_id,
                row.security_id,
                row.dividend_type,
                row.ex_dividend_date,
                row.record_date,
                row.payment_date,
                row.cash_dividend_per_share,
                row.total_cash_dividend,
                row.stock_dividend_per_thousand,
                row.total_stock_dividend,
                row.holding_quantity_at_ex_date,
                row.source,
                row.fetched_at
            ],
        )
        .map_err(|err| format!("寫入自動股利失敗: {err}"))?;
    }
    Ok(())
}

fn parse_taiex_from_mi_index(body: &str) -> Result<Option<TaiexDailyQuote>, String> {
    let parsed: Value =
        serde_json::from_str(body).map_err(|err| format!("TWSE MI_INDEX JSON 解析失敗: {err}"))?;
    let stat = parsed
        .get("stat")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    if !stat.is_empty() && stat != "OK" {
        return Ok(None);
    }
    let trade_date = parsed
        .get("date")
        .and_then(|value| value.as_str())
        .and_then(roc_date_to_iso);
    let Some(trade_date) = trade_date else {
        return Ok(None);
    };

    let tables = parsed
        .get("tables")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();

    for table in tables {
        let fields = table
            .get("fields")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();
        if fields.is_empty() {
            continue;
        }
        let field_names = fields
            .iter()
            .map(|field| field.as_str().unwrap_or("").trim().to_string())
            .collect::<Vec<_>>();
        let index_name_idx = field_names
            .iter()
            .position(|field| field.contains("指數"))
            .unwrap_or(0);
        let close_idx = field_names
            .iter()
            .position(|field| field.contains("收盤指數") || field == "收盤")
            .unwrap_or(1);
        let change_idx = field_names
            .iter()
            .position(|field| field.contains("漲跌點數"));
        let change_pct_idx = field_names
            .iter()
            .position(|field| field.contains("漲跌百分比"));
        let rows = table
            .get("data")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();

        for row in rows {
            let values = match row.as_array() {
                Some(value) => value,
                None => continue,
            };
            let index_name = values
                .get(index_name_idx)
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .trim();
            if !index_name.contains("發行量加權股價指數") {
                continue;
            }
            let close = values
                .get(close_idx)
                .and_then(|value| value.as_str())
                .and_then(parse_numeric);
            let Some(close) = close else {
                continue;
            };
            let change_points = change_idx
                .and_then(|idx| values.get(idx))
                .and_then(|value| value.as_str())
                .and_then(parse_change);
            let change_percent_ratio = change_pct_idx
                .and_then(|idx| values.get(idx))
                .and_then(|value| value.as_str())
                .and_then(parse_change)
                .map(|value| value / 100.0);

            return Ok(Some(TaiexDailyQuote {
                trade_date: trade_date.clone(),
                close,
                change_points,
                change_percent_ratio,
            }));
        }
    }

    Ok(None)
}

fn parse_taiex_from_mis(body: &str) -> Result<Option<TaiexDailyQuote>, String> {
    let parsed: Value =
        serde_json::from_str(body).map_err(|err| format!("TWSE MIS JSON 解析失敗: {err}"))?;
    let rows = parsed
        .get("msgArray")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    for row in rows {
        let symbol = row
            .get("c")
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .trim()
            .to_lowercase();
        if symbol != "t00" {
            continue;
        }
        let trade_date = row
            .get("d")
            .and_then(|value| value.as_str())
            .and_then(roc_date_to_iso);
        let Some(trade_date) = trade_date else {
            continue;
        };
        let close = row
            .get("z")
            .and_then(|value| value.as_str())
            .and_then(parse_numeric)
            .or_else(|| {
                row.get("y")
                    .and_then(|value| value.as_str())
                    .and_then(parse_numeric)
            });
        let Some(close) = close else {
            continue;
        };
        let previous_close = row
            .get("y")
            .and_then(|value| value.as_str())
            .and_then(parse_numeric);
        let change_points = previous_close.map(|prev| close - prev);
        let change_percent_ratio = previous_close.and_then(|prev| {
            if prev.abs() < f64::EPSILON {
                None
            } else {
                Some((close - prev) / prev)
            }
        });
        return Ok(Some(TaiexDailyQuote {
            trade_date,
            close,
            change_points,
            change_percent_ratio,
        }));
    }
    Ok(None)
}

fn fetch_taiex_quote_from_mis(
    client: &Client,
    last_request_at: &mut Option<Instant>,
    min_interval: Duration,
) -> Result<Option<TaiexDailyQuote>, String> {
    let url = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=tse_t00.tw&json=1&delay=0";
    retry_with_backoff(3, Duration::from_secs(2), |_attempt| {
        wait_for_request_slot(last_request_at, min_interval);
        let response = client
            .get(url)
            .send()
            .map_err(|err| format!("TWSE MIS 大盤連線失敗: {err}"))?;
        let status = response.status();
        let body = response
            .text()
            .map_err(|err| format!("TWSE MIS 大盤讀取失敗: {err}"))?;
        if !status.is_success() {
            return Err(format!(
                "TWSE MIS 大盤 HTTP {}: {}",
                status.as_u16(),
                response_preview(&body, 120)
            ));
        }
        parse_taiex_from_mis(&body)
    })
}

fn fetch_taiex_quote_by_date(
    client: &Client,
    iso_date: &str,
    last_request_at: &mut Option<Instant>,
    min_interval: Duration,
) -> Result<Option<TaiexDailyQuote>, String> {
    let yyyymmdd =
        iso_to_yyyymmdd(iso_date).ok_or_else(|| format!("無法解析大盤查詢日期：{iso_date}"))?;
    let url = format!(
        "https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={yyyymmdd}&type=IND"
    );
    let mi_result = retry_with_backoff(2, Duration::from_secs(2), |_attempt| {
        wait_for_request_slot(last_request_at, min_interval);
        let response = client
            .get(url.clone())
            .send()
            .map_err(|err| format!("TWSE MI_INDEX 連線失敗: {err}"))?;
        let status = response.status();
        let body = response
            .text()
            .map_err(|err| format!("TWSE MI_INDEX 回應讀取失敗: {err}"))?;
        if !status.is_success() {
            return Err(format!(
                "TWSE MI_INDEX HTTP {}: {}",
                status.as_u16(),
                response_preview(&body, 120)
            ));
        }
        parse_taiex_from_mi_index(&body)
    });
    match mi_result {
        Ok(Some(quote)) => Ok(Some(quote)),
        Ok(None) => {
            let today_iso = chrono::Local::now()
                .date_naive()
                .format("%Y-%m-%d")
                .to_string();
            if iso_date == today_iso {
                fetch_taiex_quote_from_mis(client, last_request_at, min_interval)
            } else {
                Ok(None)
            }
        }
        Err(_err) => {
            let today_iso = chrono::Local::now()
                .date_naive()
                .format("%Y-%m-%d")
                .to_string();
            if iso_date == today_iso {
                fetch_taiex_quote_from_mis(client, last_request_at, min_interval)
            } else {
                Ok(None)
            }
        }
    }
}

fn find_taiex_quote_on_or_before(
    client: &Client,
    iso_date: &str,
    max_lookback_days: usize,
    last_request_at: &mut Option<Instant>,
    min_interval: Duration,
) -> Result<Option<TaiexDailyQuote>, String> {
    let mut probe = iso_date.to_string();
    for _ in 0..=max_lookback_days {
        if let Some(quote) =
            fetch_taiex_quote_by_date(client, &probe, last_request_at, min_interval)?
        {
            return Ok(Some(quote));
        }
        let Some(prev) = previous_trading_date(&probe) else {
            break;
        };
        probe = prev;
    }
    Ok(None)
}

fn find_taiex_quote_on_or_after(
    client: &Client,
    iso_date: &str,
    max_lookahead_days: usize,
    last_request_at: &mut Option<Instant>,
    min_interval: Duration,
) -> Result<Option<TaiexDailyQuote>, String> {
    let mut probe = iso_date.to_string();
    for _ in 0..=max_lookahead_days {
        if let Some(quote) =
            fetch_taiex_quote_by_date(client, &probe, last_request_at, min_interval)?
        {
            return Ok(Some(quote));
        }
        let Some(next) = next_trading_date(&probe) else {
            break;
        };
        probe = next;
    }
    Ok(None)
}

fn fetch_taiex_benchmark_snapshot(
    client: &Client,
    latest_target_date: &str,
    last_request_at: &mut Option<Instant>,
    min_interval: Duration,
) -> Result<Option<TwBenchmarkIndexSnapshot>, String> {
    let Some(latest) = find_taiex_quote_on_or_before(
        client,
        latest_target_date,
        10,
        last_request_at,
        min_interval,
    )?
    else {
        return Ok(None);
    };

    let previous_seed = previous_trading_date(&latest.trade_date)
        .ok_or_else(|| format!("無法推導前一交易日：{}", latest.trade_date))?;
    let Some(previous) =
        find_taiex_quote_on_or_before(client, &previous_seed, 10, last_request_at, min_interval)?
    else {
        return Ok(None);
    };

    let today_change = latest.close - previous.close;
    let today_change_percent = if previous.close.abs() < f64::EPSILON {
        0.0
    } else {
        today_change / previous.close
    };

    let month_start = first_calendar_day_of_month(&latest.trade_date);
    let month_first_quote = if let Some(month_start_date) = month_start {
        find_taiex_quote_on_or_after(client, &month_start_date, 15, last_request_at, min_interval)?
    } else {
        None
    };
    let month_to_date_return = month_first_quote.as_ref().and_then(|quote| {
        if quote.close.abs() < f64::EPSILON {
            None
        } else {
            Some((latest.close - quote.close) / quote.close)
        }
    });

    let year_start = first_calendar_day_of_year(&latest.trade_date);
    let year_first_quote = if let Some(year_start_date) = year_start {
        find_taiex_quote_on_or_after(client, &year_start_date, 31, last_request_at, min_interval)?
    } else {
        None
    };
    let year_to_date_return = year_first_quote.as_ref().and_then(|quote| {
        if quote.close.abs() < f64::EPSILON {
            None
        } else {
            Some((latest.close - quote.close) / quote.close)
        }
    });

    Ok(Some(TwBenchmarkIndexSnapshot {
        name: "TAIEX".to_string(),
        latest_close: latest.close,
        previous_close: previous.close,
        today_change,
        today_change_percent,
        latest_date: latest.trade_date,
        previous_date: previous.trade_date,
        month_to_date_return,
        year_to_date_return,
        source: "TWSE".to_string(),
    }))
}

fn load_price_sync_targets(
    conn: &Connection,
    security_id: Option<i64>,
) -> Result<Vec<TwPriceSyncTarget>, String> {
    if let Some(id) = security_id {
        let mut stmt = conn
            .prepare(
                "
                SELECT id, symbol, name, market, currency
                FROM securities
                WHERE id = ?1
            ",
            )
            .map_err(|err| format!("讀取指定股票失敗: {err}"))?;
        let rows = stmt
            .query_map(params![id], |row| {
                Ok(TwPriceSyncTarget {
                    security_id: row.get(0)?,
                    symbol: row.get::<_, String>(1)?,
                    name: row.get::<_, String>(2)?,
                    market: row.get::<_, String>(3)?,
                    currency: row.get::<_, String>(4)?,
                })
            })
            .map_err(|err| format!("解析指定股票失敗: {err}"))?;
        let mut targets = Vec::new();
        for row in rows {
            targets.push(row.map_err(|err| format!("讀取指定股票資料列失敗: {err}"))?);
        }
        return Ok(targets);
    }

    let mut stmt = conn
        .prepare(
            "
            SELECT
              s.id,
              s.symbol,
              s.name,
              s.market,
              s.currency,
              SUM(t.quantity - COALESCE(sa.allocated_qty, 0)) AS remaining_qty
            FROM transactions t
            INNER JOIN securities s ON s.id = t.security_id
            LEFT JOIN (
              SELECT buy_transaction_id, SUM(quantity) AS allocated_qty
              FROM sell_allocations
              GROUP BY buy_transaction_id
            ) sa ON sa.buy_transaction_id = t.id
            WHERE t.security_id IS NOT NULL
              AND t.type IN ('BUY', 'STOCK_DIVIDEND')
            GROUP BY s.id, s.symbol, s.name, s.market, s.currency
            HAVING remaining_qty > 0
            ORDER BY s.symbol ASC
        ",
        )
        .map_err(|err| format!("讀取持股股票失敗: {err}"))?;

    let rows = stmt
        .query_map([], |row| {
            Ok(TwPriceSyncTarget {
                security_id: row.get(0)?,
                symbol: row.get::<_, String>(1)?,
                name: row.get::<_, String>(2)?,
                market: row.get::<_, String>(3)?,
                currency: row.get::<_, String>(4)?,
            })
        })
        .map_err(|err| format!("解析持股股票失敗: {err}"))?;

    let mut targets = Vec::new();
    for row in rows {
        targets.push(row.map_err(|err| format!("讀取持股股票資料列失敗: {err}"))?);
    }
    Ok(targets)
}

fn sync_tw_prices_inner(
    app: AppHandle,
    security_id: Option<i64>,
    force_dividend_sync: bool,
) -> Result<TwPriceSyncResult, String> {
    let synced_at = chrono::Local::now().to_rfc3339();
    let source_label = "TWSE/TPEX 官方收盤價（v2.1：TWSE 開放資料 + TPEX 日線）".to_string();
    let scope = if security_id.is_some() {
        "single".to_string()
    } else {
        "holdings".to_string()
    };

    let read_conn = open_conn(&app)?;
    let targets = load_price_sync_targets(&read_conn, security_id)?;
    drop(read_conn);

    let client = Client::builder()
        .timeout(Duration::from_secs(12))
        .build()
        .map_err(|err| format!("建立價格同步 client 失敗: {err}"))?;

    let mut items: Vec<TwPriceSyncItem> = Vec::new();
    let mut writes: Vec<(i64, String, f64, String, String)> = Vec::new();
    let mut success_count = 0usize;
    let mut failed_count = 0usize;
    let mut missing_count = 0usize;
    let mut updated_through_date: Option<String> = None;
    let mut last_request_at: Option<Instant> = None;
    let min_interval = Duration::from_millis(250);
    let has_twse_target = targets.iter().any(|target| is_twse_market(&target.market));
    let twse_symbols = targets
        .iter()
        .filter(|target| is_twse_market(&target.market))
        .map(|target| target.symbol.trim().to_uppercase())
        .collect::<Vec<_>>();
    let mut twse_quote_map: HashMap<String, TwMarketQuote> = HashMap::new();
    let mut twse_quote_error: Option<String> = None;
    if has_twse_target {
        match fetch_twse_daily_all_quotes(
            &client,
            &twse_symbols,
            &mut last_request_at,
            min_interval,
        ) {
            Ok(map) => {
                twse_quote_map = map;
            }
            Err(err) => {
                twse_quote_error = Some(err);
            }
        }
    }

    for target in &targets {
        let symbol = target.symbol.trim().to_uppercase();
        let target_name = target.name.clone();
        let target_market = target.market.clone();
        let target_currency = target.currency.clone();
        let source_tag = if is_twse_market(&target_market) {
            "twse_sync_v1:TWSE".to_string()
        } else if is_tpex_market(&target_market) {
            "twse_sync_v1:TPEX".to_string()
        } else {
            "twse_sync_v1:UNSUPPORTED".to_string()
        };

        if !is_twse_market(&target_market) && !is_tpex_market(&target_market) {
            missing_count += 1;
            items.push(TwPriceSyncItem {
                security_id: target.security_id,
                symbol: symbol.clone(),
                name: target_name.clone(),
                market: target_market.clone(),
                status: "MISSING_DATA".to_string(),
                latest_price_date: None,
                previous_price_date: None,
                source: source_tag.clone(),
                reason: Some("非台股市場（僅支援 TWSE / TPEX）".to_string()),
            });
            continue;
        }

        let quote_result = if is_twse_market(&target_market) {
            if let Some(err) = &twse_quote_error {
                Err(err.clone())
            } else {
                Ok(twse_quote_map.get(&symbol).cloned())
            }
        } else {
            fetch_tpex_symbol_quote(&client, &symbol, &mut last_request_at, min_interval)
        };
        let quote = match quote_result {
            Ok(Some(value)) => value,
            Ok(None) => {
                missing_count += 1;
                items.push(TwPriceSyncItem {
                    security_id: target.security_id,
                    symbol: symbol.clone(),
                    name: target_name.clone(),
                    market: target_market.clone(),
                    status: "MISSING_DATA".to_string(),
                    latest_price_date: None,
                    previous_price_date: None,
                    source: source_tag.clone(),
                    reason: Some("來源缺少此代號的收盤資料".to_string()),
                });
                continue;
            }
            Err(err) => {
                failed_count += 1;
                items.push(TwPriceSyncItem {
                    security_id: target.security_id,
                    symbol: symbol.clone(),
                    name: target_name.clone(),
                    market: target_market.clone(),
                    status: "FAILED".to_string(),
                    latest_price_date: None,
                    previous_price_date: None,
                    source: source_tag.clone(),
                    reason: Some(err),
                });
                continue;
            }
        };

        if updated_through_date
            .as_ref()
            .map(|date| quote.trade_date.as_str() > date.as_str())
            .unwrap_or(true)
        {
            updated_through_date = Some(quote.trade_date.clone());
        }

        writes.push((
            target.security_id,
            quote.trade_date.clone(),
            quote.close_price,
            target_currency.clone(),
            source_tag.clone(),
        ));

        let mut previous_price_date: Option<String> = None;
        if let Some(previous_close) = quote.previous_close_price {
            if let Some(prev_date) = previous_trading_date(&quote.trade_date) {
                writes.push((
                    target.security_id,
                    prev_date.clone(),
                    previous_close,
                    target_currency.clone(),
                    source_tag.clone(),
                ));
                previous_price_date = Some(prev_date);
            }
        }

        if previous_price_date.is_some() {
            success_count += 1;
            items.push(TwPriceSyncItem {
                security_id: target.security_id,
                symbol: symbol.clone(),
                name: target_name.clone(),
                market: target_market.clone(),
                status: "SUCCESS".to_string(),
                latest_price_date: Some(quote.trade_date),
                previous_price_date,
                source: source_tag.clone(),
                reason: None,
            });
        } else {
            missing_count += 1;
            items.push(TwPriceSyncItem {
                security_id: target.security_id,
                symbol,
                name: target_name,
                market: target_market,
                status: "MISSING_DATA".to_string(),
                latest_price_date: Some(quote.trade_date),
                previous_price_date: None,
                source: source_tag,
                reason: Some("無法取得前一交易日收盤價（漲跌欄位不可解析）".to_string()),
            });
        }
    }

    let mut benchmark_warning: Option<String> = None;
    let benchmark_index = if let Some(target_date) = updated_through_date.clone() {
        match fetch_taiex_benchmark_snapshot(
            &client,
            &target_date,
            &mut last_request_at,
            min_interval,
        ) {
            Ok(snapshot) => snapshot,
            Err(err) => {
                benchmark_warning = Some(err);
                None
            }
        }
    } else {
        None
    };
    let prepared_dividend_sync = prepare_auto_dividend_sync(
        &app,
        &client,
        &targets,
        updated_through_date.as_deref(),
        force_dividend_sync,
        &mut last_request_at,
        min_interval,
    )?;

    let mut write_conn = open_conn(&app)?;
    let tx = write_conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|err| format!("開始價格同步交易失敗: {err}"))?;

    let mut updated_rows = 0usize;
    for row in &writes {
        tx.execute(
            "
            INSERT INTO price_snapshots (security_id, price_date, close_price, currency, source)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(security_id, price_date)
            DO UPDATE SET
              close_price = excluded.close_price,
              currency = excluded.currency,
              source = excluded.source
        ",
            params![row.0, row.1, row.2, row.3, row.4],
        )
        .map_err(|err| format!("寫入價格快照失敗: {err}"))?;
        updated_rows += 1;
    }
    upsert_auto_dividend_events_tx(&tx, &prepared_dividend_sync.rows)?;

    let status = if success_count > 0 && failed_count == 0 && missing_count == 0 {
        "SUCCESS"
    } else if success_count > 0 || updated_rows > 0 {
        "PARTIAL"
    } else {
        "FAILED"
    };
    let summary = format!(
        "台股價格同步完成：成功 {success_count} 檔、失敗 {failed_count} 檔、缺資料 {missing_count} 檔，更新列數 {updated_rows}，更新日期 {}。",
        updated_through_date
            .clone()
            .unwrap_or_else(|| "無可用資料".to_string())
    );
    if let Some(warning) = &benchmark_warning {
        items.push(TwPriceSyncItem {
            security_id: 0,
            symbol: "TAIEX".to_string(),
            name: "加權股價指數".to_string(),
            market: "TWSE".to_string(),
            status: "FAILED".to_string(),
            latest_price_date: None,
            previous_price_date: None,
            source: "benchmark:TWSE".to_string(),
            reason: Some(format!("大盤指數同步失敗（不影響個股價格同步）：{warning}")),
        });
    }

    upsert_app_setting_tx(&tx, "tw_price_sync_last_at", &synced_at)?;
    upsert_app_setting_tx(&tx, "tw_price_sync_last_status", status)?;
    upsert_app_setting_tx(&tx, "tw_price_sync_last_source", &source_label)?;
    upsert_app_setting_tx(&tx, "tw_price_sync_last_summary", &summary)?;
    upsert_app_setting_tx(
        &tx,
        "tw_dividend_sync_last_at",
        &prepared_dividend_sync.snapshot.synced_at,
    )?;
    upsert_app_setting_tx(
        &tx,
        "tw_dividend_sync_last_status",
        &prepared_dividend_sync.snapshot.status,
    )?;
    upsert_app_setting_tx(
        &tx,
        "tw_dividend_sync_status",
        &prepared_dividend_sync.snapshot.status,
    )?;
    upsert_app_setting_tx(
        &tx,
        "tw_dividend_sync_last_source",
        &prepared_dividend_sync.snapshot.source,
    )?;
    upsert_app_setting_tx(
        &tx,
        "tw_dividend_sync_last_summary",
        &prepared_dividend_sync.snapshot.summary,
    )?;
    upsert_app_setting_tx(
        &tx,
        "tw_dividend_sync_tpex_fail_streak",
        &prepared_dividend_sync.tpex_fail_streak.to_string(),
    )?;
    if let Some(last_date) = &prepared_dividend_sync.tpex_fail_last_date {
        upsert_app_setting_tx(&tx, "tw_dividend_sync_tpex_last_fail_date", last_date)?;
    } else {
        tx.execute(
            "DELETE FROM app_settings WHERE key = 'tw_dividend_sync_tpex_last_fail_date'",
            [],
        )
        .map_err(|err| format!("清除 TPEX 失敗日期失敗: {err}"))?;
    }
    if let Some(benchmark) = &benchmark_index {
        upsert_app_setting_tx(&tx, "tw_benchmark_name", &benchmark.name)?;
        upsert_app_setting_tx(
            &tx,
            "tw_benchmark_latest_close",
            &benchmark.latest_close.to_string(),
        )?;
        upsert_app_setting_tx(
            &tx,
            "tw_benchmark_previous_close",
            &benchmark.previous_close.to_string(),
        )?;
        upsert_app_setting_tx(
            &tx,
            "tw_benchmark_today_change",
            &benchmark.today_change.to_string(),
        )?;
        upsert_app_setting_tx(
            &tx,
            "tw_benchmark_today_change_percent",
            &benchmark.today_change_percent.to_string(),
        )?;
        upsert_app_setting_tx(&tx, "tw_benchmark_latest_date", &benchmark.latest_date)?;
        upsert_app_setting_tx(&tx, "tw_benchmark_previous_date", &benchmark.previous_date)?;
        upsert_app_setting_tx(
            &tx,
            "tw_benchmark_month_to_date_return",
            &benchmark
                .month_to_date_return
                .map(|value| value.to_string())
                .unwrap_or_default(),
        )?;
        upsert_app_setting_tx(
            &tx,
            "tw_benchmark_year_to_date_return",
            &benchmark
                .year_to_date_return
                .map(|value| value.to_string())
                .unwrap_or_default(),
        )?;
        if let Some(month_start_date) = first_calendar_day_of_month(&benchmark.latest_date) {
            upsert_app_setting_tx(&tx, "tw_benchmark_month_start_date", &month_start_date)?;
        }
        if let Some(year_start_date) = first_calendar_day_of_year(&benchmark.latest_date) {
            upsert_app_setting_tx(&tx, "tw_benchmark_year_start_date", &year_start_date)?;
        }
        upsert_app_setting_tx(&tx, "tw_benchmark_source", &benchmark.source)?;
    }

    tx.commit()
        .map_err(|err| format!("提交價格同步交易失敗: {err}"))?;

    Ok(TwPriceSyncResult {
        synced_at,
        source: source_label,
        scope,
        success_count,
        failed_count,
        missing_count,
        updated_rows,
        updated_through_date,
        benchmark_index,
        dividend_sync: Some(prepared_dividend_sync.snapshot),
        items,
    })
}

#[allow(non_snake_case)]
#[tauri::command]
fn sync_tw_prices_for_holdings(
    app: AppHandle,
    forceDividendSync: Option<bool>,
) -> Result<TwPriceSyncResult, String> {
    sync_tw_prices_inner(app, None, forceDividendSync.unwrap_or(false))
}

#[allow(non_snake_case)]
#[tauri::command]
fn sync_tw_price_for_security(
    app: AppHandle,
    securityId: i64,
    forceDividendSync: Option<bool>,
) -> Result<TwPriceSyncResult, String> {
    sync_tw_prices_inner(app, Some(securityId), forceDividendSync.unwrap_or(false))
}

#[tauri::command]
fn repair_tw_stock_securities_master(app: AppHandle) -> Result<RepairStockMasterResult, String> {
    let conn = open_conn(&app)?;
    let mut stmt = conn
        .prepare(
            "
            SELECT id, symbol, COALESCE(name, '')
            FROM securities
            WHERE TRIM(COALESCE(name, '')) = ''
               OR UPPER(TRIM(COALESCE(name, ''))) = UPPER(TRIM(symbol))
            ORDER BY id ASC
        ",
        )
        .map_err(|err| format!("讀取待修復股票主檔失敗: {err}"))?;
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(|err| format!("解析待修復股票主檔失敗: {err}"))?;

    let mut checked = 0usize;
    let mut repaired = 0usize;
    let mut skipped = 0usize;
    let mut items: Vec<RepairStockMasterItem> = Vec::new();

    for row in rows {
        let (security_id, symbol, old_name) =
            row.map_err(|err| format!("讀取待修復股票資料列失敗: {err}"))?;
        checked += 1;
        match lookup_tw_stock_master(symbol.clone()) {
            Ok(result) => {
                if !result.ok || result.record.is_none() {
                    skipped += 1;
                    items.push(RepairStockMasterItem {
                        security_id,
                        symbol,
                        old_name,
                        status: "skipped".to_string(),
                        reason: result
                            .reason_message
                            .unwrap_or_else(|| "查詢台股主檔失敗".to_string()),
                        new_name: None,
                    });
                    continue;
                }
                let record = result.record.expect("record exists when ok");
                if record.name.trim().is_empty() || record.name.trim().eq_ignore_ascii_case(&symbol)
                {
                    skipped += 1;
                    items.push(RepairStockMasterItem {
                        security_id,
                        symbol,
                        old_name,
                        status: "skipped".to_string(),
                        reason: "查到的名稱仍不完整，已略過。".to_string(),
                        new_name: None,
                    });
                    continue;
                }

                conn.execute(
                    "
                    UPDATE securities
                    SET name = ?1, market = ?2, currency = ?3, board_lot_size = ?4
                    WHERE id = ?5
                ",
                    params![
                        record.name,
                        record.market,
                        record.currency,
                        record.board_lot_size,
                        security_id
                    ],
                )
                .map_err(|err| format!("修復股票主檔寫入失敗: {err}"))?;
                repaired += 1;
                items.push(RepairStockMasterItem {
                    security_id,
                    symbol,
                    old_name,
                    status: "repaired".to_string(),
                    reason: "已更新為台股主檔資料".to_string(),
                    new_name: Some(record.name),
                });
            }
            Err(error) => {
                skipped += 1;
                items.push(RepairStockMasterItem {
                    security_id,
                    symbol,
                    old_name,
                    status: "skipped".to_string(),
                    reason: format!("查詢失敗：{error}"),
                    new_name: None,
                });
            }
        }
    }

    Ok(RepairStockMasterResult {
        checked,
        repaired,
        skipped,
        items,
    })
}

#[allow(non_snake_case)]
#[tauri::command]
fn restore_database(app: AppHandle, dataBase64: String) -> Result<(), String> {
    let bytes = general_purpose::STANDARD
        .decode(dataBase64)
        .map_err(|err| format!("備份資料格式錯誤: {err}"))?;

    let restore_root = std::env::temp_dir().join(format!(
        "stock-vault-db-restore-{}",
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    fs::create_dir_all(&restore_root).map_err(|err| format!("建立還原暫存資料夾失敗: {err}"))?;
    let restore_path = restore_root.join("stock-vault.restore.db");

    let result = (|| -> Result<(), String> {
        fs::write(&restore_path, &bytes).map_err(|err| format!("寫入暫存還原檔失敗: {err}"))?;
        ensure_sqlite_header(&restore_path)?;
        let conn = Connection::open_with_flags(&restore_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|err| format!("開啟還原 SQLite 失敗: {err}"))?;
        let schema_version_number = read_sqlite_schema_version_number(&conn)?;
        let current_schema_version_number = current_sqlite_schema_version_number();
        if schema_version_number != current_schema_version_number {
            return Err(format!(
                "備份 schema 為 stock-vault.sqlite.v{}，目前程式需要 {}，已阻擋還原。",
                schema_version_number,
                current_sqlite_schema_version_label()
            ));
        }

        for target in db_candidates(&app)? {
            replace_sqlite_database_from_file(&restore_path, &target)?;
        }
        Ok(())
    })();

    let _ = fs::remove_dir_all(&restore_root);
    result
}

#[tauri::command]
fn export_db_base64(app: AppHandle) -> Result<String, String> {
    backup_database(app)
}

#[allow(non_snake_case)]
#[tauri::command]
fn restore_db_base64(app: AppHandle, dataBase64: String) -> Result<(), String> {
    restore_database(app, dataBase64)
}

fn history_db_path(app: &AppHandle) -> Result<PathBuf, String> {
    if let Some(portable_root) = portable_root_dir()? {
        let data_dir = portable_root.join("data");
        fs::create_dir_all(&data_dir).map_err(|err| format!("建立 portable History DB 目錄失敗: {err}"))?;
        return Ok(data_dir.join("stock-history.db"));
    }

    let app_data = app
        .path()
        .app_data_dir()
        .map_err(|err| format!("無法取得 History DB 目錄: {err}"))?;
    fs::create_dir_all(&app_data).map_err(|err| format!("建立 History DB 目錄失敗: {err}"))?;
    Ok(app_data.join("stock-history.db"))
}

fn open_history_conn(app: &AppHandle) -> Result<Connection, String> {
    let path = history_db_path(app)?;
    let conn = Connection::open(path).map_err(|err| format!("開啟 History DB 失敗: {err}"))?;
    conn.busy_timeout(Duration::from_millis(5000))
        .map_err(|err| format!("設定 History DB busy timeout 失敗: {err}"))?;
    Ok(conn)
}

fn snake_to_camel_key(key: &str) -> String {
    let mut result = String::new();
    let mut uppercase_next = false;
    for ch in key.chars() {
        if ch == '_' {
            uppercase_next = true;
            continue;
        }
        if uppercase_next {
            result.push(ch.to_ascii_uppercase());
            uppercase_next = false;
        } else {
            result.push(ch);
        }
    }
    result
}

fn camelize_json(value: Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.into_iter().map(camelize_json).collect()),
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(key, value)| (snake_to_camel_key(&key), camelize_json(value)))
                .collect(),
        ),
        other => other,
    }
}

fn home_documents_stock_vault() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .map(|home| home.join("Documents/stock-vault"))
}

fn resolve_stock_vault_workspace_root() -> Result<PathBuf, String> {
    let mut candidates = Vec::new();
    if let Some(value) = std::env::var_os("STOCK_VAULT_WORKSPACE_ROOT") {
        candidates.push(PathBuf::from(value));
    }
    if let Ok(current) = std::env::current_dir() {
        candidates.push(current.clone());
        candidates.push(current.join("stock-vault"));
    }
    if let Some(home_docs) = home_documents_stock_vault() {
        candidates.push(home_docs);
    }
    for candidate in candidates {
        if candidate.join("scripts/history-db-cli.mjs").exists() {
            return Ok(candidate);
        }
    }

    Err("找不到 stock-vault workspace，無法執行 History DB CLI。".to_string())
}

fn resolve_node_binary() -> Result<PathBuf, String> {
    if let Some(value) = std::env::var_os("NODE_BINARY") {
        let path = PathBuf::from(value);
        if path.exists() {
            return Ok(path);
        }
    }

    for candidate in [
        "/opt/homebrew/bin/node",
        "/usr/local/bin/node",
        "/usr/bin/node",
    ] {
        let path = PathBuf::from(candidate);
        if path.exists() {
            return Ok(path);
        }
    }

    Ok(PathBuf::from("node"))
}

fn run_history_cli_json(
    app: &AppHandle,
    subcommand: &str,
    extra_args: &[String],
) -> Result<Value, String> {
    let workspace = resolve_stock_vault_workspace_root()?;
    let node = resolve_node_binary()?;
    let script = workspace.join("scripts/history-db-cli.mjs");
    let history_db = history_db_path(app)?;

    let mut command = Command::new(node);
    command
        .arg("--no-warnings")
        .arg(script)
        .arg(subcommand)
        .arg("--history-db")
        .arg(history_db)
        .args(extra_args)
        .current_dir(workspace);

    let output = command
        .output()
        .map_err(|err| format!("執行 History DB CLI 失敗: {err}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let detail = if !stderr.is_empty() {
            stderr
        } else if !stdout.is_empty() {
            stdout
        } else {
            format!("exit code {:?}", output.status.code())
        };
        return Err(format!("History DB CLI 執行失敗：{detail}"));
    }

    let stdout = String::from_utf8(output.stdout)
        .map_err(|err| format!("History DB CLI 輸出不是有效 UTF-8: {err}"))?;
    serde_json::from_str::<Value>(stdout.trim())
        .map_err(|err| format!("解析 History DB CLI JSON 輸出失敗: {err}"))
}

fn run_timesfm_workbench_cli_json(
    app: &AppHandle,
    action: &str,
    payload: Value,
) -> Result<Value, String> {
    let workspace = resolve_stock_vault_workspace_root()?;
    let node = resolve_node_binary()?;
    let script = workspace.join("scripts/timesfm-workbench-cli.mjs");
    let history_db = history_db_path(app)?;

    let mut request = match payload {
        Value::Object(map) => map,
        _ => serde_json::Map::new(),
    };
    request.insert("action".to_string(), Value::String(action.to_string()));
    request.insert(
        "historyDbPath".to_string(),
        Value::String(history_db.to_string_lossy().to_string()),
    );

    let mut command = Command::new(node);
    command
        .arg("--no-warnings")
        .arg(script)
        .current_dir(workspace)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = command
        .spawn()
        .map_err(|err| format!("啟動 TimesFM Workbench CLI 失敗: {err}"))?;

    if let Some(stdin) = child.stdin.as_mut() {
        let input = serde_json::to_vec(&Value::Object(request))
            .map_err(|err| format!("序列化 TimesFM Workbench CLI 請求失敗: {err}"))?;
        stdin
            .write_all(&input)
            .map_err(|err| format!("寫入 TimesFM Workbench CLI stdin 失敗: {err}"))?;
    }

    let output = child
        .wait_with_output()
        .map_err(|err| format!("執行 TimesFM Workbench CLI 失敗: {err}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let detail = if !stderr.is_empty() {
            stderr
        } else if !stdout.is_empty() {
            stdout
        } else {
            format!("exit code {:?}", output.status.code())
        };
        return Err(format!("TimesFM Workbench CLI 執行失敗：{detail}"));
    }

    let stdout = String::from_utf8(output.stdout)
        .map_err(|err| format!("TimesFM Workbench CLI 輸出不是有效 UTF-8: {err}"))?;
    serde_json::from_str::<Value>(stdout.trim())
        .map_err(|err| format!("解析 TimesFM Workbench CLI JSON 輸出失敗: {err}"))
}

fn run_history_forecast_runner_json(
    app: &AppHandle,
    action: &str,
    payload: Value,
) -> Result<Value, String> {
    let workspace = resolve_stock_vault_workspace_root()?;
    let node = resolve_node_binary()?;
    let script = workspace.join("scripts/history-forecast-runner.mjs");
    let history_db = history_db_path(app)?;

    let mut request = match payload {
        Value::Object(map) => map,
        _ => serde_json::Map::new(),
    };
    request.insert("action".to_string(), Value::String(action.to_string()));
    request.insert(
        "historyDbPath".to_string(),
        Value::String(history_db.to_string_lossy().to_string()),
    );

    let mut command = Command::new(node);
    command
        .arg("--no-warnings")
        .arg(script)
        .current_dir(workspace)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = command
        .spawn()
        .map_err(|err| format!("啟動 History Forecast Runner 失敗: {err}"))?;

    if let Some(stdin) = child.stdin.as_mut() {
        let input = serde_json::to_vec(&Value::Object(request))
            .map_err(|err| format!("序列化 History Forecast Runner 請求失敗: {err}"))?;
        stdin
            .write_all(&input)
            .map_err(|err| format!("寫入 History Forecast Runner stdin 失敗: {err}"))?;
    }

    let output = child
        .wait_with_output()
        .map_err(|err| format!("執行 History Forecast Runner 失敗: {err}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let detail = if !stderr.is_empty() {
            stderr
        } else if !stdout.is_empty() {
            stdout
        } else {
            format!("exit code {:?}", output.status.code())
        };
        return Err(format!("History Forecast Runner 執行失敗：{detail}"));
    }

    let stdout = String::from_utf8(output.stdout)
        .map_err(|err| format!("History Forecast Runner 輸出不是有效 UTF-8: {err}"))?;
    serde_json::from_str::<Value>(stdout.trim())
        .map_err(|err| format!("解析 History Forecast Runner JSON 輸出失敗: {err}"))
}

fn sqlite_value_ref_to_json(value: ValueRef<'_>) -> Value {
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(number) => json!(number),
        ValueRef::Real(number) => json!(number),
        ValueRef::Text(text) => json!(String::from_utf8_lossy(text).to_string()),
        ValueRef::Blob(bytes) => json!(general_purpose::STANDARD.encode(bytes)),
    }
}

fn history_table_exists(conn: &Connection, name: &str) -> Result<bool, String> {
    conn.query_row(
        "SELECT COUNT(1) FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?1",
        params![name],
        |row| row.get::<_, i64>(0),
    )
    .map(|count| count > 0)
    .map_err(|err| format!("檢查 History DB 物件 {name} 失敗: {err}"))
}

fn history_table_count(conn: &Connection, table_name: &str) -> Result<i64, String> {
    if !history_table_exists(conn, table_name)? {
        return Ok(0);
    }
    let sql = format!("SELECT COUNT(1) FROM {table_name}");
    conn.query_row(&sql, [], |row| row.get::<_, i64>(0))
        .map_err(|err| format!("讀取 {table_name} 筆數失敗: {err}"))
}

fn parse_history_json_text(raw: &str, field_name: &str) -> Result<Value, String> {
    serde_json::from_str(raw).map_err(|err| format!("解析 {field_name} JSON 失敗: {err}"))
}

fn parse_history_optional_json_text(
    raw: Option<String>,
    field_name: &str,
) -> Result<Value, String> {
    match raw {
        Some(text) => parse_history_json_text(&text, field_name),
        None => Ok(Value::Null),
    }
}

#[tauri::command]
fn get_history_db_status(app: AppHandle) -> Result<Value, String> {
    let conn = open_history_conn(&app)?;
    let latest_portfolio_snapshot_date = if history_table_exists(&conn, "portfolio_daily_assets")? {
        conn.query_row(
            "SELECT MAX(snapshot_date) FROM portfolio_daily_assets",
            [],
            |row| row.get::<_, Option<String>>(0),
        )
        .map_err(|err| format!("讀取最新投資組合快照日期失敗: {err}"))?
    } else {
        None
    };

    let mut counts = Vec::new();
    for table_name in [
        "instruments",
        "market_daily_bars",
        "futures_continuous_daily",
        "instrument_sync_state",
        "portfolio_transactions",
        "portfolio_cash_transactions",
        "portfolio_daily_positions",
        "portfolio_daily_assets",
    ] {
        counts.push(json!({
            "tableName": table_name,
            "total": history_table_count(&conn, table_name)?,
        }));
    }

    let recent_runs = if history_table_exists(&conn, "sync_runs")? {
        let mut statement = conn
            .prepare(
                "
                SELECT id, run_type, scope, started_at, finished_at, status
                FROM sync_runs
                ORDER BY id DESC
                LIMIT 10
            ",
            )
            .map_err(|err| format!("查詢最近同步紀錄失敗: {err}"))?;
        let mapped = statement
            .query_map([], |row| {
                Ok(json!({
                    "id": row.get::<_, i64>(0)?,
                    "runType": row.get::<_, String>(1)?,
                    "scope": row.get::<_, String>(2)?,
                    "startedAt": row.get::<_, String>(3)?,
                    "finishedAt": row.get::<_, Option<String>>(4)?,
                    "status": row.get::<_, String>(5)?,
                }))
            })
            .map_err(|err| format!("讀取最近同步紀錄失敗: {err}"))?;
        mapped
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| format!("整理最近同步紀錄失敗: {err}"))?
    } else {
        Vec::new()
    };

    let instrument_sync_state = if history_table_exists(&conn, "instrument_sync_state")? {
        let mut statement = conn
            .prepare(
                "
                SELECT
                  i.instrument_id,
                  i.symbol,
                  i.instrument_type,
                  i.name,
                  i.market,
                  i.currency,
                  i.source,
                  i.finmind_dataset,
                  i.first_trade_date,
                  i.last_trade_date,
                  s.last_status,
                  s.last_success_trade_date,
                  s.last_run_at,
                  s.last_rows_fetched,
                  s.last_rows_upserted,
                  s.last_error_text
                FROM instruments i
                LEFT JOIN instrument_sync_state s
                  ON s.instrument_id = i.instrument_id
                WHERE i.is_active = 1
                ORDER BY i.instrument_type ASC, i.symbol ASC
                LIMIT 20
            ",
            )
            .map_err(|err| format!("查詢 instrument sync state 失敗: {err}"))?;
        let mapped = statement
            .query_map([], |row| {
                Ok(json!({
                    "instrumentId": row.get::<_, String>(0)?,
                    "symbol": row.get::<_, String>(1)?,
                    "instrumentType": row.get::<_, String>(2)?,
                    "name": row.get::<_, String>(3)?,
                    "market": row.get::<_, Option<String>>(4)?,
                    "currency": row.get::<_, String>(5)?,
                    "source": row.get::<_, String>(6)?,
                    "finmindDataset": row.get::<_, Option<String>>(7)?,
                    "firstTradeDate": row.get::<_, Option<String>>(8)?,
                    "lastTradeDate": row.get::<_, Option<String>>(9)?,
                    "lastStatus": row.get::<_, Option<String>>(10)?,
                    "lastSuccessTradeDate": row.get::<_, Option<String>>(11)?,
                    "lastRunAt": row.get::<_, Option<String>>(12)?,
                    "lastRowsFetched": row.get::<_, Option<i64>>(13)?,
                    "lastRowsUpserted": row.get::<_, Option<i64>>(14)?,
                    "lastErrorText": row.get::<_, Option<String>>(15)?,
                }))
            })
            .map_err(|err| format!("讀取 instrument sync state 失敗: {err}"))?;
        mapped
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| format!("整理 instrument sync state 失敗: {err}"))?
    } else {
        Vec::new()
    };

    Ok(json!({
        "historyDbPath": history_db_path(&app)?.to_string_lossy().to_string(),
        "latestPortfolioSnapshotDate": latest_portfolio_snapshot_date,
        "counts": counts,
        "recentRuns": recent_runs,
        "instrumentSyncState": instrument_sync_state,
    }))
}

#[tauri::command]
fn sync_history_db(
    app: AppHandle,
    symbols: Option<Vec<String>>,
    start_date: Option<String>,
    end_date: Option<String>,
    skip_market: Option<bool>,
    skip_portfolio: Option<bool>,
) -> Result<Value, String> {
    let mut args = Vec::new();
    if let Some(symbols) = symbols.filter(|items| !items.is_empty()) {
        args.push("--symbols".to_string());
        args.push(symbols.join(","));
    }
    if let Some(start_date) = start_date.filter(|value| !value.trim().is_empty()) {
        args.push("--start".to_string());
        args.push(start_date);
    }
    if let Some(end_date) = end_date.filter(|value| !value.trim().is_empty()) {
        args.push("--end".to_string());
        args.push(end_date);
    }
    if skip_market.unwrap_or(false) {
        args.push("--skip-market".to_string());
    }
    if skip_portfolio.unwrap_or(false) {
        args.push("--skip-portfolio".to_string());
    }

    let value = run_history_cli_json(&app, "sync", &args)?;
    Ok(camelize_json(value))
}

#[tauri::command]
fn list_history_instruments(
    app: AppHandle,
    instrument_type: Option<String>,
    limit: Option<i64>,
) -> Result<Value, String> {
    let conn = open_history_conn(&app)?;
    let limit = limit.unwrap_or(200).clamp(1, 2000);
    let has_sync_state = history_table_exists(&conn, "instrument_sync_state")?;

    let base_sql = if has_sync_state {
        "
            SELECT
              i.instrument_id,
              i.symbol,
              i.instrument_type,
              i.name,
              i.market,
              i.currency,
              i.source,
              i.finmind_dataset,
              i.first_trade_date,
              i.last_trade_date,
              s.last_status,
              s.last_success_trade_date,
              s.last_run_at,
              s.last_rows_fetched,
              s.last_rows_upserted,
              s.last_error_text
            FROM instruments i
            LEFT JOIN instrument_sync_state s
              ON s.instrument_id = i.instrument_id
            WHERE i.is_active = 1
        "
    } else {
        "
            SELECT
              i.instrument_id,
              i.symbol,
              i.instrument_type,
              i.name,
              i.market,
              i.currency,
              i.source,
              i.finmind_dataset,
              i.first_trade_date,
              i.last_trade_date,
              NULL AS last_status,
              NULL AS last_success_trade_date,
              NULL AS last_run_at,
              NULL AS last_rows_fetched,
              NULL AS last_rows_upserted,
              NULL AS last_error_text
            FROM instruments i
            WHERE i.is_active = 1
        "
    };

    let rows = if let Some(instrument_type) =
        instrument_type.filter(|value| !value.trim().is_empty())
    {
        let sql = format!(
            "{base_sql} AND i.instrument_type = ?1 ORDER BY i.instrument_type ASC, i.symbol ASC LIMIT {limit}"
        );
        let mut statement = conn
            .prepare(&sql)
            .map_err(|err| format!("查詢 instruments 失敗: {err}"))?;
        let mapped = statement
            .query_map(params![instrument_type], |row| {
                Ok(json!({
                    "instrumentId": row.get::<_, String>(0)?,
                    "symbol": row.get::<_, String>(1)?,
                    "instrumentType": row.get::<_, String>(2)?,
                    "name": row.get::<_, String>(3)?,
                    "market": row.get::<_, Option<String>>(4)?,
                    "currency": row.get::<_, String>(5)?,
                    "source": row.get::<_, String>(6)?,
                    "finmindDataset": row.get::<_, Option<String>>(7)?,
                    "firstTradeDate": row.get::<_, Option<String>>(8)?,
                    "lastTradeDate": row.get::<_, Option<String>>(9)?,
                    "lastStatus": row.get::<_, Option<String>>(10)?,
                    "lastSuccessTradeDate": row.get::<_, Option<String>>(11)?,
                    "lastRunAt": row.get::<_, Option<String>>(12)?,
                    "lastRowsFetched": row.get::<_, Option<i64>>(13)?,
                    "lastRowsUpserted": row.get::<_, Option<i64>>(14)?,
                    "lastErrorText": row.get::<_, Option<String>>(15)?,
                }))
            })
            .map_err(|err| format!("讀取 instruments 失敗: {err}"))?;
        mapped
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| format!("整理 instruments 失敗: {err}"))?
    } else {
        let sql = format!("{base_sql} ORDER BY i.instrument_type ASC, i.symbol ASC LIMIT {limit}");
        let mut statement = conn
            .prepare(&sql)
            .map_err(|err| format!("查詢 instruments 失敗: {err}"))?;
        let mapped = statement
            .query_map([], |row| {
                Ok(json!({
                    "instrumentId": row.get::<_, String>(0)?,
                    "symbol": row.get::<_, String>(1)?,
                    "instrumentType": row.get::<_, String>(2)?,
                    "name": row.get::<_, String>(3)?,
                    "market": row.get::<_, Option<String>>(4)?,
                    "currency": row.get::<_, String>(5)?,
                    "source": row.get::<_, String>(6)?,
                    "finmindDataset": row.get::<_, Option<String>>(7)?,
                    "firstTradeDate": row.get::<_, Option<String>>(8)?,
                    "lastTradeDate": row.get::<_, Option<String>>(9)?,
                    "lastStatus": row.get::<_, Option<String>>(10)?,
                    "lastSuccessTradeDate": row.get::<_, Option<String>>(11)?,
                    "lastRunAt": row.get::<_, Option<String>>(12)?,
                    "lastRowsFetched": row.get::<_, Option<i64>>(13)?,
                    "lastRowsUpserted": row.get::<_, Option<i64>>(14)?,
                    "lastErrorText": row.get::<_, Option<String>>(15)?,
                }))
            })
            .map_err(|err| format!("讀取 instruments 失敗: {err}"))?;
        mapped
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| format!("整理 instruments 失敗: {err}"))?
    };

    Ok(Value::Array(rows))
}

#[tauri::command]
fn list_history_timesfm_series(app: AppHandle, limit: Option<i64>) -> Result<Value, String> {
    let conn = open_history_conn(&app)?;
    if !history_table_exists(&conn, "v_timesfm_series")? {
        return Ok(Value::Array(Vec::new()));
    }
    let limit = limit.unwrap_or(20).clamp(1, 500);
    let mut statement = conn
        .prepare(
            "
            SELECT series_id, entity_type, symbol, COUNT(*) AS points, MIN(ds) AS first_ds, MAX(ds) AS last_ds
            FROM v_timesfm_series
            GROUP BY series_id, entity_type, symbol
            ORDER BY entity_type ASC, series_id ASC
            LIMIT ?1
        ",
        )
        .map_err(|err| format!("查詢 TimesFM 序列失敗: {err}"))?;
    let mapped = statement
        .query_map(params![limit], |row| {
            Ok(json!({
                "seriesId": row.get::<_, String>(0)?,
                "entityType": row.get::<_, String>(1)?,
                "symbol": row.get::<_, String>(2)?,
                "points": row.get::<_, i64>(3)?,
                "firstDs": row.get::<_, Option<String>>(4)?,
                "lastDs": row.get::<_, Option<String>>(5)?,
            }))
        })
        .map_err(|err| format!("讀取 TimesFM 序列失敗: {err}"))?;
    let rows = mapped
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| format!("整理 TimesFM 序列失敗: {err}"))?;
    Ok(Value::Array(rows))
}

#[tauri::command]
fn inspect_history_forecast_runtime(
    app: AppHandle,
    python_executable: Option<String>,
    model_repo_id: Option<String>,
    attempt_model_load: Option<bool>,
) -> Result<Value, String> {
    let mut args = Vec::new();
    if let Some(python) = python_executable.filter(|value| !value.trim().is_empty()) {
        args.push("--python".to_string());
        args.push(python);
    }
    if let Some(model) = model_repo_id.filter(|value| !value.trim().is_empty()) {
        args.push("--model".to_string());
        args.push(model);
    }
    if matches!(attempt_model_load, Some(false)) {
        args.push("--skip-model-load".to_string());
    }
    let value = run_history_cli_json(&app, "timesfm:doctor", &args)?;
    Ok(camelize_json(value))
}

#[tauri::command]
fn list_history_forecast_runs(
    app: AppHandle,
    limit: Option<i64>,
    series_id: Option<String>,
) -> Result<Value, String> {
    let conn = open_history_conn(&app)?;
    if !history_table_exists(&conn, "forecast_runs")?
        || !history_table_exists(&conn, "forecast_points")?
    {
        return Ok(Value::Array(Vec::new()));
    }
    let limit = limit.unwrap_or(12).clamp(1, 200);
    let series_filter = series_id
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let mut statement = conn
        .prepare(
            "
            SELECT
              fr.forecast_id,
              fr.created_at,
              fr.series_id,
              fr.entity_type,
              fr.symbol,
              fr.provider_kind,
              fr.provider_name,
              fr.provider_status,
              fr.requested_horizon,
              fr.history_points,
              fr.history_start_ds,
              fr.history_end_ds,
              fr.request_json,
              fr.provider_json,
              fr.warnings_json,
              fr.metadata_json,
              COUNT(fp.step_index) AS point_count,
              SUM(CASE WHEN actual.y IS NOT NULL THEN 1 ELSE 0 END) AS actual_points,
              MIN(fp.ds) AS forecast_start_ds,
              MAX(fp.ds) AS forecast_end_ds,
              AVG(CASE WHEN actual.y IS NOT NULL THEN ABS(fp.yhat - actual.y) END) AS mae,
              AVG(
                CASE
                  WHEN actual.y IS NOT NULL AND ABS(actual.y) > 0
                    THEN ABS((fp.yhat - actual.y) / actual.y) * 100
                END
              ) AS mape,
              MAX(CASE WHEN actual.y IS NOT NULL THEN ABS(fp.yhat - actual.y) END) AS max_absolute_error,
              json_array_length(fr.warnings_json) AS warning_count
            FROM forecast_runs fr
            JOIN forecast_points fp
              ON fp.forecast_id = fr.forecast_id
            LEFT JOIN v_timesfm_series actual
              ON actual.series_id = fr.series_id
             AND actual.ds = fp.ds
            WHERE (?1 IS NULL OR fr.series_id = ?1)
            GROUP BY
              fr.forecast_id,
              fr.created_at,
              fr.series_id,
              fr.entity_type,
              fr.symbol,
              fr.provider_kind,
              fr.provider_name,
              fr.provider_status,
              fr.requested_horizon,
              fr.history_points,
              fr.history_start_ds,
              fr.history_end_ds,
              fr.request_json,
              fr.provider_json,
              fr.warnings_json,
              fr.metadata_json
            ORDER BY fr.created_at DESC
            LIMIT ?2
        ",
        )
        .map_err(|err| format!("查詢 recent forecast runs 失敗: {err}"))?;
    let mut rows = statement
        .query(params![series_filter, limit])
        .map_err(|err| format!("執行 recent forecast runs 查詢失敗: {err}"))?;
    let mut result = Vec::new();

    while let Some(row) = rows
        .next()
        .map_err(|err| format!("讀取 recent forecast runs 失敗: {err}"))?
    {
        let point_count = row.get::<_, i64>(16).unwrap_or(0);
        let actual_points = row.get::<_, i64>(17).unwrap_or(0);
        result.push(json!({
            "forecastId": row.get::<_, String>(0).map_err(|err| format!("讀取 forecastId 失敗: {err}"))?,
            "createdAt": row.get::<_, String>(1).map_err(|err| format!("讀取 createdAt 失敗: {err}"))?,
            "seriesId": row.get::<_, String>(2).map_err(|err| format!("讀取 seriesId 失敗: {err}"))?,
            "entityType": row.get::<_, String>(3).map_err(|err| format!("讀取 entityType 失敗: {err}"))?,
            "symbol": row.get::<_, String>(4).map_err(|err| format!("讀取 symbol 失敗: {err}"))?,
            "providerKind": row.get::<_, String>(5).map_err(|err| format!("讀取 providerKind 失敗: {err}"))?,
            "providerName": row.get::<_, String>(6).map_err(|err| format!("讀取 providerName 失敗: {err}"))?,
            "providerStatus": row.get::<_, String>(7).map_err(|err| format!("讀取 providerStatus 失敗: {err}"))?,
            "requestedHorizon": row.get::<_, i64>(8).map_err(|err| format!("讀取 requestedHorizon 失敗: {err}"))?,
            "historyPoints": row.get::<_, i64>(9).map_err(|err| format!("讀取 historyPoints 失敗: {err}"))?,
            "historyStartDs": row.get::<_, String>(10).map_err(|err| format!("讀取 historyStartDs 失敗: {err}"))?,
            "historyEndDs": row.get::<_, String>(11).map_err(|err| format!("讀取 historyEndDs 失敗: {err}"))?,
            "forecastStartDs": row.get::<_, Option<String>>(18).map_err(|err| format!("讀取 forecastStartDs 失敗: {err}"))?,
            "forecastEndDs": row.get::<_, Option<String>>(19).map_err(|err| format!("讀取 forecastEndDs 失敗: {err}"))?,
            "warningCount": row.get::<_, i64>(23).unwrap_or(0),
            "request": parse_history_json_text(&row.get::<_, String>(12).map_err(|err| format!("讀取 requestJson 失敗: {err}"))?, "forecast_runs.request_json")?,
            "provider": parse_history_json_text(&row.get::<_, String>(13).map_err(|err| format!("讀取 providerJson 失敗: {err}"))?, "forecast_runs.provider_json")?,
            "warnings": parse_history_json_text(&row.get::<_, String>(14).map_err(|err| format!("讀取 warningsJson 失敗: {err}"))?, "forecast_runs.warnings_json")?,
            "metadata": parse_history_optional_json_text(row.get::<_, Option<String>>(15).map_err(|err| format!("讀取 metadataJson 失敗: {err}"))?, "forecast_runs.metadata_json")?,
            "metrics": {
                "pointCount": point_count,
                "actualPoints": actual_points,
                "pendingPoints": std::cmp::max(point_count - actual_points, 0),
                "mae": row.get::<_, Option<f64>>(20).unwrap_or(None),
                "mape": row.get::<_, Option<f64>>(21).unwrap_or(None),
                "maxAbsoluteError": row.get::<_, Option<f64>>(22).unwrap_or(None),
            },
        }));
    }

    Ok(Value::Array(result))
}

#[tauri::command]
fn get_history_forecast_run(app: AppHandle, forecast_id: String) -> Result<Value, String> {
    let conn = open_history_conn(&app)?;
    if !history_table_exists(&conn, "forecast_runs")?
        || !history_table_exists(&conn, "forecast_points")?
    {
        return Ok(Value::Null);
    }

    let mut run_statement = conn
        .prepare(
            "
            SELECT
              forecast_id,
              created_at,
              series_id,
              entity_type,
              symbol,
              provider_kind,
              provider_name,
              provider_status,
              requested_horizon,
              history_points,
              history_start_ds,
              history_end_ds,
              last_observed_y,
              request_json,
              provider_json,
              warnings_json,
              metadata_json
            FROM forecast_runs
            WHERE forecast_id = ?1
            LIMIT 1
        ",
        )
        .map_err(|err| format!("查詢 forecast run 明細失敗: {err}"))?;
    let mut run_rows = run_statement
        .query(params![forecast_id.clone()])
        .map_err(|err| format!("執行 forecast run 明細查詢失敗: {err}"))?;
    let Some(run_row) = run_rows
        .next()
        .map_err(|err| format!("讀取 forecast run 明細失敗: {err}"))?
    else {
        return Ok(Value::Null);
    };

    let series_id = run_row
        .get::<_, String>(2)
        .map_err(|err| format!("讀取明細 seriesId 失敗: {err}"))?;
    let mut point_statement = conn
        .prepare(
            "
            SELECT
              fp.step_index,
              fp.ds,
              fp.yhat,
              fp.lower_bound,
              fp.upper_bound,
              fp.metadata_json,
              actual.y AS actual_y
            FROM forecast_points fp
            LEFT JOIN v_timesfm_series actual
              ON actual.series_id = ?1
             AND actual.ds = fp.ds
            WHERE fp.forecast_id = ?2
            ORDER BY fp.step_index ASC
        ",
        )
        .map_err(|err| format!("查詢 forecast points 失敗: {err}"))?;
    let mut point_rows = point_statement
        .query(params![series_id.clone(), forecast_id])
        .map_err(|err| format!("執行 forecast points 查詢失敗: {err}"))?;
    let mut points = Vec::new();
    let mut point_count = 0_i64;
    let mut actual_points = 0_i64;
    let mut absolute_error_sum = 0.0_f64;
    let mut absolute_percentage_error_sum = 0.0_f64;
    let mut absolute_percentage_error_points = 0_i64;
    let mut max_absolute_error: Option<f64> = None;

    while let Some(point_row) = point_rows
        .next()
        .map_err(|err| format!("讀取 forecast points 失敗: {err}"))?
    {
        point_count += 1;
        let yhat = point_row
            .get::<_, f64>(2)
            .map_err(|err| format!("讀取 yhat 失敗: {err}"))?;
        let actual_y = point_row.get::<_, Option<f64>>(6).unwrap_or(None);
        let absolute_error = actual_y.map(|actual| (yhat - actual).abs());
        let absolute_percentage_error = match (absolute_error, actual_y) {
            (Some(error), Some(actual)) if actual.abs() > f64::EPSILON => {
                Some((error / actual.abs()) * 100.0)
            }
            _ => None,
        };
        if let Some(error) = absolute_error {
            actual_points += 1;
            absolute_error_sum += error;
            max_absolute_error =
                Some(max_absolute_error.map_or(error, |current| current.max(error)));
        }
        if let Some(ape) = absolute_percentage_error {
            absolute_percentage_error_sum += ape;
            absolute_percentage_error_points += 1;
        }

        points.push(json!({
            "stepIndex": point_row.get::<_, i64>(0).map_err(|err| format!("讀取 stepIndex 失敗: {err}"))?,
            "ds": point_row.get::<_, String>(1).map_err(|err| format!("讀取 ds 失敗: {err}"))?,
            "yhat": yhat,
            "lowerBound": point_row.get::<_, Option<f64>>(3).unwrap_or(None),
            "upperBound": point_row.get::<_, Option<f64>>(4).unwrap_or(None),
            "metadata": parse_history_optional_json_text(point_row.get::<_, Option<String>>(5).map_err(|err| format!("讀取 point metadata 失敗: {err}"))?, "forecast_points.metadata_json")?,
            "actualY": actual_y,
            "absoluteError": absolute_error,
            "absolutePercentageError": absolute_percentage_error,
        }));
    }

    let mae = if actual_points > 0 {
        Some(absolute_error_sum / actual_points as f64)
    } else {
        None
    };
    let mape = if absolute_percentage_error_points > 0 {
        Some(absolute_percentage_error_sum / absolute_percentage_error_points as f64)
    } else {
        None
    };

    Ok(json!({
        "forecastId": run_row.get::<_, String>(0).map_err(|err| format!("讀取明細 forecastId 失敗: {err}"))?,
        "createdAt": run_row.get::<_, String>(1).map_err(|err| format!("讀取明細 createdAt 失敗: {err}"))?,
        "seriesId": series_id,
        "entityType": run_row.get::<_, String>(3).map_err(|err| format!("讀取明細 entityType 失敗: {err}"))?,
        "symbol": run_row.get::<_, String>(4).map_err(|err| format!("讀取明細 symbol 失敗: {err}"))?,
        "providerKind": run_row.get::<_, String>(5).map_err(|err| format!("讀取明細 providerKind 失敗: {err}"))?,
        "providerName": run_row.get::<_, String>(6).map_err(|err| format!("讀取明細 providerName 失敗: {err}"))?,
        "providerStatus": run_row.get::<_, String>(7).map_err(|err| format!("讀取明細 providerStatus 失敗: {err}"))?,
        "requestedHorizon": run_row.get::<_, i64>(8).map_err(|err| format!("讀取明細 requestedHorizon 失敗: {err}"))?,
        "historyPoints": run_row.get::<_, i64>(9).map_err(|err| format!("讀取明細 historyPoints 失敗: {err}"))?,
        "historyStartDs": run_row.get::<_, String>(10).map_err(|err| format!("讀取明細 historyStartDs 失敗: {err}"))?,
        "historyEndDs": run_row.get::<_, String>(11).map_err(|err| format!("讀取明細 historyEndDs 失敗: {err}"))?,
        "lastObservedY": run_row.get::<_, Option<f64>>(12).unwrap_or(None),
        "request": parse_history_json_text(&run_row.get::<_, String>(13).map_err(|err| format!("讀取明細 requestJson 失敗: {err}"))?, "forecast_runs.request_json")?,
        "provider": parse_history_json_text(&run_row.get::<_, String>(14).map_err(|err| format!("讀取明細 providerJson 失敗: {err}"))?, "forecast_runs.provider_json")?,
        "warnings": parse_history_json_text(&run_row.get::<_, String>(15).map_err(|err| format!("讀取明細 warningsJson 失敗: {err}"))?, "forecast_runs.warnings_json")?,
        "metadata": parse_history_optional_json_text(run_row.get::<_, Option<String>>(16).map_err(|err| format!("讀取明細 metadataJson 失敗: {err}"))?, "forecast_runs.metadata_json")?,
        "points": points,
        "metrics": {
            "pointCount": point_count,
            "actualPoints": actual_points,
            "pendingPoints": std::cmp::max(point_count - actual_points, 0),
            "mae": mae,
            "mape": mape,
            "maxAbsoluteError": max_absolute_error,
        },
    }))
}

#[tauri::command]
fn run_history_series_forecast(
    app: AppHandle,
    series_id: String,
    horizon: Option<i64>,
    from_date: Option<String>,
    to_date: Option<String>,
    persist: Option<bool>,
    provider_preference: Option<String>,
) -> Result<Value, String> {
    let mut args = vec![
        "--series".to_string(),
        series_id,
        "--horizon".to_string(),
        horizon.unwrap_or(5).clamp(1, 60).to_string(),
    ];
    if let Some(from_date) = from_date.filter(|value| !value.trim().is_empty()) {
        args.push("--from".to_string());
        args.push(from_date);
    }
    if let Some(to_date) = to_date.filter(|value| !value.trim().is_empty()) {
        args.push("--to".to_string());
        args.push(to_date);
    }
    if matches!(persist, Some(false)) {
        args.push("--no-persist".to_string());
    }
    if let Some(provider) = provider_preference.filter(|value| !value.trim().is_empty()) {
        args.push("--provider".to_string());
        args.push(provider);
    }
    let value = run_history_cli_json(&app, "forecast", &args)?;
    Ok(camelize_json(value))
}

#[tauri::command]
fn invoke_custom_forecast(
    app: AppHandle,
    dates: Vec<String>,
    values: Vec<f64>,
    horizon: Option<i64>,
    max_context: Option<i64>,
) -> Result<Value, String> {
    if dates.is_empty() || values.is_empty() {
        return Err("請提供至少一筆自訂序列資料。".to_string());
    }
    if dates.len() != values.len() {
        return Err("自訂序列的 dates 與 values 筆數不一致。".to_string());
    }

    let mut payload = serde_json::Map::new();
    payload.insert("dates".to_string(), json!(dates));
    payload.insert("values".to_string(), json!(values));
    payload.insert(
        "horizon".to_string(),
        json!(horizon.unwrap_or(5).clamp(1, 30)),
    );
    if let Some(max_context) = max_context {
        payload.insert("maxContext".to_string(), json!(max_context.clamp(1, 4096)));
    }

    run_history_forecast_runner_json(&app, "custom_forecast", Value::Object(payload))
}

#[tauri::command]
fn get_history_market_bars(
    app: AppHandle,
    symbol: String,
    instrument_type: Option<String>,
    from_date: Option<String>,
    to_date: Option<String>,
    limit: Option<i64>,
) -> Result<Value, String> {
    let mut args = vec!["--symbol".to_string(), symbol];
    if let Some(instrument_type) = instrument_type.filter(|value| !value.trim().is_empty()) {
        args.push("--instrument-type".to_string());
        args.push(instrument_type);
    }
    if let Some(from_date) = from_date.filter(|value| !value.trim().is_empty()) {
        args.push("--from".to_string());
        args.push(from_date);
    }
    if let Some(to_date) = to_date.filter(|value| !value.trim().is_empty()) {
        args.push("--to".to_string());
        args.push(to_date);
    }
    if let Some(limit) = limit {
        args.push("--limit".to_string());
        args.push(limit.clamp(1, 2000).to_string());
    }
    let value = run_history_cli_json(&app, "candles", &args)?;
    let rows = value
        .get("rows")
        .cloned()
        .unwrap_or(Value::Array(Vec::new()));
    Ok(camelize_json(rows))
}

#[tauri::command]
fn get_history_portfolio_snapshot(
    app: AppHandle,
    date: Option<String>,
    scope: Option<String>,
) -> Result<Value, String> {
    let mut args = Vec::new();
    if let Some(date) = date.filter(|value| !value.trim().is_empty()) {
        args.push("--date".to_string());
        args.push(date);
    }
    if let Some(scope) = scope.filter(|value| !value.trim().is_empty()) {
        args.push("--scope".to_string());
        args.push(scope);
    }
    let value = run_history_cli_json(&app, "portfolio", &args)?;
    Ok(camelize_json(value))
}

#[tauri::command]
fn run_history_readonly_sql(
    app: AppHandle,
    sql: String,
    limit: Option<i64>,
) -> Result<Value, String> {
    let sql = sql.trim();
    let upper = sql.to_ascii_uppercase();
    let allowed =
        upper.starts_with("SELECT") || upper.starts_with("WITH") || upper.starts_with("PRAGMA");
    let forbidden = [
        "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "ATTACH", "DETACH", "VACUUM",
    ];
    if !allowed || forbidden.iter().any(|keyword| upper.contains(keyword)) {
        return Err("只允許唯讀 SQL（SELECT / WITH / PRAGMA）。".to_string());
    }

    let conn = open_history_conn(&app)?;
    let mut statement = conn
        .prepare(sql)
        .map_err(|err| format!("準備唯讀 SQL 失敗: {err}"))?;
    let column_names = statement
        .column_names()
        .iter()
        .map(|name| snake_to_camel_key(name))
        .collect::<Vec<_>>();
    let limit = limit.unwrap_or(200).clamp(1, 2000) as usize;
    let mut rows = statement
        .query([])
        .map_err(|err| format!("執行唯讀 SQL 失敗: {err}"))?;
    let mut result = Vec::new();

    while let Some(row) = rows
        .next()
        .map_err(|err| format!("讀取唯讀 SQL 結果失敗: {err}"))?
    {
        let mut object = serde_json::Map::new();
        for (index, column_name) in column_names.iter().enumerate() {
            let value = row
                .get_ref(index)
                .map(sqlite_value_ref_to_json)
                .map_err(|err| format!("整理唯讀 SQL 欄位失敗: {err}"))?;
            object.insert(column_name.clone(), value);
        }
        result.push(Value::Object(object));
        if result.len() >= limit {
            break;
        }
    }

    Ok(Value::Array(result))
}

#[tauri::command]
fn timesfm_workbench_action(
    app: AppHandle,
    action: String,
    payload_json: Option<String>,
) -> Result<Value, String> {
    let payload = match payload_json.filter(|value| !value.trim().is_empty()) {
        Some(raw) => serde_json::from_str::<Value>(&raw)
            .map_err(|err| format!("解析 TimesFM Workbench payload 失敗: {err}"))?,
        None => Value::Object(serde_json::Map::new()),
    };

    run_timesfm_workbench_cli_json(&app, action.trim(), payload)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(
            tauri_plugin_sql::Builder::default()
                .add_migrations("sqlite:stock-vault.db", sqlite_migrations())
                .build(),
        )
        .setup(|app| {
            if let Err(err) = try_backfill_bootstrap_formal_on_app_startup(&app.handle()) {
                eprintln!("[startup-bootstrap-backfill] failed: {err}");
            }
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            run_seed_if_needed,
            save_transaction,
            delete_transaction_and_rebuild,
            import_transactions_csv,
            import_prices_csv,
            import_tdcc_intermediate,
            backfill_bootstrap_formal_from_intermediate,
            clear_tdcc_intermediate,
            upsert_price,
            backup_database,
            get_settings_backup_status,
            inspect_settings_backup,
            create_settings_backup,
            restore_settings_backup,
            create_full_backup,
            restore_full_backup,
            restore_database,
            write_text_to_path,
            atomic_write_text_to_path,
            read_text_from_path,
            read_file_base64,
            write_base64_to_path,
            get_openclaw_snapshot_dir,
            get_runtime_storage_info,
            get_app_version,
            open_folder_in_finder,
            lookup_tw_stock_master,
            sync_tw_prices_for_holdings,
            sync_tw_price_for_security,
            backfill_auto_dividends_last_12_months,
            repair_tw_stock_securities_master,
            create_account,
            update_account,
            set_account_archived,
            delete_account,
            delete_holding_transactions,
            upsert_cash_transaction,
            delete_cash_transaction,
            get_history_db_status,
            sync_history_db,
            list_history_instruments,
            list_history_timesfm_series,
            inspect_history_forecast_runtime,
            list_history_forecast_runs,
            get_history_forecast_run,
            run_history_series_forecast,
            invoke_custom_forecast,
            get_history_market_bars,
            get_history_portfolio_snapshot,
            run_history_readonly_sql,
            timesfm_workbench_action,
            export_db_base64,
            restore_db_base64
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn setup_conn() -> Connection {
        let conn = Connection::open_in_memory().expect("open memory db");
        conn.execute_batch(
            "
            PRAGMA foreign_keys = ON;
            CREATE TABLE transactions (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              account_id INTEGER NOT NULL,
              security_id INTEGER,
              type TEXT NOT NULL,
              trade_date TEXT NOT NULL,
              quantity REAL NOT NULL DEFAULT 0,
              price REAL NOT NULL DEFAULT 0,
              fee REAL NOT NULL DEFAULT 0,
              tax REAL NOT NULL DEFAULT 0,
              interest REAL NOT NULL DEFAULT 0,
              amount REAL NOT NULL DEFAULT 0,
              note TEXT,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE sell_allocations (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              sell_transaction_id INTEGER NOT NULL,
              buy_transaction_id INTEGER NOT NULL,
              quantity REAL NOT NULL
            );
        ",
        )
        .expect("create schema");
        conn
    }

    fn setup_conn_with_cash() -> Connection {
        let conn = Connection::open_in_memory().expect("open memory db");
        conn.execute_batch(
            "
            PRAGMA foreign_keys = ON;
            CREATE TABLE accounts (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              name TEXT NOT NULL,
              broker TEXT,
              currency TEXT NOT NULL
            );
            CREATE TABLE transactions (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              account_id INTEGER NOT NULL,
              security_id INTEGER,
              type TEXT NOT NULL,
              trade_date TEXT NOT NULL,
              quantity REAL NOT NULL DEFAULT 0,
              price REAL NOT NULL DEFAULT 0,
              fee REAL NOT NULL DEFAULT 0,
              tax REAL NOT NULL DEFAULT 0,
              interest REAL NOT NULL DEFAULT 0,
              amount REAL NOT NULL DEFAULT 0,
              note TEXT,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE cash_transactions (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              account_id INTEGER NOT NULL,
              trade_date TEXT NOT NULL,
              type TEXT NOT NULL,
              amount REAL NOT NULL,
              currency TEXT NOT NULL,
              note TEXT,
              related_transaction_id INTEGER,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              FOREIGN KEY (account_id) REFERENCES accounts(id),
              FOREIGN KEY (related_transaction_id) REFERENCES transactions(id)
            );
            CREATE UNIQUE INDEX idx_cash_transactions_related_type_unique
              ON cash_transactions(related_transaction_id, type);
        ",
        )
        .expect("create cash schema");
        conn.execute(
            "INSERT INTO accounts (id, name, broker, currency) VALUES (1, '測試帳戶', '測試券商', 'TWD')",
            [],
        )
        .expect("seed account");
        conn
    }

    fn setup_conn_for_auto_dividend() -> Connection {
        let conn = Connection::open_in_memory().expect("open memory db");
        conn.execute_batch(
            "
            PRAGMA foreign_keys = ON;
            CREATE TABLE accounts (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              name TEXT NOT NULL,
              broker TEXT,
              currency TEXT NOT NULL DEFAULT 'TWD'
            );
            CREATE TABLE securities (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              symbol TEXT NOT NULL UNIQUE,
              name TEXT NOT NULL,
              market TEXT NOT NULL,
              currency TEXT NOT NULL
            );
            CREATE TABLE transactions (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              account_id INTEGER NOT NULL,
              security_id INTEGER,
              type TEXT NOT NULL,
              trade_date TEXT NOT NULL,
              quantity REAL NOT NULL DEFAULT 0,
              price REAL NOT NULL DEFAULT 0,
              fee REAL NOT NULL DEFAULT 0,
              tax REAL NOT NULL DEFAULT 0,
              interest REAL NOT NULL DEFAULT 0,
              amount REAL NOT NULL DEFAULT 0,
              note TEXT,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE auto_dividend_events (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              account_id INTEGER NOT NULL,
              security_id INTEGER NOT NULL,
              dividend_type TEXT NOT NULL,
              ex_dividend_date TEXT NOT NULL,
              record_date TEXT,
              payment_date TEXT,
              cash_dividend_per_share REAL,
              total_cash_dividend REAL,
              stock_dividend_per_thousand REAL,
              total_stock_dividend REAL,
              holding_quantity_at_ex_date REAL NOT NULL,
              source TEXT NOT NULL DEFAULT 'auto-fetched',
              fetched_at TEXT NOT NULL,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              UNIQUE(account_id, security_id, dividend_type, ex_dividend_date, source)
            );
        ",
        )
        .expect("create auto dividend schema");

        conn.execute(
            "INSERT INTO accounts (id, name, broker, currency) VALUES (1, '長期帳戶', '永豐', 'TWD')",
            [],
        )
        .expect("seed account");
        conn.execute(
            "INSERT INTO securities (id, symbol, name, market, currency) VALUES (1, '2330', '台積電', 'TWSE', 'TWD')",
            [],
        )
        .expect("seed security");
        conn
    }

    fn setup_conn_for_tdcc_import() -> Connection {
        let conn = Connection::open_in_memory().expect("open memory db");
        conn.execute_batch(
            "
            PRAGMA foreign_keys = ON;
            CREATE TABLE accounts (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              name TEXT NOT NULL,
              broker TEXT,
              currency TEXT NOT NULL DEFAULT 'TWD',
              bank_name TEXT,
              bank_branch TEXT,
              bank_account_last4 TEXT,
              settlement_account_note TEXT,
              is_primary INTEGER NOT NULL DEFAULT 0,
              note TEXT,
              is_archived INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE securities (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              symbol TEXT NOT NULL UNIQUE,
              name TEXT NOT NULL,
              market TEXT NOT NULL,
              currency TEXT NOT NULL,
              board_lot_size INTEGER NOT NULL DEFAULT 1000,
              category TEXT,
              tags TEXT
            );
            CREATE TABLE transactions (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              account_id INTEGER NOT NULL,
              security_id INTEGER,
              type TEXT NOT NULL,
              trade_date TEXT NOT NULL,
              quantity REAL NOT NULL DEFAULT 0,
              price REAL NOT NULL DEFAULT 0,
              fee REAL NOT NULL DEFAULT 0,
              tax REAL NOT NULL DEFAULT 0,
              interest REAL NOT NULL DEFAULT 0,
              amount REAL NOT NULL DEFAULT 0,
              note TEXT,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE sell_allocations (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              sell_transaction_id INTEGER NOT NULL,
              buy_transaction_id INTEGER NOT NULL,
              quantity REAL NOT NULL
            );
            CREATE TABLE cash_transactions (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              account_id INTEGER NOT NULL,
              trade_date TEXT NOT NULL,
              type TEXT NOT NULL,
              amount REAL NOT NULL,
              currency TEXT NOT NULL,
              note TEXT,
              related_transaction_id INTEGER,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              UNIQUE(related_transaction_id, type)
            );
            CREATE TABLE price_snapshots (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              security_id INTEGER NOT NULL,
              price_date TEXT NOT NULL,
              close_price REAL NOT NULL,
              currency TEXT NOT NULL,
              source TEXT NOT NULL,
              UNIQUE(security_id, price_date)
            );
            CREATE TABLE tdcc_account_mappings (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              account_id INTEGER NOT NULL,
              broker_name TEXT NOT NULL,
              account_name TEXT NOT NULL,
              account_no TEXT NOT NULL,
              source TEXT NOT NULL,
              source_note TEXT,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              UNIQUE(account_no, source)
            );
            CREATE TABLE tdcc_import_runs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              run_key TEXT NOT NULL UNIQUE,
              summary_json TEXT,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE tdcc_holdings_seed (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              run_key TEXT NOT NULL,
              account_id INTEGER NOT NULL,
              security_id INTEGER NOT NULL,
              quantity REAL NOT NULL,
              snapshot_date TEXT NOT NULL,
              close_price REAL,
              market_value REAL,
              source TEXT NOT NULL,
              source_pdf TEXT,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE tdcc_transaction_drafts (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              run_key TEXT NOT NULL,
              account_id INTEGER NOT NULL,
              security_id INTEGER NOT NULL,
              trade_date TEXT NOT NULL,
              side TEXT NOT NULL,
              quantity REAL NOT NULL,
              running_balance_from_pdf REAL,
              source_memo TEXT,
              source_pdf TEXT,
              page_no INTEGER,
              source TEXT NOT NULL,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE tdcc_skipped_events (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              run_key TEXT NOT NULL,
              account_id INTEGER,
              broker_name TEXT,
              account_name TEXT,
              account_no TEXT,
              trade_date TEXT,
              symbol TEXT,
              name TEXT,
              memo TEXT,
              withdrawal_qty REAL,
              deposit_qty REAL,
              running_balance_from_pdf REAL,
              skip_reason TEXT NOT NULL,
              source_pdf TEXT,
              page_no INTEGER,
              source TEXT NOT NULL,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE tdcc_imported_formal_transactions (
              transaction_id INTEGER PRIMARY KEY,
              run_key TEXT NOT NULL,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE tdcc_imported_formal_price_snapshots (
              price_snapshot_id INTEGER PRIMARY KEY,
              run_key TEXT NOT NULL,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        ",
        )
        .expect("create tdcc schema");
        conn
    }

    fn seed_scope(conn: &Connection) {
        conn.execute(
            "INSERT INTO transactions (id, account_id, security_id, type, trade_date, quantity, price, fee, tax, interest, amount, note)
             VALUES (1, 1, 10, 'BUY', '2026-01-02', 100, 10, 0, 0, 0, 0, '')",
            [],
        )
        .expect("seed buy 1");
        conn.execute(
            "INSERT INTO transactions (id, account_id, security_id, type, trade_date, quantity, price, fee, tax, interest, amount, note)
             VALUES (2, 1, 10, 'BUY', '2026-01-03', 100, 12, 0, 0, 0, 0, '')",
            [],
        )
        .expect("seed buy 2");
        conn.execute(
            "INSERT INTO transactions (id, account_id, security_id, type, trade_date, quantity, price, fee, tax, interest, amount, note)
             VALUES (3, 1, 10, 'SELL', '2026-01-04', 50, 13, 0, 0, 0, 0, '')",
            [],
        )
        .expect("seed sell");
        conn.execute(
            "INSERT INTO sell_allocations (sell_transaction_id, buy_transaction_id, quantity)
             VALUES (3, 1, 50)",
            [],
        )
        .expect("seed allocation");
    }

    #[test]
    fn manual_allocations_validation_success() {
        let mut conn = setup_conn();
        seed_scope(&conn);
        let tx = conn.transaction().expect("tx");
        let lots = build_buy_lot_state_map_tx(&tx, 1, 10, None).expect("lots");
        let allocations = vec![ManualAllocationPayload {
            buy_transaction_id: 1,
            quantity: 20.0,
        }];
        let result = validate_manual_allocations(&lots, 1, 10, 20.0, &allocations);
        assert!(result.is_ok());
    }

    #[test]
    fn manual_allocations_total_must_equal_sell_quantity() {
        let mut conn = setup_conn();
        seed_scope(&conn);
        let tx = conn.transaction().expect("tx");
        let lots = build_buy_lot_state_map_tx(&tx, 1, 10, None).expect("lots");
        let allocations = vec![ManualAllocationPayload {
            buy_transaction_id: 1,
            quantity: 20.0,
        }];
        let result = validate_manual_allocations(&lots, 1, 10, 30.0, &allocations);
        assert!(result.is_err());
        assert!(result
            .err()
            .unwrap_or_default()
            .contains("必須等於賣出股數"));
    }

    #[test]
    fn manual_allocations_cannot_exceed_lot_remaining() {
        let mut conn = setup_conn();
        seed_scope(&conn);
        let tx = conn.transaction().expect("tx");
        let lots = build_buy_lot_state_map_tx(&tx, 1, 10, None).expect("lots");
        let allocations = vec![ManualAllocationPayload {
            buy_transaction_id: 1,
            quantity: 60.0,
        }];
        let result = validate_manual_allocations(&lots, 1, 10, 60.0, &allocations);
        assert!(result.is_err());
        assert!(result.err().unwrap_or_default().contains("超過可用 lot"));
    }

    #[test]
    fn manual_allocations_persist_matches_expected_rows() {
        let mut conn = setup_conn();
        seed_scope(&conn);
        let tx = conn.transaction().expect("tx");
        tx.execute(
            "INSERT INTO transactions (id, account_id, security_id, type, trade_date, quantity, price, fee, tax, interest, amount, note)
             VALUES (4, 1, 10, 'SELL', '2026-01-05', 80, 14, 0, 0, 0, 0, '')",
            [],
        )
        .expect("seed target sell");
        tx.execute(
            "DELETE FROM sell_allocations WHERE sell_transaction_id = 4",
            [],
        )
        .expect("clear target allocations");
        let allocations = vec![
            ManualAllocationPayload {
                buy_transaction_id: 1,
                quantity: 30.0,
            },
            ManualAllocationPayload {
                buy_transaction_id: 2,
                quantity: 50.0,
            },
        ];
        write_manual_allocations_tx(&tx, 4, &allocations).expect("write manual allocations");

        let rows = tx
            .prepare(
                "SELECT buy_transaction_id, quantity
                 FROM sell_allocations
                 WHERE sell_transaction_id = 4
                 ORDER BY buy_transaction_id",
            )
            .expect("prepare verify")
            .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, f64>(1)?)))
            .expect("query verify")
            .map(|row| row.expect("row"))
            .collect::<Vec<_>>();
        assert_eq!(rows, vec![(1, 30.0), (2, 50.0)]);
    }

    #[test]
    fn fifo_path_still_works_without_manual_allocations() {
        let rows = vec![
            TxRow {
                id: 1,
                tx_type: "BUY".to_string(),
                quantity: 100.0,
            },
            TxRow {
                id: 2,
                tx_type: "SELL".to_string(),
                quantity: 60.0,
            },
        ];
        let allocations = build_allocations(&rows).expect("fifo allocations");
        assert_eq!(allocations.len(), 1);
        assert_eq!(allocations[0], (2, 1, 60.0));
    }

    #[test]
    fn cash_dividend_cash_flow_sync_insert_update_delete() {
        let mut conn = setup_conn_with_cash();
        let tx = conn.transaction().expect("tx");
        tx.execute(
            "INSERT INTO transactions (id, account_id, security_id, type, trade_date, amount) VALUES (9, 1, 1, 'CASH_DIVIDEND', '2026-03-20', 1200)",
            [],
        )
        .expect("seed cash dividend tx");

        sync_cash_flow_for_transaction_tx(
            &tx,
            9,
            1,
            "CASH_DIVIDEND",
            "2026-03-20",
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            1200.0,
        )
        .expect("sync insert");
        let inserted: (f64, String) = tx
            .query_row(
                "SELECT amount, type FROM cash_transactions WHERE related_transaction_id = 9",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("inserted row");
        assert_eq!(inserted.0, 1200.0);
        assert_eq!(inserted.1, "CASH_DIVIDEND_IN");

        sync_cash_flow_for_transaction_tx(
            &tx,
            9,
            1,
            "CASH_DIVIDEND",
            "2026-03-21",
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            1500.0,
        )
        .expect("sync update");
        let count: i64 = tx
            .query_row(
                "SELECT COUNT(1) FROM cash_transactions WHERE related_transaction_id = 9",
                [],
                |row| row.get(0),
            )
            .expect("count row");
        assert_eq!(count, 1);
        let updated_amount: f64 = tx
            .query_row(
                "SELECT amount FROM cash_transactions WHERE related_transaction_id = 9",
                [],
                |row| row.get(0),
            )
            .expect("updated amount");
        assert_eq!(updated_amount, 1500.0);

        sync_cash_flow_for_transaction_tx(
            &tx,
            9,
            1,
            "BUY",
            "2026-03-22",
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
        )
        .expect("sync delete");
        let remaining: i64 = tx
            .query_row(
                "SELECT COUNT(1) FROM cash_transactions WHERE related_transaction_id = 9",
                [],
                |row| row.get(0),
            )
            .expect("remaining count");
        assert_eq!(remaining, 0);
    }

    #[test]
    fn buy_cash_flow_mapping_contains_settlement_and_cost_rows() {
        let rows = build_cash_flow_mappings_for_transaction("BUY", 100.0, 10.0, 3.0, 2.0, 1.0, 0.0);
        assert_eq!(rows.len(), 4);
        let settlement = rows
            .iter()
            .find(|row| row.cash_type == "BUY_SETTLEMENT")
            .expect("buy settlement row");
        assert_eq!(settlement.amount, -1006.0);
        let fee = rows
            .iter()
            .find(|row| row.cash_type == "FEE_OUT")
            .expect("fee row");
        assert_eq!(fee.amount, -3.0);
        let tax = rows
            .iter()
            .find(|row| row.cash_type == "TAX_OUT")
            .expect("tax row");
        assert_eq!(tax.amount, -2.0);
    }

    #[test]
    fn buy_cash_flow_sync_insert_update_delete() {
        let mut conn = setup_conn_with_cash();
        let tx = conn.transaction().expect("tx");
        tx.execute(
            "INSERT INTO transactions (id, account_id, security_id, type, trade_date, quantity, price, fee, tax, interest, amount)
             VALUES (11, 1, 1, 'BUY', '2026-03-20', 100, 10, 3, 2, 1, 1000)",
            [],
        )
        .expect("seed buy tx");

        sync_cash_flow_for_transaction_tx(
            &tx,
            11,
            1,
            "BUY",
            "2026-03-20",
            100.0,
            10.0,
            3.0,
            2.0,
            1.0,
            1000.0,
        )
        .expect("sync buy insert");
        let count_inserted: i64 = tx
            .query_row(
                "SELECT COUNT(1) FROM cash_transactions WHERE related_transaction_id = 11",
                [],
                |row| row.get(0),
            )
            .expect("count inserted rows");
        assert_eq!(count_inserted, 4);

        sync_cash_flow_for_transaction_tx(
            &tx,
            11,
            1,
            "BUY",
            "2026-03-21",
            120.0,
            11.0,
            4.0,
            3.0,
            2.0,
            1320.0,
        )
        .expect("sync buy update");
        let settlement_after_update: f64 = tx
            .query_row(
                "SELECT amount FROM cash_transactions WHERE related_transaction_id = 11 AND type = 'BUY_SETTLEMENT'",
                [],
                |row| row.get(0),
            )
            .expect("settlement after update");
        assert_eq!(settlement_after_update, -1329.0);

        sync_cash_flow_for_transaction_tx(
            &tx,
            11,
            1,
            "STOCK_DIVIDEND",
            "2026-03-22",
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
        )
        .expect("sync buy delete");
        let count_after_delete: i64 = tx
            .query_row(
                "SELECT COUNT(1) FROM cash_transactions WHERE related_transaction_id = 11",
                [],
                |row| row.get(0),
            )
            .expect("count after delete");
        assert_eq!(count_after_delete, 0);
    }

    #[test]
    fn tdcc_intermediate_import_writes_formal_transactions_and_rebuilds_allocations() {
        let mut conn = setup_conn_for_tdcc_import();
        let result = import_tdcc_intermediate_tx(
            &mut conn,
            TdccIntermediateImportPayload {
                accounts: vec![TdccAccountSeedPayload {
                    account_name: "長期投資帳戶".to_string(),
                    broker_name: "永豐金證券".to_string(),
                    account_no: "987654".to_string(),
                    source: "TDCC_PDF_ACCOUNT_SEED".to_string(),
                    source_note: Some("seed".to_string()),
                }],
                holdings: vec![TdccHoldingSeedPayload {
                    account_no: "987654".to_string(),
                    broker_name: "永豐金證券".to_string(),
                    account_name: "長期投資帳戶".to_string(),
                    symbol: "2330".to_string(),
                    name: "台積電".to_string(),
                    quantity: 1000.0,
                    snapshot_date: "2026-03-21".to_string(),
                    close_price: Some(938.0),
                    market_value: Some(938000.0),
                    source: "TDCC_PDF_HOLDINGS_SEED".to_string(),
                    source_pdf: Some("a.pdf".to_string()),
                }],
                drafts: vec![TdccTransactionDraftPayload {
                    account_no: "987654".to_string(),
                    broker_name: "永豐金證券".to_string(),
                    account_name: "長期投資帳戶".to_string(),
                    trade_date: "2026-03-20".to_string(),
                    symbol: "2330".to_string(),
                    name: "台積電".to_string(),
                    side: "BUY".to_string(),
                    quantity: 1000.0,
                    running_balance_from_pdf: Some(1000.0),
                    source_memo: Some("測試".to_string()),
                    source_pdf: Some("a.pdf".to_string()),
                    page_no: Some(1),
                    source: "TDCC_PDF_TX_DRAFT".to_string(),
                }],
                skipped_events: vec![TdccSkippedEventPayload {
                    account_no: "987654".to_string(),
                    broker_name: "永豐金證券".to_string(),
                    account_name: "長期投資帳戶".to_string(),
                    trade_date: Some("2026-03-19".to_string()),
                    symbol: Some("2330".to_string()),
                    name: Some("台積電".to_string()),
                    memo: Some("配股".to_string()),
                    withdrawal_qty: Some(0.0),
                    deposit_qty: Some(100.0),
                    running_balance_from_pdf: Some(1000.0),
                    skip_reason: "SPECIAL_EVENT".to_string(),
                    source_pdf: Some("a.pdf".to_string()),
                    page_no: Some(2),
                    source: "TDCC_PDF_SKIPPED".to_string(),
                }],
                import_summary_json: Some("{\"accounts_count\":1}".to_string()),
            },
        )
        .expect("tdcc import success");

        assert_eq!(result.holdings_seeded, 1);
        assert_eq!(result.draft_imported, 1);
        assert_eq!(result.skipped_logged, 1);
        assert_eq!(result.accounts_seeded + result.accounts_reused, 1);

        let account_count: i64 = conn
            .query_row("SELECT COUNT(1) FROM accounts", [], |row| row.get(0))
            .expect("account count");
        assert_eq!(account_count, 1);

        let mapping_count: i64 = conn
            .query_row("SELECT COUNT(1) FROM tdcc_account_mappings", [], |row| {
                row.get(0)
            })
            .expect("mapping count");
        assert_eq!(mapping_count, 1);

        let tx_count: i64 = conn
            .query_row("SELECT COUNT(1) FROM transactions", [], |row| row.get(0))
            .expect("formal tx count");
        assert!(tx_count >= 1);

        let draft_count: i64 = conn
            .query_row("SELECT COUNT(1) FROM tdcc_transaction_drafts", [], |row| {
                row.get(0)
            })
            .expect("draft count");
        assert_eq!(draft_count, 1);

        let skipped_count: i64 = conn
            .query_row("SELECT COUNT(1) FROM tdcc_skipped_events", [], |row| {
                row.get(0)
            })
            .expect("skipped count");
        assert_eq!(skipped_count, 1);

        let holdings_count: i64 = conn
            .query_row("SELECT COUNT(1) FROM tdcc_holdings_seed", [], |row| {
                row.get(0)
            })
            .expect("holdings count");
        assert_eq!(holdings_count, 1);

        let tracked_tx_count: i64 = conn
            .query_row(
                "SELECT COUNT(1) FROM tdcc_imported_formal_transactions",
                [],
                |row| row.get(0),
            )
            .expect("tracked tx count");
        assert_eq!(tracked_tx_count, tx_count);
    }

    #[test]
    fn tdcc_clear_wipes_imported_formal_rows_but_keeps_manual_rows() {
        let mut conn = setup_conn_for_tdcc_import();
        import_tdcc_intermediate_tx(
            &mut conn,
            TdccIntermediateImportPayload {
                accounts: vec![TdccAccountSeedPayload {
                    account_name: "長期投資帳戶".to_string(),
                    broker_name: "永豐金證券".to_string(),
                    account_no: "987654".to_string(),
                    source: "TDCC_PDF_ACCOUNT_SEED".to_string(),
                    source_note: Some("seed".to_string()),
                }],
                holdings: vec![TdccHoldingSeedPayload {
                    account_no: "987654".to_string(),
                    broker_name: "永豐金證券".to_string(),
                    account_name: "長期投資帳戶".to_string(),
                    symbol: "2330".to_string(),
                    name: "台積電".to_string(),
                    quantity: 1000.0,
                    snapshot_date: "2026-03-21".to_string(),
                    close_price: Some(938.0),
                    market_value: Some(938000.0),
                    source: "TDCC_PDF_HOLDINGS_SEED".to_string(),
                    source_pdf: Some("a.pdf".to_string()),
                }],
                drafts: vec![TdccTransactionDraftPayload {
                    account_no: "987654".to_string(),
                    broker_name: "永豐金證券".to_string(),
                    account_name: "長期投資帳戶".to_string(),
                    trade_date: "2026-03-20".to_string(),
                    symbol: "2330".to_string(),
                    name: "台積電".to_string(),
                    side: "BUY".to_string(),
                    quantity: 1000.0,
                    running_balance_from_pdf: Some(1000.0),
                    source_memo: Some("測試".to_string()),
                    source_pdf: Some("a.pdf".to_string()),
                    page_no: Some(1),
                    source: "TDCC_PDF_TX_DRAFT".to_string(),
                }],
                skipped_events: vec![TdccSkippedEventPayload {
                    account_no: "987654".to_string(),
                    broker_name: "永豐金證券".to_string(),
                    account_name: "長期投資帳戶".to_string(),
                    trade_date: Some("2026-03-19".to_string()),
                    symbol: Some("2330".to_string()),
                    name: Some("台積電".to_string()),
                    memo: Some("配股".to_string()),
                    withdrawal_qty: Some(0.0),
                    deposit_qty: Some(100.0),
                    running_balance_from_pdf: Some(1000.0),
                    skip_reason: "SPECIAL_EVENT".to_string(),
                    source_pdf: Some("a.pdf".to_string()),
                    page_no: Some(2),
                    source: "TDCC_PDF_SKIPPED".to_string(),
                }],
                import_summary_json: Some("{\"accounts_count\":1}".to_string()),
            },
        )
        .expect("tdcc import success");

        conn.execute(
            "
            INSERT INTO transactions
              (account_id, security_id, type, trade_date, quantity, price, fee, tax, interest, amount, note)
            VALUES
              (1, 1, 'BUY', '2026-03-01', 100, 10, 0, 0, 0, 1000, 'manual')
        ",
            [],
        )
        .expect("insert manual tx");

        let tx = conn.transaction().expect("clear tx");
        clear_bootstrap_formal_rows_tx(&tx).expect("clear bootstrap formal rows");
        tx.execute("DELETE FROM tdcc_holdings_seed", [])
            .expect("clear holdings");
        tx.execute("DELETE FROM tdcc_transaction_drafts", [])
            .expect("clear drafts");
        tx.execute("DELETE FROM tdcc_skipped_events", [])
            .expect("clear skipped");
        tx.execute("DELETE FROM tdcc_import_runs", [])
            .expect("clear runs");
        tx.execute("DELETE FROM tdcc_account_mappings", [])
            .expect("clear mappings");
        tx.commit().expect("commit clear");

        let hold_count: i64 = conn
            .query_row("SELECT COUNT(1) FROM tdcc_holdings_seed", [], |row| {
                row.get(0)
            })
            .expect("hold count");
        let draft_count: i64 = conn
            .query_row("SELECT COUNT(1) FROM tdcc_transaction_drafts", [], |row| {
                row.get(0)
            })
            .expect("draft count");
        let skip_count: i64 = conn
            .query_row("SELECT COUNT(1) FROM tdcc_skipped_events", [], |row| {
                row.get(0)
            })
            .expect("skip count");
        let map_count: i64 = conn
            .query_row("SELECT COUNT(1) FROM tdcc_account_mappings", [], |row| {
                row.get(0)
            })
            .expect("mapping count");
        let tx_count: i64 = conn
            .query_row("SELECT COUNT(1) FROM transactions", [], |row| row.get(0))
            .expect("transaction count");
        assert_eq!(hold_count, 0);
        assert_eq!(draft_count, 0);
        assert_eq!(skip_count, 0);
        assert_eq!(map_count, 0);
        assert_eq!(tx_count, 1);
    }

    #[test]
    fn tdcc_backfill_promotes_existing_intermediate_rows_to_formal() {
        let mut conn = setup_conn_for_tdcc_import();
        import_tdcc_intermediate_tx(
            &mut conn,
            TdccIntermediateImportPayload {
                accounts: vec![TdccAccountSeedPayload {
                    account_name: "長期投資帳戶".to_string(),
                    broker_name: "永豐金證券".to_string(),
                    account_no: "987654".to_string(),
                    source: "TDCC_PDF_ACCOUNT_SEED".to_string(),
                    source_note: Some("seed".to_string()),
                }],
                holdings: vec![TdccHoldingSeedPayload {
                    account_no: "987654".to_string(),
                    broker_name: "永豐金證券".to_string(),
                    account_name: "長期投資帳戶".to_string(),
                    symbol: "2330".to_string(),
                    name: "台積電".to_string(),
                    quantity: 1000.0,
                    snapshot_date: "2026-03-21".to_string(),
                    close_price: Some(938.0),
                    market_value: Some(938000.0),
                    source: "TDCC_PDF_HOLDINGS_SEED".to_string(),
                    source_pdf: Some("a.pdf".to_string()),
                }],
                drafts: vec![TdccTransactionDraftPayload {
                    account_no: "987654".to_string(),
                    broker_name: "永豐金證券".to_string(),
                    account_name: "長期投資帳戶".to_string(),
                    trade_date: "2026-03-20".to_string(),
                    symbol: "2330".to_string(),
                    name: "台積電".to_string(),
                    side: "BUY".to_string(),
                    quantity: 1000.0,
                    running_balance_from_pdf: Some(1000.0),
                    source_memo: Some("測試".to_string()),
                    source_pdf: Some("a.pdf".to_string()),
                    page_no: Some(1),
                    source: "TDCC_PDF_TX_DRAFT".to_string(),
                }],
                skipped_events: vec![],
                import_summary_json: Some("{\"accounts_count\":1}".to_string()),
            },
        )
        .expect("tdcc import success");

        let tx = conn.transaction().expect("clear tx");
        clear_bootstrap_formal_rows_tx(&tx).expect("clear bootstrap formal rows");
        tx.commit().expect("commit clear");

        let before_count: i64 = conn
            .query_row("SELECT COUNT(1) FROM transactions", [], |row| row.get(0))
            .expect("before count");
        assert_eq!(before_count, 0);

        let result = backfill_bootstrap_formal_from_intermediate_conn(&mut conn)
            .expect("backfill should succeed");
        assert!(result.executed);

        let after_count: i64 = conn
            .query_row("SELECT COUNT(1) FROM transactions", [], |row| row.get(0))
            .expect("after count");
        assert!(after_count > 0);
    }

    #[test]
    fn tdcc_backfill_skips_when_no_intermediate_seed() {
        let mut conn = setup_conn_for_tdcc_import();
        let result = backfill_bootstrap_formal_from_intermediate_conn(&mut conn)
            .expect("backfill should return status");
        assert!(!result.executed);
        assert_eq!(result.reason, "no_bootstrap_seed");
    }

    #[test]
    fn auto_dividend_cash_formula_is_correct() {
        let mut conn = setup_conn_for_auto_dividend();
        conn.execute(
            "
            INSERT INTO transactions
              (account_id, security_id, type, trade_date, quantity, price, fee, tax, interest, amount, note)
            VALUES
              (1, 1, 'BUY', '2026-03-01', 1000, 900, 0, 0, 0, 900000, 'seed')
        ",
            [],
        )
        .expect("seed buy tx");

        let targets = vec![TwPriceSyncTarget {
            security_id: 1,
            symbol: "2330".to_string(),
            name: "台積電".to_string(),
            market: "TWSE".to_string(),
            currency: "TWD".to_string(),
        }];
        let events = vec![TwDividendMarketEvent {
            symbol: "2330".to_string(),
            ex_dividend_date: "2026-03-20".to_string(),
            cash_dividend_per_share: Some(1.5),
            stock_dividend_per_thousand: None,
            record_date: None,
            payment_date: None,
        }];

        let (rows, stats) = build_auto_dividend_upsert_rows(
            &conn,
            &targets,
            &events,
            "2026-03-23T10:00:00+08:00",
            "auto-fetched",
        )
        .expect("build auto dividend rows");

        assert_eq!(stats.matched_events, 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].total_cash_dividend, Some(1500.0));
        assert_eq!(rows[0].holding_quantity_at_ex_date, 1000.0);

        let tx = conn.transaction().expect("open tx");
        upsert_auto_dividend_events_tx(&tx, &rows).expect("upsert auto dividend rows");
        tx.commit().expect("commit tx");

        let stored_total: f64 = conn
            .query_row(
                "SELECT total_cash_dividend FROM auto_dividend_events WHERE account_id = 1 AND security_id = 1",
                [],
                |row| row.get(0),
            )
            .expect("read stored auto dividend");
        assert_eq!(stored_total, 1500.0);
    }

    #[test]
    fn auto_dividend_does_not_override_manual_transaction() {
        let conn = setup_conn_for_auto_dividend();
        conn.execute(
            "
            INSERT INTO transactions
              (account_id, security_id, type, trade_date, quantity, price, fee, tax, interest, amount, note)
            VALUES
              (1, 1, 'BUY', '2026-03-01', 1000, 900, 0, 0, 0, 900000, 'seed')
        ",
            [],
        )
        .expect("seed buy tx");
        conn.execute(
            "
            INSERT INTO transactions
              (account_id, security_id, type, trade_date, quantity, price, fee, tax, interest, amount, note)
            VALUES
              (1, 1, 'CASH_DIVIDEND', '2026-03-20', 0, 0, 0, 0, 0, 1500, 'manual')
        ",
            [],
        )
        .expect("seed manual cash dividend");

        let targets = vec![TwPriceSyncTarget {
            security_id: 1,
            symbol: "2330".to_string(),
            name: "台積電".to_string(),
            market: "TWSE".to_string(),
            currency: "TWD".to_string(),
        }];
        let events = vec![TwDividendMarketEvent {
            symbol: "2330".to_string(),
            ex_dividend_date: "2026-03-20".to_string(),
            cash_dividend_per_share: Some(1.5),
            stock_dividend_per_thousand: None,
            record_date: None,
            payment_date: None,
        }];

        let (rows, stats) = build_auto_dividend_upsert_rows(
            &conn,
            &targets,
            &events,
            "2026-03-23T10:00:00+08:00",
            "auto-fetched",
        )
        .expect("build auto dividend rows");

        assert_eq!(rows.len(), 0);
        assert_eq!(stats.skipped_manual_rows, 1);
    }

    #[test]
    fn auto_dividend_skips_existing_auto_rows_for_backfill_source() {
        let conn = setup_conn_for_auto_dividend();
        conn.execute(
            "
            INSERT INTO transactions
              (account_id, security_id, type, trade_date, quantity, price, fee, tax, interest, amount, note)
            VALUES
              (1, 1, 'BUY', '2026-03-01', 1000, 900, 0, 0, 0, 900000, 'seed')
        ",
            [],
        )
        .expect("seed buy tx");
        conn.execute(
            "
            INSERT INTO auto_dividend_events
              (account_id, security_id, dividend_type, ex_dividend_date, cash_dividend_per_share, total_cash_dividend, holding_quantity_at_ex_date, source, fetched_at)
            VALUES
              (1, 1, 'CASH', '2026-03-20', 1.5, 1500, 1000, 'auto-fetched', '2026-03-21T00:00:00+08:00')
        ",
            [],
        )
        .expect("seed existing auto row");

        let targets = vec![TwPriceSyncTarget {
            security_id: 1,
            symbol: "2330".to_string(),
            name: "台積電".to_string(),
            market: "TWSE".to_string(),
            currency: "TWD".to_string(),
        }];
        let events = vec![TwDividendMarketEvent {
            symbol: "2330".to_string(),
            ex_dividend_date: "2026-03-20".to_string(),
            cash_dividend_per_share: Some(1.5),
            stock_dividend_per_thousand: None,
            record_date: None,
            payment_date: None,
        }];

        let (rows, stats) = build_auto_dividend_upsert_rows(
            &conn,
            &targets,
            &events,
            "2026-03-23T10:00:00+08:00",
            "auto-fetched-backfill",
        )
        .expect("build backfill rows");

        assert!(rows.is_empty());
        assert_eq!(stats.skipped_existing_rows, 1);
    }

    #[test]
    fn auto_dividend_backfill_source_is_persisted() {
        let mut conn = setup_conn_for_auto_dividend();
        conn.execute(
            "
            INSERT INTO transactions
              (account_id, security_id, type, trade_date, quantity, price, fee, tax, interest, amount, note)
            VALUES
              (1, 1, 'BUY', '2026-03-01', 1000, 900, 0, 0, 0, 900000, 'seed')
        ",
            [],
        )
        .expect("seed buy tx");
        let targets = vec![TwPriceSyncTarget {
            security_id: 1,
            symbol: "2330".to_string(),
            name: "台積電".to_string(),
            market: "TWSE".to_string(),
            currency: "TWD".to_string(),
        }];
        let events = vec![TwDividendMarketEvent {
            symbol: "2330".to_string(),
            ex_dividend_date: "2026-03-21".to_string(),
            cash_dividend_per_share: Some(2.0),
            stock_dividend_per_thousand: None,
            record_date: None,
            payment_date: None,
        }];
        let (rows, _) = build_auto_dividend_upsert_rows(
            &conn,
            &targets,
            &events,
            "2026-03-23T10:00:00+08:00",
            "auto-fetched-backfill",
        )
        .expect("build rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].source, "auto-fetched-backfill");

        let tx = conn.transaction().expect("open tx");
        upsert_auto_dividend_events_tx(&tx, &rows).expect("upsert rows");
        tx.commit().expect("commit tx");
        let stored_source: String = conn
            .query_row(
                "SELECT source FROM auto_dividend_events WHERE account_id = 1 AND security_id = 1 AND ex_dividend_date = '2026-03-21'",
                [],
                |row| row.get(0),
            )
            .expect("read stored source");
        assert_eq!(stored_source, "auto-fetched-backfill");
    }

    #[test]
    fn retry_with_backoff_retries_until_success() {
        let mut attempts = 0usize;
        let result = retry_with_backoff(3, Duration::from_millis(0), |_| {
            attempts += 1;
            if attempts < 3 {
                Err("temporary error".to_string())
            } else {
                Ok(42)
            }
        })
        .expect("retry success");

        assert_eq!(result, 42);
        assert_eq!(attempts, 3);
    }

    #[test]
    fn compute_tpex_fail_streak_counts_consecutive_days() {
        let (day1_streak, day1_date) = compute_tpex_fail_streak(None, 0, "2026-03-21", true);
        assert_eq!(day1_streak, 1);
        assert_eq!(day1_date, Some("2026-03-21".to_string()));

        let (day2_streak, day2_date) =
            compute_tpex_fail_streak(day1_date, day1_streak, "2026-03-22", true);
        assert_eq!(day2_streak, 2);
        assert_eq!(day2_date, Some("2026-03-22".to_string()));

        let (day3_streak, _) = compute_tpex_fail_streak(
            Some("2026-03-22".to_string()),
            day2_streak,
            "2026-03-23",
            true,
        );
        assert_eq!(day3_streak, 3);

        let (reset_streak, reset_date) = compute_tpex_fail_streak(
            Some("2026-03-23".to_string()),
            day3_streak,
            "2026-03-24",
            false,
        );
        assert_eq!(reset_streak, 0);
        assert_eq!(reset_date, None);
    }

    #[test]
    fn compute_dividend_sync_start_date_extends_to_90d_lookback() {
        let start = compute_dividend_sync_start_date("2026-03-10", "2026-03-23", 90)
            .expect("start date should compute");
        assert_eq!(start, "2026-03-10");

        let start_from_old_trade = compute_dividend_sync_start_date("2025-10-01", "2026-03-23", 90)
            .expect("start date should cap to lookback window");
        assert_eq!(start_from_old_trade, "2025-12-23");
    }

    #[test]
    fn parse_twse_etf_dividend_events_supports_roc_text_dates() {
        let sample = r#"{
          "stat": "OK",
          "fields": [
            "證券代號",
            "證券名稱",
            "除息交易日",
            "收益分配基準日",
            "收益分配發放日",
            "每受益權益單位現金股利(元)"
          ],
          "data": [
            ["00878", "國泰永續高股息", "115年02月26日", "115年03月03日", "115年03月23日", "0.42"]
          ]
        }"#;

        let events =
            parse_twse_etf_dividend_events(sample, "00878").expect("ETF events should parse");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].symbol, "00878");
        assert_eq!(events[0].ex_dividend_date, "2026-02-26");
        assert_eq!(events[0].record_date.as_deref(), Some("2026-03-03"));
        assert_eq!(events[0].payment_date.as_deref(), Some("2026-03-23"));
        assert_eq!(events[0].cash_dividend_per_share, Some(0.42));
    }

    #[test]
    fn parse_twse_etf_dividend_events_supports_twse_live_field_name() {
        let sample = r#"{
          "status": "ok",
          "fields": [
            "證券代號",
            "證券簡稱",
            "除息交易日",
            "收益分配基準日",
            "收益分配發放日",
            "收益分配金額 (每1受益權益單位)"
          ],
          "data": [
            ["00878", "國泰永續高股息", "115年02月26日", "115年03月03日", "115年03月23日", "0.42"]
          ]
        }"#;

        let events = parse_twse_etf_dividend_events(sample, "00878")
            .expect("ETF events should parse with live field name");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].symbol, "00878");
        assert_eq!(events[0].ex_dividend_date, "2026-02-26");
        assert_eq!(events[0].cash_dividend_per_share, Some(0.42));
    }

    #[test]
    fn parse_twse_dividend_events_supports_history_rows() {
        let sample = r#"{
          "stat":"OK",
          "fields":["除權除息日期","股票代號","名稱","權息","盈餘配股率","公積配股率","現增配股率","現金股利(元)"],
          "data":[
            ["115/03/17","2330","台積電","息","","","","6.0"]
          ]
        }"#;
        let events = parse_twse_dividend_events(sample).expect("parse twse rows");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].symbol, "2330");
        assert_eq!(events[0].ex_dividend_date, "2026-03-17");
        assert_eq!(events[0].cash_dividend_per_share, Some(6.0));
    }

    #[test]
    fn tw_price_sync_date_and_change_parse() {
        assert_eq!(roc_date_to_iso("1150320"), Some("2026-03-20".to_string()));
        assert_eq!(roc_date_to_iso("20260320"), Some("2026-03-20".to_string()));
        assert_eq!(parse_change("+7.00"), Some(7.0));
        assert_eq!(parse_change("-0.23"), Some(-0.23));
        assert_eq!(parse_change("除權息"), None);
    }

    #[test]
    fn tw_price_sync_previous_trading_date_skips_weekend() {
        assert_eq!(
            previous_trading_date("2026-03-23"),
            Some("2026-03-20".to_string())
        );
    }

    #[test]
    fn parse_tw_quote_map_from_stock_day_all_row() {
        let sample = r#"[
          {
            "Date":"1150320",
            "Code":"2330",
            "Name":"台積電",
            "ClosingPrice":"938.00",
            "Change":"+6.00"
          }
        ]"#;
        let map = parse_tw_quote_map(sample, "Code", "ClosingPrice", "Change", "Date")
            .expect("parse quote map");
        let quote = map.get("2330").expect("2330 exists");
        assert_eq!(quote.trade_date, "2026-03-20");
        assert_eq!(quote.close_price, 938.0);
        assert_eq!(quote.previous_close_price, Some(932.0));
    }

    #[test]
    fn tw_benchmark_taiex_parse_and_formula() {
        let sample = r#"{
          "date":"1150320",
          "stat":"OK",
          "tables":[
            {
              "fields":["指數","收盤指數","漲跌(+/-)","漲跌點數","漲跌百分比(%)","特殊處理註記"],
              "data":[
                ["發行量加權股價指數","33,543.88","<p style ='color:green'>-</p>","145.80","-0.43",""]
              ]
            }
          ]
        }"#;
        let parsed = parse_taiex_from_mi_index(sample)
            .expect("parse taiex")
            .expect("taiex should exist");
        assert_eq!(parsed.trade_date, "2026-03-20");
        assert_eq!(parsed.close, 33543.88);
        assert_eq!(parsed.change_points, Some(-145.80));
        assert_eq!(parsed.change_percent_ratio, Some(-0.0043));

        let previous_close = parsed.close - parsed.change_points.unwrap_or_default();
        let pct = (parsed.close - previous_close) / previous_close;
        assert!(pct < 0.0);
    }

    #[test]
    fn tw_benchmark_taiex_parse_from_mis() {
        let sample = r#"{
          "msgArray":[
            {
              "c":"t00",
              "n":"發行量加權股價指數",
              "d":"1150323",
              "z":"32722.50",
              "y":"33543.88"
            }
          ]
        }"#;
        let parsed = parse_taiex_from_mis(sample)
            .expect("parse mis taiex")
            .expect("taiex should exist");
        assert_eq!(parsed.trade_date, "2026-03-23");
        assert_eq!(parsed.close, 32722.50);
        let change_points = parsed.change_points.expect("change points");
        assert!((change_points - (-821.38)).abs() < 0.001);
        assert_eq!(
            parsed
                .change_percent_ratio
                .map(|value| (value * 100000.0).round()),
            Some(-2449.0)
        );
    }

    #[test]
    fn twse_quote_merge_prefers_newer_mis_quote() {
        let openapi_quote = TwMarketQuote {
            trade_date: "2026-04-02".to_string(),
            close_price: 1810.0,
            previous_close_price: Some(1855.0),
        };
        let mis_quote = TwMarketQuote {
            trade_date: "2026-04-07".to_string(),
            close_price: 1860.0,
            previous_close_price: Some(1810.0),
        };

        assert!(should_replace_tw_quote(&openapi_quote, &mis_quote));
        assert!(!should_replace_tw_quote(&mis_quote, &openapi_quote));
    }

    #[test]
    fn twse_quote_merge_prefers_quote_with_previous_close_on_same_day() {
        let current = TwMarketQuote {
            trade_date: "2026-04-07".to_string(),
            close_price: 1860.0,
            previous_close_price: None,
        };
        let candidate = TwMarketQuote {
            trade_date: "2026-04-07".to_string(),
            close_price: 1860.0,
            previous_close_price: Some(1810.0),
        };

        assert!(should_replace_tw_quote(&current, &candidate));
        assert!(!should_replace_tw_quote(&candidate, &current));
    }

    #[test]
    fn portable_mode_skips_macos_app_bundle() {
        let exe = PathBuf::from("/Applications/股市亨通.app/Contents/MacOS/stock-vault");
        assert!(is_inside_app_bundle(&exe));
        assert_eq!(portable_mode_for_exe_path(&exe, false), None);
    }

    #[test]
    fn portable_mode_uses_exe_directory_when_data_marker_exists() {
        let temp = std::env::temp_dir().join("stock-vault-portable-mode-test");
        let _ = fs::remove_dir_all(&temp);
        fs::create_dir_all(temp.join("data")).expect("create data dir");
        let exe = temp.join("stock-vault.exe");

        let portable_root = portable_mode_for_exe_path(&exe, false).expect("portable root");
        assert_eq!(portable_root, temp);

        let _ = fs::remove_dir_all(&temp);
    }

    #[test]
    fn portable_mode_can_be_forced_without_markers() {
        let exe = PathBuf::from("/tmp/stock-vault-portable/stock-vault.exe");
        let portable_root = portable_mode_for_exe_path(&exe, true).expect("portable root");
        assert_eq!(portable_root, PathBuf::from("/tmp/stock-vault-portable"));
    }

    #[test]
    fn ensure_sqlite_schema_bootstraps_fresh_database() {
        let temp_root = std::env::temp_dir().join(format!(
            "stock-vault-schema-test-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        fs::create_dir_all(&temp_root).expect("create temp root");
        let db_path = temp_root.join("stock-vault.db");
        let conn = Connection::open(&db_path).expect("open temp db");

        ensure_sqlite_schema(&conn).expect("apply migrations");

        let table_exists: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'app_settings'",
                [],
                |row| row.get(0),
            )
            .expect("query app_settings existence");
        let applied_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM _sqlx_migrations", [], |row| row.get(0))
            .expect("query migration count");

        assert_eq!(table_exists, 1);
        assert_eq!(applied_count, sqlite_migrations().len() as i64);

        drop(conn);
        let _ = fs::remove_dir_all(&temp_root);
    }

    #[test]
    #[ignore = "live network check for Taiwan stock master endpoints"]
    fn live_lookup_samples() {
        for symbol in ["2330", "0056", "00878", "4551"] {
            let result = lookup_tw_stock_master(symbol.to_string()).expect("lookup result");
            assert!(
                result.ok,
                "lookup should succeed for {symbol}, reason={:?}",
                result.reason_message
            );
            let record = result.record.expect("record should exist");
            assert_eq!(record.symbol, symbol);
            assert!(
                !record.name.trim().is_empty(),
                "name should not be empty for {symbol}"
            );
            assert_eq!(record.currency, "TWD");
            assert!(record.board_lot_size > 0);
        }
    }
}
