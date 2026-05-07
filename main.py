import os
import time
import csv
import re
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from playwright.sync_api import sync_playwright
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --- 設定 ---
LOG_SHEET_NAME = 'LogSummary'
CV_SHEET_NAME = 'PrescoCV'
CAMPAIGN_START_DATE = datetime(2025, 11, 27, tzinfo=ZoneInfo("Asia/Tokyo"))
LOOKBACK_MONTHS = 6

# 差分更新設定（PrescoCVのみ適用）
OVERWRITE_DAYS = 3        # 直近何日分を上書きするか
DATE_COL_INDEX = 0        # 日付が入っている列（0始まり）。A列=0
FULL_REFRESH = False      # Trueにすると全件再取得モードに切り替わる

# サイト名フィルタ設定（PrescoCVのみ適用）
FILTER_ENABLED = True                # True にするとフィルタ有効化
FILTER_KEYWORDS = ["介護", "看護"]    # サイト名に含まれる文字列（部分一致）
SITE_NAME_COL_INDEX = 5               # F列=サイト名（0始まり）

BATCH_ROWS = 500
MAX_RETRIES = 3
RETRY_WAIT_SEC = 10


def get_data_start_date():
    """取得開始日：配信開始日と半年前のうち、新しい方を返す"""
    JST = ZoneInfo("Asia/Tokyo")
    today = datetime.now(JST)

    year, month = today.year, today.month - LOOKBACK_MONTHS
    if month <= 0:
        month += 12
        year -= 1
    try:
        six_months_ago = today.replace(year=year, month=month)
    except ValueError:
        six_months_ago = today.replace(year=year, month=month, day=28)

    return max(CAMPAIGN_START_DATE, six_months_ago)


def get_target_date_range():
    JST = ZoneInfo("Asia/Tokyo")
    start = get_data_start_date()
    today = datetime.now(JST)
    return start.strftime("%Y/%m/%d"), today.strftime("%Y/%m/%d")


def get_report_url():
    JST = ZoneInfo("Asia/Tokyo")
    start = get_data_start_date()
    today = datetime.now(JST)

    date_from = start.strftime("%Y/%m/%d").replace("/", "%2F")
    date_to = today.strftime("%Y/%m/%d").replace("/", "%2F")

    return (
        "https://presco.ai/partner/report/search?"
        f"searchDateTimeFrom={date_from}&"
        f"searchDateTimeTo={date_to}&"
        "searchItemType=2&searchPeriodType=4&searchProgramId=&"
        "searchDateType=3&searchPartnerSiteId=&searchProgramUrlId=&"
        "searchPartnerSitePageId=&searchLargeGenreId=&searchMediumGenreId=&"
        "searchSmallGenreId=&_searchJoinType=on"
    )


def extract_gclid(url):
    if not url:
        return ""
    match = re.search(r'gclid=([^&]+)', url)
    return match.group(1) if match else ""


def col_num_to_letter(n):
    """1始まりの列番号をA,B,...,Z,AA,...に変換"""
    result = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        result = chr(65 + r) + result
    return result


def parse_date_cell(cell_value):
    """セルの値から日付部分(YYYY-MM-DD)を抽出。失敗時はNone"""
    if not cell_value:
        return None
    s = str(cell_value).strip()
    s_normalized = s.replace('/', '-').replace('.', '-')
    match = re.match(r'(\d{4})-(\d{1,2})-(\d{1,2})', s_normalized)
    if not match:
        return None
    try:
        y, m, d = int(match.group(1)), int(match.group(2)), int(match.group(3))
        return datetime(y, m, d, tzinfo=ZoneInfo("Asia/Tokyo"))
    except ValueError:
        return None


def matches_site_filter(row):
    """サイト名フィルタにマッチするか判定（部分一致）"""
    if not FILTER_ENABLED:
        return True
    if len(row) <= SITE_NAME_COL_INDEX:
        return False
    site_name = str(row[SITE_NAME_COL_INDEX])
    return any(keyword in site_name for keyword in FILTER_KEYWORDS)


def safe_update(worksheet, values, range_name, value_input_option='USER_ENTERED'):
    """リトライ付きでupdateする"""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            worksheet.update(values=values, range_name=range_name,
                             value_input_option=value_input_option)
            return
        except gspread.exceptions.APIError as e:
            print(f"  ⚠ APIエラー (試行 {attempt}/{MAX_RETRIES}): {e}")
            if attempt == MAX_RETRIES:
                raise
            time.sleep(RETRY_WAIT_SEC * attempt)


def upload_in_batches(worksheet, processed_data):
    """データをバッチ分割して書き込む"""
    if not processed_data:
        return

    num_cols = max(len(row) for row in processed_data)
    last_col = col_num_to_letter(num_cols)

    header = processed_data[0]
    safe_update(worksheet, [header], f"A1:{last_col}1")

    data_rows = processed_data[1:]
    total = len(data_rows)
    print(f"  データ行数: {total} 行 / 列数: {num_cols} / バッチサイズ: {BATCH_ROWS}")

    if total == 0:
        return

    for i in range(0, total, BATCH_ROWS):
        chunk = data_rows[i:i + BATCH_ROWS]
        start_row = i + 2
        end_row = start_row + len(chunk) - 1
        range_name = f"A{start_row}:{last_col}{end_row}"
        print(f"  書き込み中: {range_name} ({len(chunk)}行)")
        safe_update(worksheet, chunk, range_name)
        time.sleep(1)


def merge_with_existing(new_data, existing_data, overwrite_days, apply_site_filter=False):
    """
    既存データと新規データをマージする（PrescoCV用）。
    - 直近N日分は新規データで置き換え
    - それ以前は既存データを保持
    - apply_site_filter=Trueの場合、既存データもフィルタにかける
    """
    if not new_data:
        return existing_data
    if not existing_data or len(existing_data) <= 1:
        print(f"  既存データ空 → 新規データ全件を投入します")
        if apply_site_filter:
            header = new_data[0]
            filtered = [row for row in new_data[1:] if matches_site_filter(row)]
            print(f"  サイト名フィルタ適用: {len(new_data) - 1}行 → {len(filtered)}行")
            return [header] + filtered
        return new_data

    JST = ZoneInfo("Asia/Tokyo")
    today = datetime.now(JST).replace(hour=0, minute=0, second=0, microsecond=0)
    cutoff_date = today - timedelta(days=overwrite_days - 1)
    print(f"  上書き境界日: {cutoff_date.strftime('%Y-%m-%d')} 以降を新規データで置換")

    header = new_data[0]

    kept_old_rows = []
    discarded_old_rows = 0
    unparsable_rows = 0
    filtered_out_old = 0
    for row in existing_data[1:]:
        if len(row) <= DATE_COL_INDEX:
            unparsable_rows += 1
            kept_old_rows.append(row)
            continue
        row_date = parse_date_cell(row[DATE_COL_INDEX])
        if row_date is None:
            unparsable_rows += 1
            kept_old_rows.append(row)
            continue
        if row_date < cutoff_date:
            if apply_site_filter and not matches_site_filter(row):
                filtered_out_old += 1
                continue
            kept_old_rows.append(row)
        else:
            discarded_old_rows += 1

    new_recent_rows = []
    filtered_out_new = 0
    for row in new_data[1:]:
        if len(row) <= DATE_COL_INDEX:
            continue
        row_date = parse_date_cell(row[DATE_COL_INDEX])
        if row_date is None:
            continue
        if row_date >= cutoff_date:
            if apply_site_filter and not matches_site_filter(row):
                filtered_out_new += 1
                continue
            new_recent_rows.append(row)

    print(f"  既存維持: {len(kept_old_rows)}行 / 既存破棄: {discarded_old_rows}行 / "
          f"日付解析不能: {unparsable_rows}行 / 新規追加: {len(new_recent_rows)}行")
    if apply_site_filter:
        print(f"  サイト名フィルタで除外: 既存 {filtered_out_old}行 / 新規 {filtered_out_new}行")

    return [header] + kept_old_rows + new_recent_rows


def process_csv_to_data(csv_path, is_cv_data=False):
    """CSVを読み込んで数値変換とGCLID抽出を行い、二次元リストを返す"""
    raw_data = []
    encodings = ['utf-8-sig', 'utf-8', 'shift_jis', 'cp932']
    for enc in encodings:
        try:
            with open(csv_path, 'r', encoding=enc) as f:
                reader = csv.reader(f)
                raw_data = list(reader)
                break
        except:
            continue

    if not raw_data:
        return []

    processed_data = []
    for i, row in enumerate(raw_data):
        if i == 0:
            processed_data.append(row)
            continue
        new_row = []
        for cell in row:
            clean_val = cell.replace(',', '')
            try:
                if clean_val == "":
                    new_row.append("")
                elif '.' in clean_val:
                    new_row.append(float(clean_val))
                else:
                    new_row.append(int(clean_val))
            except ValueError:
                new_row.append(cell)
        processed_data.append(new_row)

    # GCLID列の追加（CVデータのみ）
    if is_cv_data:
        if len(processed_data[0]) > 12:
            processed_data[0].insert(13, "GCLID")
            for row in processed_data[1:]:
                if len(row) > 12:
                    row.insert(13, extract_gclid(str(row[12])))
                else:
                    row.append("")

    return processed_data


def get_worksheet(sheet_name):
    """Google Sheets認証してワークシートを取得"""
    creds_json = os.environ.get('GOOGLE_CREDENTIALS')
    spreadsheet_id = os.environ.get('SPREADSHEET_ID')

    if not creds_json or not spreadsheet_id:
        raise Exception("Google Sheets関連の環境変数が設定されていません")

    creds_dict = json.loads(creds_json)
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    gc = gspread.authorize(credentials)

    spreadsheet = gc.open_by_key(spreadsheet_id)
    sheet_exists = True
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
    except:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=10000, cols=30)
        sheet_exists = False
    return worksheet, sheet_exists


def upload_cv_data(csv_path, sheet_name):
    """PrescoCV用：差分更新＋サイト名フィルタ"""
    print(f"[{datetime.now()}] {sheet_name} への転記を開始します（差分更新モード）")

    new_data = process_csv_to_data(csv_path, is_cv_data=True)
    if not new_data:
        print(f"警告: {csv_path} にデータがありません")
        return

    apply_site_filter = FILTER_ENABLED
    if apply_site_filter:
        print(f"  サイト名フィルタ有効: {FILTER_KEYWORDS} のいずれかを含む行のみ書き込み")

    worksheet, sheet_exists = get_worksheet(sheet_name)

    if FULL_REFRESH or not sheet_exists:
        print(f"  {'全件再取得モード' if FULL_REFRESH else 'シート新規作成のため全件投入'}")
        if apply_site_filter:
            header = new_data[0]
            filtered = [row for row in new_data[1:] if matches_site_filter(row)]
            print(f"  サイト名フィルタ適用: {len(new_data) - 1}行 → {len(filtered)}行")
            final_data = [header] + filtered
        else:
            final_data = new_data
    else:
        existing_data = worksheet.get_all_values()
        final_data = merge_with_existing(new_data, existing_data, OVERWRITE_DAYS,
                                         apply_site_filter=apply_site_filter)

    worksheet.clear()
    upload_in_batches(worksheet, final_data)
    print(f"[{datetime.now()}] {sheet_name} 完了")


def upload_log_data(csv_path, sheet_name):
    """LogSummary用：シンプル全置換"""
    print(f"[{datetime.now()}] {sheet_name} への転記を開始します（全置換モード）")

    new_data = process_csv_to_data(csv_path, is_cv_data=False)
    if not new_data:
        print(f"警告: {csv_path} にデータがありません")
        return

    worksheet, _ = get_worksheet(sheet_name)
    worksheet.clear()
    upload_in_batches(worksheet, new_data)
    print(f"[{datetime.now()}] {sheet_name} 完了")


def main():
    email = os.environ.get('PRESCO_EMAIL')
    password = os.environ.get('PRESCO_PASSWORD')

    if not email or not password:
        raise Exception("PRESCOのログイン情報が環境変数に設定されていません")

    date_from, date_to = get_target_date_range()
    print(f"[{datetime.now()}] 取得期間: {date_from} 〜 {date_to}")
    print(f"[{datetime.now()}] PrescoCV: {'全件再取得' if FULL_REFRESH else f'直近{OVERWRITE_DAYS}日のみ上書き'}")
    print(f"[{datetime.now()}] LogSummary: 全置換モード")
    print(f"[{datetime.now()}] サイト名フィルタ: "
          f"{'有効 (' + ', '.join(FILTER_KEYWORDS) + ')' if FILTER_ENABLED else '無効'}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox'])
        context = browser.new_context(viewport={'width': 1920, 'height': 1080})
        page = context.new_page()

        try:
            # 1. ログイン
            print(f"[{datetime.now()}] ログイン中...")
            page.goto('https://presco.ai/partner/')
            page.fill('input[name="username"]', email)
            page.fill('input[name="password"]', password)
            page.click('input[type="submit"][value="ログイン"]')
            page.wait_for_selector('text=ログアウト', timeout=20000)

            # --- 2. 成果一覧CSVの処理 ---
            print(f"[{datetime.now()}] 成果一覧を取得します")
            page.goto('https://presco.ai/partner/actionLog/list')
            page.fill('#dateTimeFrom', date_from)
            page.fill('#dateTimeTo', date_to)
            page.click('span:has-text("検索条件で絞り込む")')
            time.sleep(3)
            with page.expect_download() as download_info:
                page.click('#csv-link')
            cv_csv_path = '/tmp/cv_data.csv'
            download_info.value.save_as(cv_csv_path)

            # --- 3. ログ集計CSVの処理 ---
            print(f"[{datetime.now()}] ログ集計を取得します")
            report_url = get_report_url()
            page.goto(report_url)
            page.wait_for_selector('#report-link')
            with page.expect_download() as download_info:
                page.click('#report-link')
            log_csv_path = '/tmp/log_data.csv'
            download_info.value.save_as(log_csv_path)

            # --- 4. スプレッドシートへ転記 ---
            upload_cv_data(cv_csv_path, CV_SHEET_NAME)       # 差分更新
            upload_log_data(log_csv_path, LOG_SHEET_NAME)    # 全置換

        finally:
            browser.close()


if __name__ == "__main__":
    main()
