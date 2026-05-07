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
BATCH_ROWS = 500          # 一度に書き込む行数
MAX_RETRIES = 3           # API失敗時のリトライ回数
RETRY_WAIT_SEC = 10       # リトライまでの待機秒数


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
            time.sleep(RETRY_WAIT_SEC * attempt)  # 指数バックオフ風


def upload_in_batches(worksheet, processed_data):
    """データをバッチ分割して書き込む"""
    if not processed_data:
        return

    num_cols = max(len(row) for row in processed_data)
    last_col = col_num_to_letter(num_cols)

    # ヘッダーは1行目に書く
    header = processed_data[0]
    safe_update(worksheet, [header], f"A1:{last_col}1")

    # データ行はBATCH_ROWS行ごとに分割書き込み
    data_rows = processed_data[1:]
    total = len(data_rows)
    print(f"  データ行数: {total} 行 / 列数: {num_cols} / バッチサイズ: {BATCH_ROWS}")

    for i in range(0, total, BATCH_ROWS):
        chunk = data_rows[i:i + BATCH_ROWS]
        start_row = i + 2  # 1始まり、ヘッダーの次から
        end_row = start_row + len(chunk) - 1
        range_name = f"A{start_row}:{last_col}{end_row}"
        print(f"  書き込み中: {range_name} ({len(chunk)}行)")
        safe_update(worksheet, chunk, range_name)
        time.sleep(1)  # APIレート制限緩和


def process_and_upload(csv_path, sheet_name, is_cv_data=False):
    """CSVを読み込んで数値変換し、指定のシートにアップロードする共通関数"""
    print(f"[{datetime.now()}] {sheet_name} への転記を開始します")

    # CSV読み込み
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
        print(f"警告: {csv_path} にデータがありません")
        return

    # --- 数値変換処理 ---
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

    # 成果一覧（CVデータ）の場合のみGCLID抽出処理を実行
    if is_cv_data:
        if len(processed_data[0]) > 12:
            processed_data[0].insert(13, "GCLID")
            for row in processed_data[1:]:
                if len(row) > 12:
                    row.insert(13, extract_gclid(str(row[12])))
                else:
                    row.append("")

    # Google Sheets 認証と書き込み
    creds_json = os.environ.get('GOOGLE_CREDENTIALS')
    spreadsheet_id = os.environ.get('SPREADSHEET_ID')

    if not creds_json or not spreadsheet_id:
        raise Exception("Google Sheets関連の環境変数が設定されていません")

    creds_dict = json.loads(creds_json)
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    gc = gspread.authorize(credentials)

    spreadsheet = gc.open_by_key(spreadsheet_id)
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
    except:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=10000, cols=30)

    worksheet.clear()

    # バッチ書き込み（500エラー対策）
    upload_in_batches(worksheet, processed_data)
    print(f"[{datetime.now()}] {sheet_name} 完了")


def main():
    email = os.environ.get('PRESCO_EMAIL')
    password = os.environ.get('PRESCO_PASSWORD')

    if not email or not password:
        raise Exception("PRESCOのログイン情報が環境変数に設定されていません")

    date_from, date_to = get_target_date_range()
    print(f"[{datetime.now()}] 取得期間: {date_from} 〜 {date_to}")

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
            process_and_upload(cv_csv_path, CV_SHEET_NAME, is_cv_data=True)
            process_and_upload(log_csv_path, LOG_SHEET_NAME, is_cv_data=False)

        finally:
            browser.close()


if __name__ == "__main__":
    main()
