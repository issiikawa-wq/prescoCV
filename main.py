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

def get_target_date_range():
    """成果一覧用：先月1日から今日までの日付"""
    JST = ZoneInfo("Asia/Tokyo")
    today = datetime.now(JST)
    first_day_of_this_month = today.replace(day=1)
    last_day_of_last_month = first_day_of_this_month - timedelta(days=1)
    first_day_of_last_month = last_day_of_last_month.replace(day=1)
    return first_day_of_last_month.strftime("%Y/%m/%d"), today.strftime("%Y/%m/%d")

def get_report_url():
    """ログ集計用：2025/12/08から今日までの動的URL"""
    JST = ZoneInfo("Asia/Tokyo")
    today_str = datetime.now(JST).strftime("%Y/%m/%d").replace("/", "%2F")
    return (
        "https://presco.ai/partner/report/search?"
        "searchDateTimeFrom=2025%2F12%2F08&"
        f"searchDateTimeTo={today_str}&"
        "searchItemType=2&searchPeriodType=4&searchProgramId=&"
        "searchDateType=3&searchPartnerSiteId=&searchProgramUrlId=&"
        "searchPartnerSitePageId=&searchLargeGenreId=&searchMediumGenreId=&"
        "searchSmallGenreId=&_searchJoinType=on"
    )

def extract_gclid(url):
    if not url: return ""
    match = re.search(r'gclid=([^&]+)', url)
    return match.group(1) if match else ""

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
        except: continue

    if not raw_data:
        print(f"警告: {csv_path} にデータがありません")
        return

    # --- 数値変換処理 ---
    # CSVは全データが文字列として読み込まれるため、数値に変換可能なセルをキャストする
    processed_data = []
    for i, row in enumerate(raw_data):
        if i == 0:  # ヘッダー行はそのまま
            processed_data.append(row)
            continue
        
        new_row = []
        for cell in row:
            # カンマを除去（例: "1,200" -> "1200"）
            clean_val = cell.replace(',', '')
            
            # 数値変換を試行
            try:
                if clean_val == "":
                    new_row.append("")
                elif '.' in clean_val:
                    new_row.append(float(clean_val))
                else:
                    new_row.append(int(clean_val))
            except ValueError:
                # 数値にできない場合は元の文字列（日付やIDなど）のまま
                new_row.append(cell)
        processed_data.append(new_row)

    # 成果一覧（CVデータ）の場合のみGCLID抽出処理を実行
    if is_cv_data:
        # ヘッダーにGCLID追加
        if len(processed_data[0]) > 12:
            processed_data[0].insert(13, "GCLID")
            for row in processed_data[1:]:
                if len(row) > 12:
                    # row[12]は参照元URL
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
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=20)

    worksheet.clear()
    
    # value_input_option='USER_ENTERED' を使うことで数値や日付が適切に処理されます
    worksheet.update(values=processed_data, range_name="A1", value_input_option='USER_ENTERED')
    print(f"[{datetime.now()}] {sheet_name} 完了")

def main():
    email = os.environ.get('PRESCO_EMAIL')
    password = os.environ.get('PRESCO_PASSWORD')
    
    if not email or not password:
        raise Exception("PRESCOのログイン情報が環境変数に設定されていません")

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
            date_from, date_to = get_target_date_range()
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
            # 成果一覧のアップロード
            process_and_upload(cv_csv_path, CV_SHEET_NAME, is_cv_data=True)
            # ログ集計のアップロード
            process_and_upload(log_csv_path, LOG_SHEET_NAME, is_cv_data=False)

        finally:
            browser.close()

if __name__ == "__main__":
    main()
