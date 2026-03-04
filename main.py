import os
import time
import csv
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from playwright.sync_api import sync_playwright
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json

def get_target_date_range():
    """先月1日から今日までの日付文字列を取得"""
    JST = ZoneInfo("Asia/Tokyo")
    today = datetime.now(JST)
    
    # 先月1日の計算
    first_day_of_this_month = today.replace(day=1)
    last_day_of_last_month = first_day_of_this_month - timedelta(days=1)
    first_day_of_last_month = last_day_of_last_month.replace(day=1)
    
    date_from = first_day_of_last_month.strftime("%Y/%m/%d")
    date_to = today.strftime("%Y/%m/%d")
    
    return date_from, date_to

def login_and_download_csv():
    """Presco.aiにログインして先月〜今月のCSVをダウンロード"""
    print(f"[{datetime.now()}] 処理を開始します")
    
    email = os.environ.get('PRESCO_EMAIL')
    password = os.environ.get('PRESCO_PASSWORD')
    
    if not email or not password:
        raise Exception("環境変数 PRESCO_EMAIL, PRESCO_PASSWORD が設定されていません")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox'])
        context = browser.new_context(viewport={'width': 1920, 'height': 1080})
        context.set_default_timeout(60000)
        page = context.new_page()
        
        try:
            # ログイン
            print(f"[{datetime.now()}] ログインページにアクセスします")
            page.goto('https://presco.ai/partner/', timeout=60000)
            page.wait_for_selector('input[name="username"]', timeout=10000)
            
            page.fill('input[name="username"]', email)
            page.fill('input[name="password"]', password)
            
            with page.expect_navigation(timeout=60000):
                page.click('input[type="submit"][value="ログイン"]')
            
            if 'home' not in page.url and 'actionLog' not in page.url:
                raise Exception("ログインに失敗しました")
            print(f"[{datetime.now()}] ログイン成功")
            
            # 成果一覧へ移動
            page.goto('https://presco.ai/partner/actionLog/list', timeout=60000)
            time.sleep(5)
            
            # 集計基準を「成果判定日時」に変更
            print(f"[{datetime.now()}] 集計基準を「成果判定日時」に変更します")
            try:
                page.click('label:has-text("成果判定日時")', timeout=3000)
                time.sleep(1)
            except:
                print(f"[{datetime.now()}] 警告: 集計基準の変更に失敗しました（デフォルトのまま続行）")

            # 期間を「先月1日〜今日」に変更（確実に入力させる処理）
            date_from, date_to = get_target_date_range()
            print(f"[{datetime.now()}] 期間を {date_from} 〜 {date_to} に設定します")
            
            # Enterキーの誤爆を防ぐため、fillとJavaScriptのchangeイベントを使用
            page.fill('#dateTimeFrom', date_from)
            page.evaluate('document.getElementById("dateTimeFrom").dispatchEvent(new Event("change", { bubbles: true }))')
            time.sleep(1)
            
            page.fill('#dateTimeTo', date_to)
            page.evaluate('document.getElementById("dateTimeTo").dispatchEvent(new Event("change", { bubbles: true }))')
            time.sleep(1)
            
            # 検索ボタンクリック（複数のセレクタでアタックする）
            print(f"[{datetime.now()}] 検索条件で絞り込むをクリックします")
            selectors = [
                'span.text:has-text("検索条件で絞り込む")',
                'span:has-text("検索条件で絞り込む")',
                'text="検索条件で絞り込む"'
            ]
            
            clicked = False
            for selector in selectors:
                try:
                    # 5秒ずつ各セレクタを試す
                    page.click(selector, timeout=5000)
                    clicked = True
                    print(f"[{datetime.now()}] 成功: {selector} でクリックしました")
                    break
                except:
                    continue
            
            if not clicked:
                raise Exception("検索ボタンの特定・クリックに失敗しました")
                
            time.sleep(5)
            
            # CSVダウンロード
            page.wait_for_selector('#csv-link', state='visible', timeout=30000)
            print(f"[{datetime.now()}] CSVダウンロードを開始します")
            with page.expect_download(timeout=60000) as download_info:
                page.click('#csv-link')
            
            download = download_info.value
            csv_path = '/tmp/presco_data.csv'
            download.save_as(csv_path)
            print(f"[{datetime.now()}] CSVを保存しました: {csv_path}")
            
            return csv_path
            
        finally:
            browser.close()

def extract_gclid(url):
    """URLからgclidの値を抽出する"""
    if not url:
        return ""
    match = re.search(r'gclid=([^&]+)', url)
    if match:
        return match.group(1)
    return ""

def read_csv_data(csv_path):
    """ダウンロードしたCSVを読み込み、gclid列を追加して返す"""
    encodings = ['utf-8-sig', 'utf-8', 'shift_jis', 'cp932']
    for encoding in encodings:
        try:
            with open(csv_path, 'r', encoding=encoding) as f:
                reader = csv.reader(f)
                data = list(reader)
                
                if not data:
                    return []
                
                # 1行目（ヘッダー）のM列(インデックス12)の隣に「GCLID」列を追加
                if len(data[0]) > 12:
                    data[0].insert(13, "GCLID")
                
                # 2行目以降のデータ処理
                for row in data[1:]:
                    if len(row) > 12:
                        referrer_url = row[12]
                        gclid_value = extract_gclid(referrer_url)
                        row.insert(13, gclid_value)
                    else:
                        # M列が存在しない短い行の場合は空文字を追加
                        row.append("")
                        
                return data
                
        except UnicodeDecodeError:
            continue
            
    raise Exception("CSVファイルの読み込みに失敗しました")

def upload_to_spreadsheet(csv_path):
    """スプレッドシートにデータを転記"""
    print(f"[{datetime.now()}] Google Sheetsへのアップロードを開始します")
    
    creds_json = os.environ.get('GOOGLE_CREDENTIALS')
    spreadsheet_id = os.environ.get('SPREADSHEET_ID')
    
    creds_dict = json.loads(creds_json)
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    gc = gspread.authorize(credentials)
    
    spreadsheet = gc.open_by_key(spreadsheet_id)
    
    # 指定のシート名
    sheet_name = 'PrescoCV' 
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
    except:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=20)
    
    data = read_csv_data(csv_path)
    
    print(f"[{datetime.now()}] シートの中身をクリア（リセット）します")
    worksheet.clear()
    
    if data:
        print(f"[{datetime.now()}] データを書き込みます（{len(data)}行）")
        worksheet.update(values=data, range_name="A1")
    
    print(f"[{datetime.now()}] 完了しました。")

def main():
    try:
        csv_path = login_and_download_csv()
        upload_to_spreadsheet(csv_path)
    except Exception as e:
        print(f"[{datetime.now()}] エラーが発生しました: {str(e)}")
        raise

if __name__ == "__main__":
    main()
