import requests
import pandas as pd
import boto3
import time
import random
from bs4 import BeautifulSoup
from datetime import datetime
from io import StringIO

# AWS S3 설정
s3 = boto3.client('s3', region_name='us-east-1')
bucket_name = 'hwan-baseball-bucket'

# 오늘 날짜
today = datetime.now().strftime('%Y%m%d')
year = today[:4]
month = today[4:6]
day = today[6:8]

# 기록별 컬럼 미리 지정
columns_dict = {
    '타격기록': ["타순", "이름", "포지션", "타석", "타수", "득점", "안타", "홈런", "타점", 
                "볼넷/4구", "몸에 맞는 볼", "삼진", "땅볼", "플라이", "투구 수", "병살타", 
                "잔루", "타율", "OPS", "LI", "WPA", "RE24"],
    '투구기록': ["이름", "이닝", "타자", "안타", "실점", "자책점", "볼넷/4구", "몸에 맞는 볼", 
                "삼진", "홈런", "땅볼-플라이", "투구수-스트라이크수", "승계주자-승계주자득점", 
                "선발투수 평점", "ERA", "WHIP", "LI", "WPA", "RE24"],
    '수비기록': ["포지션", "이름", "수비이닝", "자살", "보살", "실책", "포구 실책", 
                "송구 실책", "병살상황 자살", "병살상황 보살"]
}

# 시작 url 주소 번호 (임시)
start_no = 20250036

# 하루에 5경기 진행
for game_offset in range(5):
    s_no = start_no + game_offset
    url = f'https://statiz.sporki.com/schedule/?m=boxscore&s_no={s_no}'
    
    response = requests.get(url)

    # 페이지가 없거나 실패하면 넘어가기
    if response.status_code != 200:
        print(f"경기 {s_no} 데이터 없음 (status {response.status_code}), 스킵합니다.")
        continue
    
    # BeautifulSoup 객체 생성
    soup = BeautifulSoup(response.text, 'html.parser')
    box_heads = soup.find_all('div', class_='box_head')
    tables = soup.find_all('div', class_='table_type03')

    # 팀별 선수 기록 저장
    for head, table in zip(box_heads, tables):
        title_text = head.get_text(strip=True)
        
        # 어떤 기록인지 구분
        if '타격기록' in title_text:
            record_type = '타격기록'
        elif '투구기록' in title_text:
            record_type = '투구기록'
        elif '수비기록' in title_text:
            record_type = '수비기록'
        else:
            continue  # 필요한 기록이 아니면 스킵

        # 컬럼 설정
        columns = columns_dict[record_type]

        # 빈 DataFrame 생성
        df = pd.DataFrame(columns=columns)

        # 테이블에서 데이터 추출
        tbody = table.find('tbody')
        rows = tbody.find_all('tr')

        for row in rows:
            if 'total' in row.get('class', []):
                continue
            
            cells = row.find_all('td')

            if len(cells) == len(columns):
                data = []
                for idx, cell in enumerate(cells):
                    if record_type == '수비기록' and idx == 1:
                        if cell.find('a'):
                            data.append(cell.find('a').get_text(strip=True))
                        else:
                            data.append(cell.get_text(strip=True))
                    else:
                        data.append(cell.get_text(strip=True))

                temp_df = pd.DataFrame([data], columns=columns)
                df = pd.concat([df, temp_df], ignore_index=True)

        # 파일명: 연도/월/일/오늘날짜_기록명_(팀명).csv
        s3_path = f"{year}/{month}/{day}/{today}_{title_text.replace(' ', '_')}.csv"

        # DataFrame을 메모리에 CSV로 저장
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')

        # S3 업로드
        s3.put_object(Bucket=bucket_name, Key=s3_path, Body=csv_buffer.getvalue(), ContentType='text/csv')

        print(f"S3 업로드 완료: s3://{bucket_name}/{s3_path}")

        # ✅ 요청 끝나고, 다음 요청 전에 1~3초 랜덤 딜레이 주기
        time.sleep(random.uniform(1, 3))