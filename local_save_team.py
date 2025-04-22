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

columns = ["팀", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, "R", "H", "E", "B", "결과"]

# 시작 url 주소 번호 (임시)
start_no = 20250036

# 하루에 5경기 진행
for game_offset in range(5):
    s_no = start_no + game_offset
    url = f'https://statiz.sporki.com/schedule/?m=summary&s_no={s_no}'
    
    response = requests.get(url)

    # 페이지가 없거나 실패하면 넘어가기
    if response.status_code != 200:
        print(f"경기 {s_no} 데이터 없음 (status {response.status_code}), 스킵합니다.")
        continue
    
    # BeautifulSoup 객체 생성
    soup = BeautifulSoup(response.text, 'html.parser')
    head = soup.find('div', class_='box_head')
    table = soup.find('div', class_='table_type03')

    # 없으면 스킵
    if head is None or table is None:
        print(f"경기 {s_no}에서 box_head 또는 table_type03이 없습니다. 스킵합니다.")
        continue

    # 팀별 경기 결과 기록 저장
    title_text = head.get_text(strip=True)
    # 빈 DataFrame 생성
    df = pd.DataFrame(columns=columns)

    # 테이블에서 데이터 추출
    tbody = table.find('tbody')
    rows = tbody.find_all('tr')

    for row in rows:
        if 'total' in row.get('class', []):
            continue
        
        cells = row.find_all('td')
        data = []
        for idx, cell in enumerate(cells):
            # 첫 번째 컬럼(팀명)은 <a> 안에 있으니 처리
            if idx == 0:
                team_name = cell.find('a').text.strip()
                data.append(team_name)
            else:
                # 점수만 추출 (div.score 안에 있음)
                score_div = cell.find('div', class_='score')
                if score_div:
                    score_text = score_div.contents[0].strip()  # 점수만 추출
                    data.append(score_text)
                else:
                    data.append('')
        
        if len(data) < len(columns) - 1:  # "결과" 컬럼은 아직 없음
            data += [''] * (len(columns) - 1 - len(data))

        temp_df = pd.DataFrame([data], columns=columns[:-1])  # "결과" 제외
        df = pd.concat([df, temp_df], ignore_index=True)
    
    # 'R' 컬럼 기준으로 승/패/무 결과 입력
    if not df.empty and "R" in df.columns:
        try:
            score1 = int(df.loc[0, "R"])
            score2 = int(df.loc[1, "R"])
            if score1 > score2:
                df.loc[0, "결과"] = "승"
                df.loc[1, "결과"] = "패"
            elif score1 < score2:
                df.loc[0, "결과"] = "패"
                df.loc[1, "결과"] = "승"
            else:
                df.loc[0, "결과"] = "무"
                df.loc[1, "결과"] = "무"
        except Exception as e:
            print(f"결과 계산 실패: {e}")

    # 파일명: 연도/월/일/오늘날짜_기록명_(팀명).csv
    s3_path = f"{year}/{month}/{day}/{title_text.replace(' ', '_')}.csv"

    # DataFrame을 메모리에 CSV로 저장
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')

    # S3 업로드
    s3.put_object(Bucket=bucket_name, Key=s3_path, Body=csv_buffer.getvalue(), ContentType='text/csv')

    print(f"S3 업로드 완료: s3://{bucket_name}/{s3_path}")

    # 요청 끝나고, 다음 요청 전에 1~3초 랜덤 딜레이 주기
    time.sleep(random.uniform(1, 3))