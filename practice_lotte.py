import os
import requests
from bs4 import BeautifulSoup
import pandas as pd

# 오늘 날짜 설정 (폴더명용)
from datetime import datetime
today = '20250323'
save_folder = f'./{today}'
os.makedirs(save_folder, exist_ok=True)

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

# 5경기 반복 (01~05)
start_no = 20250006

for game_offset in range(5):
    s_no = start_no + game_offset
    url = f'https://statiz.sporki.com/schedule/?m=boxscore&s_no={s_no}'
    
    # 요청 보내기
    response = requests.get(url)
    
    # 페이지가 없거나 실패하면 넘어가기
    if response.status_code != 200:
        print(f"경기 {s_no} 데이터 없음 (status {response.status_code}), 스킵합니다.")
        continue
    
    # BeautifulSoup 객체 생성
    soup = BeautifulSoup(response.text, 'html.parser')

    # box_head, table_type03 각각 매칭
    box_heads = soup.find_all('div', class_='box_head')
    tables = soup.find_all('div', class_='table_type03')

    # 팀별 기록 저장
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

        # 파일명: 날짜폴더/경기번호_기록명_(팀명).csv
        file_name = f"{s_no}_{title_text.replace(' ', '_')}.csv"
        file_path = os.path.join(save_folder, file_name)

        # CSV 저장
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"파일 저장 완료: {file_path}")
