import pandas as pd
try:
    import pymysql
except:
    print('you need to install pymysql\n$ : python -m pip install pymysql')
import traceback, json, re

"""
사용할 라이브러리 : pymysql, pandas

요구사항
1. 클래스 내 생성자, 소멸자, insert, select 함수 구현
2. 테스트를 위한 실행 코드 작성

참고해볼만한 코드
https://github.com/devsosin/sosin/blob/main/sosin/databases/rdb.py
"""

class NewsDB:
    """
    클래스 설명
    """

    def __init__(self, db_config: str|dict) -> None:
        """
        데이터베이스 접속
        인자 : 데이터베이스 접속정보
        """
        # 데이터베이스에서 select 해서 가져와도 상관없음.

        # 계정 정보 파일에서 필요 인자 딕셔너리로 생성
        if type(db_config)==str:
            with open('secret_db.config') as f:
                db_config = dict(map(lambda x: x.replace('\n','').split('='), f.readlines()))
                for i, v in db_config.items():
                    if v.isdigit(): db_config[i] = int(v)

        try:
            res = {k: db_config.get(k) for k in ['host', 'port', 'user', 'password', 'database']}
        except:
            print("db_config must have keys ['host', 'port', 'user', 'password', 'database']\n\
                  port's dtype must be int")

        # 딕셔너리 언패킹하여 인자 값 할당 후 서버 연동
        self.remote = pymysql.connect(**res)


        # # 메인카테고리 정보 - DB에 적재된 id값, 이름
        # # 1 - 정치, 2 - 사회
        # tmp = [l.rstrip().split(',') for l in open('./main_category').readlines()]
        # self.MAIN_CATEGORY_DICT = {v: k for k, v in tmp}
        # # 서브카테고리 정보 - DB에 적재된 id값, 이름
        # # 1 - 대통령실, 2 - ...
        # tmp = [l.rstrip().split(',') for l in open('./sub_category').readlines()]
        with open('tmp_json.json', 'r') as f:
            self.SUB_CATEGORY_DICT = json.load(f)

        # self.PLATFORM_DICT = {
        #     '네이버': 1,
        #     '다음': 2
        # }
        # # tmp = [l.rstrip().split(',') for l in open('./platform_info').readlines()]
        # # self.PLATFORM_DICT = {v: k for k, v in tmp}


        # 테이블 생성. if not exists로 오류 해결.
        sql_file = 'Elementary_ERD.sql'
        # sql_file = 'Elementary_ERD.txt'

        with self.remote.cursor() as cur:
            # 파일에서 '\ufeff'가 읽힐 경우 encoding하거나 replace로 제거
            with open(sql_file, 'r', encoding='utf-8-sig') as f :
                # split(';')이기에 마지막에 ['']가 존재하여 [:-1]로 슬라이싱
                commands = f.read().split(';')[:-1]
            for command in commands:
                cur.execute(command.strip())

        
        # category 테이블 record 적재.
        with self.remote.cursor() as cur:
            # category 불러오기
            try:
                with open('category.json', 'r') as f:
                    category = json.load(f)
            except:
                print("category.json don't exist or path is worng.")

            # category 확인 및 적재
            done_list = []
            error_list = []
            try:
                cur.execute('select count(*) from category')
                if cur.fetchall()[0][0]==145:
                    print('='*50)
                    print('already all record is loaded on CATEGORY table')
                    pass
                else:
                    for platfrom in category.keys():
                        for cat_1 in category[platfrom].keys():
                            for name, id in category[platfrom][cat_1].items():
                                try:
                                    cur.execute(f"insert into category values({id}, '{cat_1}','{name}', '{platfrom}')")
                                    done_list.append([id, cat_1, name, platfrom])
                                except:
                                    error_list.append([id, cat_1, name, platfrom])
                                    print(f"already values({id}, '{cat_1}','{name}', '{platfrom}' exist")
                    print('='*50)
                    print(f'done_tasl: {len(done_list)}, error_task: {len(error_list)}')
            except:
                print("make tables first!")

        # DML은 별도 commit 필요!
        self.remote.commit()
        print('task is done!')

    def __del__(self) -> None:
        """
        데이터베이스 연결 해제
        """
        self.remote.close()

    def insert_news_with_comment(self, news_df: pd.DataFrame|list, comment_df: pd.DataFrame|list) -> None:
        """
        인자 : 댓글 데이터프레임

        데이터프레임 칼럼 체크하여 Comment 테이블의 칼럼과 일치하지 않을 경우 에러

        1. 댓글 id로 변환하는 함수 호출하여 변환한 데이터프레임 가져오기
        2. DB에 적재
        """
        """
        인자 : 뉴스기사 데이터프레임
        [cat2_id, title, press, writer, date_upload, date_fix, content, sticker, url]
        
        우선 데이터프레임의 column명 체크하여 News 테이블의 칼럼이름과 일치하지 않을 경우 에러 발생시키기

        insert SQL문 생성
        execute 대신 execute_many 메서드로 한번에 삽입

        1. 플랫폼 정보 id로 변환
        2. 메인카테고리 숫자로 변환
        3. 서브카테고리 숫자로 변환
        4. DB에 적재

        """
        pass


    def insert_news(self):
        """
        인자 : 뉴스기사 데이터프레임
        
        우선 데이터프레임의 column명 체크하여 News 테이블의 칼럼이름과 일치하지 않을 경우 에러 발생시키기

        insert SQL문 생성
        execute 대신 execute_many 메서드로 한번에 삽입

        1. 플랫폼 정보 id로 변환
        2. 메인카테고리 숫자로 변환
        3. 서브카테고리 숫자로 변환
        4. DB에 적재

        """
        pass

    # insert_user()
    # 댓글 데이터프레임에서 유저 정보만 뽑아서 넣는 방법도 있음

    def change_comment_df(self):
        """
        인자 : 댓글 데이터프레임

        데이터프레임 칼럼 체크하여 Comment 테이블의 칼럼과 일치하지 않을 경우 에러
        
        1. 유저 테이블에서 있는지 체크, id값 있을 경우 변환
        2. 신규 유저일 경우 유저 테이블에 추가, id값 가져오기 (DB에 유저 정보가 저장되어있다면 가져오기)
        3. url을 통해 코멘트 별 뉴스기사 id 가져오기 (select)
        """

    def insert_comment(self):
        """
        인자 : 댓글 데이터프레임

        데이터프레임 칼럼 체크하여 Comment 테이블의 칼럼과 일치하지 않을 경우 에러

        1. 댓글 id로 변환하는 함수 호출하여 변환한 데이터프레임 가져오기
        2. DB에 적재
        """

    # 각 인원이 ERD 통해 데이터베이스에 테이블 생성해서 수집한 데이터로 테스트해 볼 것
        

    def select_news(self):
        """
        인자 : 데이터를 꺼내올 때 사용할 parameters 
        (어떻게 검색(필터)해서 뉴스기사를 가져올 것인지)

        DB에 들어있는 데이터를 꺼내올 것인데, 어떻게 꺼내올지를 고민

        인자로 받은 파라미터 별 조건을 넣은 select SQL문 작성

        SQL문에 추가할 내용들
        1. 가져올 칼럼
        2. JOIN할 경우 JOIN문 (플랫폼, 카테고리)
        3. WHERE 조건문
        4. LIMIT, OFFSET 등 처리
        """
        pass

    def select_user(self):
        """
        인자 : 데이터를 꺼내올 때 사용할 parameters
        (어떻게 검색(필터)해서 유저를 가져올 것인지)

        SQL문에 추가할 내용들
        1. 가져올 칼럼
        2. JOIN할 경우 JOIN문
        3. WHERE 조건문
        4. LIMIT, OFFSET 등 처리
        """
    
    def select_comment(self):
        """
        인자 : 데이터를 꺼내올 때 사용할 parameters
        (어떻게 검색(필터)해서 댓글을 가져올 것인지)

        SQL문에 추가할 내용들
        1. 가져올 칼럼
        2. JOIN할 경우 JOIN문 (유저정보를 같이 가져올 경우)
        3. WHERE 조건문
        4. LIMIT, OFFSET 등 처리
        """

if __name__ == '__main__':
    # 테스트코드 작성

    # 개인 데이터베이스에 연결
    
    # insert 테스트 (뉴스, 코멘트)

    # select 테스트 (뉴스, 코멘트, 유저)

    pass
