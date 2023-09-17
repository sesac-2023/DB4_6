import pandas as pd
try:
    import pymysql
except:
    raise Exception('you need to install pymysql\n$ : python -m pip install pymysql')
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

    def __init__(self, db_config: str|dict, category_file: str|dict, sql_file: str|None=None) -> None:
        """
        데이터베이스 접속
        인자 : 데이터베이스 접속정보
        """
        # 데이터베이스에서 select 해서 가져와도 상관없음.

        # 계정 정보 파일에서 필요 인자 딕셔너리로 생성
        if type(db_config)==str:
            with open(db_config) as f:
                db_config = dict(map(lambda x: x.replace('\n','').split('='), f.readlines()))
                for i, v in db_config.items():
                    if v.isdigit(): db_config[i] = int(v)

        try:
            res = {k: db_config.get(k) for k in ['host', 'port', 'user', 'password', 'database']}
        except:
            raise Exception("db_config must have keys ['host', 'port', 'user', 'password', 'database']\n\
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
        if type(category_file)==str:
            if category_file[-5:]=='.json':
                with open(category_file, 'r') as f:
                    self.SUB_CATEGORY_DICT = json.load(f)
            else:
                raise Exception('category_file can only use .json!\nuse .json or insert dictionary')
        elif type(category_file)==dict:
                self.SUB_CATEGORY_DICT = category_file
        else:
            raise Exception('category_file can only use .json and dictionary!\ninsert path of .json or dictionary')


        # self.PLATFORM_DICT = {
        #     '네이버': 1,
        #     '다음': 2
        # }
        # # tmp = [l.rstrip().split(',') for l in open('./platform_info').readlines()]
        # # self.PLATFORM_DICT = {v: k for k, v in tmp}


        # 테이블 생성. if not exists로 오류 해결.

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
                with open(category_file, 'r') as f:
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
                    print(f'done_task: {len(done_list)}, error_task: {len(error_list)}')
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

    def insert_news(self, df: pd.DataFrame) -> None:
        """
        인자 : 뉴스기사 데이터프레임
        columns = ['cat1_name', 'cat2_name', 'platform_name', 'title', 'press', 'writer', 'date_upload', 'date_fix', 'content', 'sticker', 'url']

        우선 데이터프레임의 column명 체크하여 News 테이블의 칼럼이름과 일치하지 않을 경우 에러 발생시키기

        insert SQL문 생성
        execute 대신 execute_many 메서드로 한번에 삽입

        1. 플랫폼 정보 id로 변환
        2. 메인카테고리 숫자로 변환
        3. 서브카테고리 숫자로 변환
        4. DB에 적재

        """

        # column 이름 일치 확인
        df_columns = ['cat1_name', 'cat2_name', 'platform_name', 'title', 'press', 'writer', 'date_upload', 'date_fix', 'content', 'sticker', 'url']
        if sum(~(df_columns==df.columns)):
            raise Exception(f"columns' name dont matched!!\nmake columns' name like {df_columns}")

        # platform_name 변환 및 cat2_id 할당
        df['platform_name'] = [platform if platform in ("네이버", "다음") else "네이버" if platform.upper()=="NAVER" else "다음" if platform.upper()=="DAUM" else None for platform in df['platform_name']]
        if df['platform_name'].isna().any():
            raise Exception('some platform_name rows wrong!!')
        
        df['cat2_id'] = [self.SUB_CATEGORY_DICT[_[0]][_[1]][_[2]] for _ in df[['platform_name', 'cat1_name', 'cat2_name']].values]
        df_columns = ['cat2_id']+df_columns
        df = df[df_columns]
        df.drop(columns=['cat1_name', 'cat2_name', 'platform_name'], inplace=True)

        # sticker json 따옴표 변경
        if not sum(df['sticker'].apply(str).str.count('"')):
            df['sticker'] = df['sticker'].apply(json.dumps)

        with self.remote.cursor() as cur:
            my_query = "insert ignore into NEWS(cat2_id, title, press, writer, date_upload, date_fix, content, sticker, url) values(%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cur.executemany(my_query, df.values.tolist())
        self.remote.commit()
        print('inserted news!')

    def change_comment_df(self, df: pd.DataFrame([list, str])) -> None:
        """
        인자 : 댓글 데이터프레임
        columns = ['comment', 'url']

        데이터프레임 칼럼 체크하여 Comment 테이블의 칼럼과 일치하지 않을 경우 에러
        
        1. 유저 테이블에서 있는지 체크, id값 있을 경우 변환
        2. 신규 유저일 경우 유저 테이블에 추가, id값 가져오기 (DB에 유저 정보가 저장되어있다면 가져오기)
        3. url을 통해 코멘트 별 뉴스기사 id 가져오기 (select)
        """
        pass

    def insert_comment(self, df: pd.DataFrame([list, str])) -> None:
        """
        인자 : 댓글 데이터프레임
        columns = ['comment', 'url']

        데이터프레임 칼럼 체크하여 Comment 테이블의 칼럼과 일치하지 않을 경우 에러

        1. 댓글 id로 변환하는 함수 호출하여 변환한 데이터프레임 가져오기
        2. DB에 적재
        """

        # column과 url 확인
        df_columns = ['comment', 'url']
        if sum(~(df_columns==df.columns)):
            raise Exception(f"columns' name dont matched!!\nmake columns' name like {df_columns}")
        elif sum(df.url.str.find('comment')>=0):
            raise Exception(f"urls are comments' url!! function needs news contents' urls!!")
        df_columns = ['news_id', 'user_id', 'user_name', 'comment', 'date_upload', 'date_fix', 'good_cnt', 'bad_cnt']
        
        # get news_id
        tmp_list = []
        with self.remote.cursor() as cur:
            my_query = "select id, url from news where url=%s"
            for v in df['url'].values:
                cur.execute(my_query, v)
                tmp_list.extend(cur.fetchall())

        tmp_list = pd.DataFrame(tmp_list, columns=['news_id', 'url'])
        df = pd.merge(df, tmp_list, 'left', 'url').explode('comment').reset_index(drop=True)
        del tmp_list

        # comment_df 변환
        trash, df['comment'], df['user_id'], df['user_name'], df['date_upload'], df['date_fix'], df['good_cnt'], df['bad_cnt'] = zip(*df.comment.values)
        del trash
        df['date_upload'] =  df['date_upload'].str.split('+').str[0]
        if df['date_upload'][0].find('T')>=0:
            df['date_upload'] = [' '.join(date__[:-5].split('T')) for date__ in df['date_upload']]
            df['date_fix'] = [' '.join(date__[:-5].split('T')) for date__ in df['date_fix']]
        df = df[~df.user_id.isna()].reset_index(drop=True)
        df_columns = ['news_id', 'user_id', 'user_name', 'comment', 'date_upload', 'date_fix', 'good_cnt', 'bad_cnt']
        df = df[df_columns]

        # user_df 생성 및 적재
        user_df = df.groupby(['user_id', 'user_name']).count().reset_index()[['user_id', 'user_name']]
        with self.remote.cursor() as cur:
            my_query = "insert ignore into USER(user_id, user_name) values(%s, %s)"
            cur.executemany(my_query, user_df.values.tolist())
        self.remote.commit()
        print('inserted user!')

        # get user_id
        tmp_list = []
        with self.remote.cursor() as cur:
            my_query = "select id, user_id from user where user_id=%s"
            for v in user_df.user_id.values:
                cur.execute(my_query, v)
                tmp_list.extend(cur.fetchall())
        tmp_list = pd.DataFrame(tmp_list, columns=['id', 'user_id'])

        # comment_df 변환 및 적재
        df_columns.pop(2)
        df = pd.merge(df, tmp_list.drop_duplicates('user_id').reset_index(drop=True), 'left', 'user_id').reset_index(drop=True)
        df = df.drop(columns='user_id').rename(columns={'id': 'user_id'})[df_columns]
        del user_df, tmp_list

        with self.remote.cursor() as cur:
            my_query = "insert ignore into COMMENT(news_id, user_id, comment, date_upload, date_fix, good_cnt, bad_cnt) values(%s, %s, %s, %s, %s, %s, %s)"
            cur.executemany(my_query, df.values.tolist())
        self.remote.commit()
        print('inserted comment!')


    # 각 인원이 ERD 통해 데이터베이스에 테이블 생성해서 수집한 데이터로 테스트해 볼 것
        

    def select_news(self, query_command: str) -> tuple|pd.DataFrame:
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
        with self.remote.cursor() as cur:
            cur.execute(query_command)
            if query_command.find('select * from news')==0:
                res = pd.DataFrame(cur.fetchall(), columns=['id', 'cat2_id', 'title', 'press', 'writer', 'date_upload', 'date_fix', 'content', 'sticker', 'url'])
            else:
                res = cur.fetchall()
        return res

    def select_user(self) -> pd.DataFrame:
        """
        인자 : 데이터를 꺼내올 때 사용할 parameters
        (어떻게 검색(필터)해서 유저를 가져올 것인지)

        SQL문에 추가할 내용들
        1. 가져올 칼럼
        2. JOIN할 경우 JOIN문
        3. WHERE 조건문
        4. LIMIT, OFFSET 등 처리
        """
        with self.remote.cursor() as cur:
            cur.execute('select * from user')
            res = pd.DataFrame(cur.fetchall(), columns=['id', 'user_id', 'user_name'])
        return res
        
    def select_comment(self) -> pd.DataFrame:
        """
        인자 : 데이터를 꺼내올 때 사용할 parameters
        (어떻게 검색(필터)해서 댓글을 가져올 것인지)

        SQL문에 추가할 내용들
        1. 가져올 칼럼
        2. JOIN할 경우 JOIN문 (유저정보를 같이 가져올 경우)
        3. WHERE 조건문
        4. LIMIT, OFFSET 등 처리
        """
        with self.remote.cursor() as cur:
            cur.execute('select * from comment')
            res = pd.DataFrame(cur.fetchall(), columns=['id', 'news_id', 'user_id', 'comment', 'date_upload', 'date_fix', 'good_cnt', 'bad_cnt'])
        return res

if __name__ == '__main__':
    # 테스트코드 작성

    # 개인 데이터베이스에 연결
    
    # insert 테스트 (뉴스, 코멘트)

    # select 테스트 (뉴스, 코멘트, 유저)

    pass
