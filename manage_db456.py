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

    def __init__(self, db_config: str|dict, category_file: str|dict|None=None, sql_file: str|None=None) -> None:
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

        if category_file:
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
            
        else:
            with self.remote.cursor() as cur:
                cur.execute('select * from CATEGORY')
                tmp = cur.fetchall()

            self.SUB_CATEGORY_DF = pd.DataFrame(tmp, columns=['cat2_id', 'cat1_name', 'cat2_name', 'platform_name'])
            self.SUB_CATEGORY_DICT = {}
            [self.SUB_CATEGORY_DICT.update({p: {}}) for p in self.SUB_CATEGORY_DF.platform_name.unique()]
            [self.SUB_CATEGORY_DICT[p].update({c1: {}}) for p in self.SUB_CATEGORY_DF.platform_name.unique() for c1 in self.SUB_CATEGORY_DF[self.SUB_CATEGORY_DF.platform_name==p].cat1_name.unique()]
            [self.SUB_CATEGORY_DICT[p][c1].update({c2[1]: c2[0]}) for p in self.SUB_CATEGORY_DF.platform_name.unique() for c1 in self.SUB_CATEGORY_DF[self.SUB_CATEGORY_DF.platform_name==p].cat1_name.unique() for c2 in self.SUB_CATEGORY_DF[(self.SUB_CATEGORY_DF.platform_name==p)&(self.SUB_CATEGORY_DF.cat1_name==c1)][['cat2_id', 'cat2_name']].values]

        # 테이블 생성. if not exists로 오류 해결.
        if sql_file:
            with self.remote.cursor() as cur:
                # 파일에서 '\ufeff'가 읽힐 경우 encoding하거나 replace로 제거
                with open(sql_file, 'r', encoding='utf-8-sig') as f :
                    # split(';')이기에 마지막에 ['']가 존재하여 [:-1]로 슬라이싱
                    commands = f.read().split(';')[:-1]
                for command in commands:
                    cur.execute(command.strip())

        
        # category 테이블 record 적재.
        if category_file:
            with self.remote.cursor() as cur:
                # category 불러오기
                if type(category_file)==str:
                    try:
                        with open(category_file, 'r') as f:
                            category = json.load(f)
                    except:
                        raise Exception("category.json don't exist or path is worng.")

                # category 확인 및 적재
                done_list = []
                error_list = []
                try:
                    cur.execute('select count(*) from CATEGORY')
                    if cur.fetchall()[0][0]==145:
                        print('='*50)
                        print('already all record is loaded on CATEGORY table')
                        pass
                    else:
                        for platfrom in category.keys():
                            for cat_1 in category[platfrom].keys():
                                for name, id in category[platfrom][cat_1].items():
                                    try:
                                        cur.execute(f"insert into CATEGORY values({id}, '{cat_1}','{name}', '{platfrom}')")
                                        done_list.append([id, cat_1, name, platfrom])
                                    except:
                                        error_list.append([id, cat_1, name, platfrom])
                                        print(f"already values({id}, '{cat_1}','{name}', '{platfrom}' exist")
                        print('='*50)
                        print(f'done_task: {len(done_list)}, error_task: {len(error_list)}')
                except:
                    raise Exception("make tables first!")
                
            if category_file:
                with self.remote.cursor() as cur:
                    cur.execute('select * from CATEGORY')
                    tmp = cur.fetchall()
                self.SUB_CATEGORY_DF = pd.DataFrame(tmp, columns=['cat2_id', 'cat1_name', 'cat2_name', 'platform_name'])

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
        df = df[df_columns]
        for col in df.columns:
            df.loc[df[col].isna(), col] = None
            df.loc[(df[col]==''), col] = None

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
        columns = ['user_id', 'user_name']

        데이터프레임 칼럼 체크하여 Comment 테이블의 칼럼과 일치하지 않을 경우 에러
        
        1. 유저 테이블에서 있는지 체크, id값 있을 경우 변환
        2. 신규 유저일 경우 유저 테이블에 추가, id값 가져오기 (DB에 유저 정보가 저장되어있다면 가져오기)
        3. url을 통해 코멘트 별 뉴스기사 id 가져오기 (select)
        """
        
        # user_df 생성 및 적재
        with self.remote.cursor() as cur:
            my_query = "insert ignore into USER(user_id, user_name) values(%s, %s)"
            cur.executemany(my_query, df.values.tolist())
        self.remote.commit()

        # get user_id
        with self.remote.cursor() as cur:
            cur.execute("select id, user_id from USER")
            user_df = pd.DataFrame(cur.fetchall(), columns=['id', 'user_id'])
        df = user_df[user_df.user_id.isin(df.user_id.values)]

        # get news_id
        with self.remote.cursor() as cur:
            cur.execute("select c.user_id, c.news_id, c.comment, n.url from COMMENT c, NEWS n where c.news_id = n.id")
            news_df = pd.DataFrame(cur.fetchall(), columns=['id', 'news_id', 'comment', 'url'])
        df = news_df[news_df.id.isin(df.id.values)]
        return df
        
        

    def insert_comment(self, df: pd.DataFrame) -> None:
        """
        인자 : 댓글 데이터프레임
        columns = ['user_id', 'user_name', 'comment', 'date_upload', 'date_fix', 'good_cnt', 'bad_cnt', 'url']

        데이터프레임 칼럼 체크하여 Comment 테이블의 칼럼과 일치하지 않을 경우 에러

        1. 댓글 id로 변환하는 함수 호출하여 변환한 데이터프레임 가져오기
        2. DB에 적재
        """

        # column과 url 확인
        df_columns = ['user_id', 'user_name', 'comment', 'date_upload', 'date_fix', 'good_cnt', 'bad_cnt', 'url']
        df = df[df_columns]
        for col in df.columns:
            df.loc[df[col].isna(), col] = None
            df.loc[(df[col]==''), col] = None

        # get news_id
        with self.remote.cursor() as cur:
            cur.execute("select id, url from NEWS")
            tmp_df = pd.DataFrame(cur.fetchall(), columns=['news_id', 'url'])

        df = pd.merge(df, tmp_df, 'left', 'url').reset_index(drop=True)
        del tmp_df

        # comment_df 변환
        df = df[~df.user_id.isna()].reset_index(drop=True)
        df_columns.pop()
        df_columns = ['news_id']+df_columns
        df = df[df_columns]

        # user_df 생성 및 적재
        user_df = df.groupby(['user_id', 'user_name']).count().reset_index()[['user_id', 'user_name']]
        with self.remote.cursor() as cur:
            my_query = "insert ignore into USER(user_id, user_name) values(%s, %s)"
            cur.executemany(my_query, user_df.values.tolist())
        self.remote.commit()
        print('inserted user!')

        # get user_id
        with self.remote.cursor() as cur:
            cur.execute("select id, user_id from USER")
            tmp_df = pd.DataFrame(cur.fetchall(), columns=['id', 'user_id'])

        # comment_df 변환 및 적재
        df_columns.pop(2)
        df = pd.merge(df, tmp_df.drop_duplicates('user_id').reset_index(drop=True), 'left', 'user_id').reset_index(drop=True)
        df = df.drop(columns='user_id').rename(columns={'id': 'user_id'})[df_columns]
        del user_df,

        with self.remote.cursor() as cur:
            my_query = "insert ignore into COMMENT(news_id, user_id, comment, date_upload, date_fix, good_cnt, bad_cnt) values(%s, %s, %s, %s, %s, %s, %s)"
            cur.executemany(my_query, df.values.tolist())
        self.remote.commit()
        print('inserted comment!')


    # 각 인원이 ERD 통해 데이터베이스에 테이블 생성해서 수집한 데이터로 테스트해 볼 것
        
    def select_news(self, start_date=None, end_date=None, 
                    platform: str|None=None, category1: str|list|None=None, category2: str|list|None=None, 
                    columns_name: list=['id', 'cat2_id', 'title', 'press', 'writer', 'date_upload', 'date_fix', 'content', 'sticker', 'url'], 
                    where_sql: list=[],
                    limit: str|int=100000) -> pd.DataFrame:
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


        if start_date and end_date:
            where_sql.append(f"date_upload BETWEEN '{start_date}' AND '{end_date}'")
        elif start_date:
            where_sql.append(f"date_upload >= '{start_date}'")
        elif end_date:
            where_sql.append(f"date_upload <= '{end_date}'")

        if platform or category1 or category2:
            tmp_SUB_CATEGORY_DF = self.SUB_CATEGORY_DF.copy()
            if platform:
                if platform=='다음':
                    where_sql.append(f"cat2_id<20000")
                elif platform=='네이버':
                    where_sql.append(f"cat2_id>20000")
                else:
                    raise Exception('you can use only "다음" or "네이버"!')
                tmp_SUB_CATEGORY_DF = tmp_SUB_CATEGORY_DF[self.SUB_CATEGORY_DF.platform_name==platform]
            
            if category1:
                isin_list = []
                if type(category1)==str:
                    category1 = [category1]
                for value in category1:
                    isin_list.append(value)
                tmp_SUB_CATEGORY_DF = tmp_SUB_CATEGORY_DF[tmp_SUB_CATEGORY_DF.cat1_name.isin(isin_list)]
            
            if category2:
                isin_list = []
                if type(category2)==str:
                    category2 = [category2]
                for value in category2:
                    isin_list.append(value)
                tmp_SUB_CATEGORY_DF = tmp_SUB_CATEGORY_DF[tmp_SUB_CATEGORY_DF.cat2_name.isin(isin_list)]
            where_sql.append(f"cat2_id in ({','.join(tmp_SUB_CATEGORY_DF.cat2_id.apply(str).values.tolist())})")
        else:
            tmp_SUB_CATEGORY_DF=None
        

        # main_query = f'SELECT id,cat2_id,title,press,writer,date_upload,content,sticker,url FROM NEWS '
        main_query = f'SELECT {", ".join(columns_name)} FROM NEWS '

        final_result = []
        if where_sql:
            main_query += f' WHERE {" AND ".join(where_sql)}'
        if int(limit)<100000:
            main_query += f' limit {limit}'
            with self.remote.cursor() as cur:
                cur.execute(main_query)
                result = cur.fetchall()
                final_result.extend(result)
        else:
            # 1GB Ram 제한 (limit, offset)
            pagination_sql = ' LIMIT 100000 OFFSET {}'
            offset = 0
            while True:
                with self.remote.cursor() as cur:
                    cur.execute(main_query + pagination_sql.format(offset))
                    result = cur.fetchall()
                    final_result.extend(result)

                if len(result) < 100000:
                    break

                offset += 100000 # LIMIT
        
        df = pd.DataFrame(final_result, columns=columns_name)
        if 'cat2_id' in columns_name:
            tmp_SUB_CATEGORY_DF = self.SUB_CATEGORY_DF[self.SUB_CATEGORY_DF.cat2_id.isin(df.cat2_id.unique())]
            df = pd.merge(df, tmp_SUB_CATEGORY_DF, 'left', 'cat2_id')
            for col in ['platform_name', 'cat2_name', 'cat1_name']:
                columns_name.insert(columns_name.index('cat2_id')+1, col)
            df = df[columns_name]

        return df
    
    def select(self, query_command: str) -> tuple:
        """
        select문 직접 작성
        """
        if query_command.find(';')>=0:
            raise Exception('you can use only one query')
        with self.remote.cursor() as cur:
            cur.execute(query_command)
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
            cur.execute('select * from USER')
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
            cur.execute('select * from COMMENT')
            res = pd.DataFrame(cur.fetchall(), columns=['id', 'news_id', 'user_id', 'comment', 'date_upload', 'date_fix', 'good_cnt', 'bad_cnt'])
        return res

if __name__ == '__main__':
    # 테스트코드 작성

    # 개인 데이터베이스에 연결
    
    # insert 테스트 (뉴스, 코멘트)

    # select 테스트 (뉴스, 코멘트, 유저)

    pass
