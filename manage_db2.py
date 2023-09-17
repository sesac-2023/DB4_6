import pandas as pd
import pymysql
import json

class NewsDB:
    """
    클래스 설명
    """

    def __init__(self, db_config:dict, cursor_type="tuple") -> None:
        """
        데이터베이스 접속
        인자 : 데이터베이스 접속정보
        """
        db_config['port'] = int(db_config.get('port', '3306'))
        self.DB = pymysql.connect(**db_config)
        
        if cursor_type == 'dict':
            self.cursor_type = pymysql.cursors.DictCursor
        else:
            self.cursor_type = None
    
    def __del__(self) -> None:
        """
        데이터베이스 연결 해제
        """

        self.DB.close()


    def insert_news(self, news_df):
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
        # 기본적으로는 NOT NULL인 값들만 체크
        required_columns = ['platform', 'category1', 'category2', 'content', 'title', 'date_upload', 'writer', 'url', 'sticker']
        assert not set(required_columns) - set(news_df.columns), '테이블 칼럼이 부족합니다.'
        
        # select 함수
        category_info = self.DB.select('*', 'CATEGORY')
        self.cat_dict = {tuple(k[1:]): k[0] for k in category_info}
        cat2_ids = []

        for platform, cat1, cat2 in news_df[['platform', 'category1', 'category2']].iloc:
            try:
                cat2_ids.append(self.db2cat_dict[(cat2, cat1, platform)])
            except:
                cat2_ids.append('')
        news_df['cat2_id'] = cat2_ids
        news_df['sticker'] = news_df['sticker'].apply(json.dumps)

        # 데이터 INSERT
        target_column = ['cat2_id', 'title', 'content', 'date_upload', 'writer', 'url', 'sticker']
        table = 'NEWS'
        columns = ','.join(target_column)
        values = news_df[target_column].values.tolist()

        sql = f"INSERT INTO {table}({columns}) " \
                  "VALUES ("  + ','.join(["%s"]*len(values[0])) + ");"
        
        try:
            with self.DB.cursor() as cur:
                cur.executemany(sql, values)
                self.DB.commit()
            return True
        except:
            import traceback
            traceback.print_exc()
            self.DB.rollback()
            return False

    # insert_user()
    # 댓글 데이터프레임에서 유저 정보만 뽑아서 넣는 방법도 있음
    def insert_user(self, user_df):
        """
        유저 insert
        """
        user_df.drop_duplicates(subset='user_id', inplace=True)

        target_column = ['user_id', 'user_name']
        table = 'USER'
        column = ','.join(target_column)
        values = user_df[target_column].values.tolist()
        sql = f'INSERT IGNORE INTO {table}({column}) VALUES (%s, %s)'
        try:
            with self.DB.cursor() as cur:
                cur.executemany(sql, values)
                self.DB.commit()
        except:
            import traceback
            traceback.print_exc()
            self.DB.rollback()

    def change_comment_df(self, df):
        """
        인자 : 댓글 데이터프레임

        데이터프레임 칼럼 체크하여 Comment 테이블의 칼럼과 일치하지 않을 경우 에러
        
        1. 유저 테이블에서 있는지 체크, id값 있을 경우 변환
        2. 신규 유저일 경우 유저 테이블에 추가, id값 가져오기 (DB에 유저 정보가 저장되어있다면 가져오기)
        3. url을 통해 코멘트 별 뉴스기사 id 가져오기 (select)
        """
        users = self.select('*', 'user')
        user_dict = {u[1]: u[0] for u in users}
        df['user_id'] = df['user_id'].map(user_dict)

        news = self.select('url,id', 'news')
        news_dict = {n[0]: n[1] for n in news}
        df['news_id'] = df['url'].map(news_dict)

        df.dropna(inplace=True)
        df['user_id'] = df['user_id'].astype(int)
        df['news_id'] = df['news_id'].astype(int)

        return df

    def select(self, column_qry:str, table:str):
        """
        셀렉트 all
        """
        sql_qr = "SELECT {0} FROM {1}".format(column_qry, table)

        with self.DB.cursor() as cur:
            cur.execute(sql_qr)
            return cur.fetchall()


    def insert_comment(self):
        """
        인자 : 댓글 데이터프레임

        데이터프레임 칼럼 체크하여 Comment 테이블의 칼럼과 일치하지 않을 경우 에러

        1. 댓글 id로 변환하는 함수 호출하여 변환한 데이터프레임 가져오기
        2. DB에 적재
        """
        # 기본적으로는 NOT NULL인 값들만 체크
        required_columns = ['comment', 'user_id', 'user_name', 'url', 'date_upload']
        assert not set(required_columns) - set(comment_df.columns), '테이블 칼럼이 부족합니다.'

        # 변환
        comment_df = self.change_comment_df(comment_df)

        target_column = ['news_id', 'date_upload', 'user_id', 'comment']
        self.DB.insert_many('comment',','.join(target_column), comment_df[target_column].values.tolist())

        table = 'COMMENT'
        column = ','.join(target_column)
        values = comment_df[target_column].values.tolist()
        sql = f'INSERT INTO {table}({column}) VALUES (%s, %s)'
        try:
            with self.DB.cursor() as cur:
                cur.executemany(sql, values)
                self.DB.commit()
            return True
        except:
            import traceback
            traceback.print_exc()
            self.DB.rollback()
            return False

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

        return self.select('*', 'USER')

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
        return self.select('*', 'COMMENT')

    def insert_many(self, table: str, columns: str, values: list) -> bool:
        """
        Insert Many Datas
        
        example)
        table = "Students"
        columns = "name, email, phone"
        values = [
            ('hong gildong', 'hgd123@gmail.com', '01012345678'),
            ...
        ]
        """
        sql = f"INSERT INTO {table}({columns}) " \
                  "VALUES ("  + ','.join(["%s"]*len(values[0])) + ");"
        try:
            with self.DB.cursor() as cur:
                cur.executemany(sql, values)
                self.DB.commit()
            return True
        except:
            import traceback
            traceback.print_exc()
            self.DB.rollback()
            return False

def read_config(config_path:str, splitter:str='=', encoding=None) -> dict:
    """
    config 파일 읽고 반환
    config_path = 파일 경로
    splitter = 구분 기호
    """
    temp = {}
    with open(config_path, 'r', encoding=encoding) as f:
        for l in f.readlines():
            k, v = l.rstrip().split(splitter)
            temp[k] = v
    return temp

if __name__ == '__main__':
    # 테스트코드 작성
    db2 = read_config('./db2.config')

    # 개인 데이터베이스에 연결
    news_db = NewsDB(db2)
        
    categories = [l.strip().split(',') for l in open('./category.csv').readlines()]
    categories[0][0] = '다음'    

    category_datas = []
    for category in categories:
        # 플랫폼
        if category[1].endswith('0000'):
            platform = category[0]
            category_datas.append(
                ('', '', platform, category[1])
            )
            continue
        elif category[1].endswith('00'):
            category_1 = category[0]
            category_datas.append(
                ('', category_1, platform, category[1])
            )
            continue
        else:
            category_2 = category[0]
            category_datas.append(
                (category_2, category_1, platform, category[1])
            )
    
    news_db.insert_many('CATEGORY', 'cat1_name,cat2_name,platform_name,cat2_id', category_datas)

    # insert 테스트 (뉴스, 코멘트)

    # select 테스트 (뉴스, 코멘트, 유저)

    pass
