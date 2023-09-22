# DB4_6
## ERD
![image](https://github.com/sesac-2023/DB4_6/assets/124233972/a1a1edee-bac6-4c80-b6b7-94978937b639)

## 컨벤션
### Code Convention

#### SQL
원형 사용. 복수형 등 사용 금지  

- 테이블명: `SNAKE_CASE`
- 열이름: `snake_case`
- 주키(단일): `id`
- 주키(복합): `id_column`
- 외래키: `table_id`
  
#### 소주제_id 지정
![image](https://github.com/sesac-2023/DB4_6/assets/124233972/457eada9-35a8-4666-84f3-9d03eac45696)
![image](https://github.com/sesac-2023/DB4_6/assets/124233972/9dd35c72-9e28-4e5d-a618-ba8547828f76)

#### python
- 클래스: `PascalCase`
- 변수명: `snake_case`
- 함수명: `snake_case`
- 상수: `SNAKE_CASE`

### folder and files Convention
가능하면 영문 소문자로만 구성한다.  
가능하면 짧게 구성한다(축약어 사용).  
특수문자와 빈 공백sᴘᴀᴄᴇ은 사용하지 않는다.  
단어와 단어의 구분은 ‘_’(ᴜɴᴅᴇʀʙᴀʀ) 로 구성한다.  

### Git Convention

기본 브랜치: `main`

<aside>
💡 `COMMIT_TYPE: COMMIT_SUMMARY`

</aside>

- **COMMIT_TYPE**
    - feat : 새로운 기능 추가
    - fix : 버그 수정
    - docs : 문서 추가 및 수정
    - style : 코드 포맷팅, 세미콜론 누락, 오타 수정 등
    - test : 테스트코드
    - refactor : 코드 리팩토링
    - chore : 빌드 업무 수정, 패키지 매니저 수정
- **COMMIT_SUMMARY**
    - 영어로 작성
    - 마침표를 붙이지 않음
    - 50자를 넘기지 않음

- **BRANCH_NAME**  
미정  
  
### etc
subject: 과거형은 쓰지 않는다. fixed -> fix  
type: 대문자는 쓰지 않는다.  
body: 부연설명이 필요할 시 작성  
footer: 선택사항. issue tracker id를 작성할때 사용.  

### References
https://jjam89.tistory.com/284  
https://devsosin.notion.site/1a4f6e2b5d0c44719acdacae79028244  
https://bestinu.tistory.com/62  
