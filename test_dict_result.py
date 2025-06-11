#!/usr/bin/env python3
import sys
import os
sys.path.append('earthling')

# BaseDAO를 직접 테스트
from earthling.handler.dao.NaverDAO import NaverDAO

def test_dict_result():
    print("=== 딕셔너리 반환 테스트 ===")
    
    try:
        # NaverDAO 인스턴스 생성
        dao = NaverDAO()
        
        # 간단한 테스트 쿼리 실행
        print("1. 테스트 쿼리 실행 중...")
        query = "SELECT 1 as test_id, 'hello' as test_name, 'world' as test_value"
        result = dao.exec(query)
        
        print(f"2. 결과 타입: {type(result)}")
        print(f"3. 결과 내용: {result}")
        
        if result and len(result) > 0:
            first_row = result[0]
            print(f"4. 첫 번째 행 타입: {type(first_row)}")
            print(f"5. 첫 번째 행 내용: {first_row}")
            
            if isinstance(first_row, dict):
                print("✅ 성공! 딕셔너리 형태로 반환됨")
                print(f"   - test_id: {first_row['test_id']}")
                print(f"   - test_name: {first_row['test_name']}")  
                print(f"   - test_value: {first_row['test_value']}")
            else:
                print("❌ 실패! 여전히 튜플 형태로 반환됨")
                print(f"   - 첫 번째 값: {first_row[0]}")
                print(f"   - 두 번째 값: {first_row[1]}")
                print(f"   - 세 번째 값: {first_row[2]}")
        else:
            print("❌ 결과가 비어있거나 오류 발생")
            
    except Exception as e:
        print(f"❌ 오류 발생: {e}")

if __name__ == "__main__":
    test_dict_result() 