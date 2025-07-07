#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DynamoDB 기반 설정 어댑터
기존 CleanApplication.py가 DynamoDB 설정을 사용하도록 하는 호환성 레이어
"""

# DynamoDB 설정 관리자 import
from .dynamodb_config_manager import (
    get_config_manager, 
    get_all_stopwords, 
    load_domain_stopwords
)

# --- 설정 변수 선언 ---
STOPWORDS = set()
DOMAIN_STOPWORDS = {}
MULTI_WORD_EXPRESSIONS = []
MORPHEME_PATTERNS = {}
COMPOUND_NOUN_PATTERNS = []
SEMANTIC_CLUSTERS = {}
REPEAT_PATTERNS = []
NGRAM_STOPWORDS = {}
MEANINGLESS_AFFIXES = {}
CONTEXT_STOPWORDS = {}
POS_MIN_LENGTH = {}

def refresh_all_configs():
    """모든 설정을 DynamoDB에서 다시 로드합니다."""
    global STOPWORDS, DOMAIN_STOPWORDS, MULTI_WORD_EXPRESSIONS, MORPHEME_PATTERNS, \
           COMPOUND_NOUN_PATTERNS, SEMANTIC_CLUSTERS, REPEAT_PATTERNS, NGRAM_STOPWORDS, \
           MEANINGLESS_AFFIXES, CONTEXT_STOPWORDS, POS_MIN_LENGTH

    # print("🔄 모든 설정을 DynamoDB에서 새로고침합니다...")
    
    config_manager = get_config_manager()
    config_manager._clear_cache()

    # 불용어 설정
    STOPWORDS.clear()
    STOPWORDS.update(get_all_stopwords(use_cache=False))

    # 도메인별 불용어
    DOMAIN_STOPWORDS.clear()
    domain_categories = ['blog', 'news', 'review', 'social', 'ecommerce']
    for domain in domain_categories:
        DOMAIN_STOPWORDS[domain] = load_domain_stopwords(domain, use_cache=False)

    # 복합명사 설정
    MULTI_WORD_EXPRESSIONS[:] = config_manager.get_compound_nouns('basic', use_cache=False)

    # 패턴 및 기타 설정들
    try:
        # 형태소 패턴
        response = config_manager.table.query(
            KeyConditionExpression='config_type = :type AND begins_with(config_key, :pattern_prefix)',
            ExpressionAttributeValues={':type': 'PATTERNS', ':pattern_prefix': 'morpheme_'}
        )
        MORPHEME_PATTERNS.clear()
        for item in response.get('Items', []):
            pattern_key = item['config_key'].replace('morpheme_', '')
            MORPHEME_PATTERNS[pattern_key] = item.get('patterns', [])
        
        # 복합명사 패턴
        pattern_response = config_manager.table.get_item(
            Key={'config_type': 'PATTERNS', 'config_key': 'compound_noun_patterns'}
        )
        COMPOUND_NOUN_PATTERNS[:] = pattern_response.get('Item', {}).get('patterns', [])

        # 의미론적 클러스터
        semantic_response = config_manager.table.query(
            KeyConditionExpression='config_type = :type AND begins_with(config_key, :semantic_prefix)',
            ExpressionAttributeValues={':type': 'STOPWORDS', ':semantic_prefix': 'semantic_'}
        )
        SEMANTIC_CLUSTERS.clear()
        for item in semantic_response.get('Items', []):
            cluster_key = item['config_key'].replace('semantic_', '')
            SEMANTIC_CLUSTERS[cluster_key] = item.get('words', [])

        # REPEAT_PATTERNS
        item = config_manager.table.get_item(Key={'config_type': 'PATTERNS', 'config_key': 'repeat_patterns'}).get('Item', {})
        REPEAT_PATTERNS[:] = item.get('patterns', [])

        # NGRAM_STOPWORDS
        ngram_response = config_manager.table.query(
            KeyConditionExpression='config_type = :type AND begins_with(config_key, :prefix)',
            ExpressionAttributeValues={':type': 'STOPWORDS', ':prefix': 'ngram_'}
        )
        NGRAM_STOPWORDS.clear()
        for item in ngram_response.get('Items', []):
            n_type = item['config_key'].replace('ngram_', '')
            NGRAM_STOPWORDS[n_type] = [tuple(w) for w in item.get('stopwords', [])]

        # MEANINGLESS_AFFIXES
        affix_response = config_manager.table.query(
            KeyConditionExpression='config_type = :type AND begins_with(config_key, :prefix)',
            ExpressionAttributeValues={':type': 'AFFIXES', ':prefix': 'meaningless_'}
        )
        MEANINGLESS_AFFIXES.clear()
        for item in affix_response.get('Items', []):
            a_type = item['config_key'].replace('meaningless_', '')
            MEANINGLESS_AFFIXES[a_type] = set(item.get('affixes', []))

        # CONTEXT_STOPWORDS
        context_response = config_manager.table.query(
            KeyConditionExpression='config_type = :type AND begins_with(config_key, :prefix)',
            ExpressionAttributeValues={':type': 'STOPWORDS', ':prefix': 'context_'}
        )
        CONTEXT_STOPWORDS.clear()
        for item in context_response.get('Items', []):
            verb = item['config_key'].replace('context_', '')
            CONTEXT_STOPWORDS[verb] = item.get('words', [])

        # POS_MIN_LENGTH
        item = config_manager.table.get_item(Key={'config_type': 'CONFIG', 'config_key': 'pos_min_length'}).get('Item', {})
        POS_MIN_LENGTH.clear()
        POS_MIN_LENGTH.update(item.get('settings', {}))

        # print("✅ 모든 정적 설정 DynamoDB에서 로드 성공")

    except Exception as e:
        # print(f"⚠️ 정적 설정 로드 실패, 기본값 사용: {e}")
        REPEAT_PATTERNS[:] = [
          r'^(.)\\1+$', r'^[ㅋㅎㅠㅜㅏㅓㅗㅜㅡㅣㅛㅕㅑㅒㅖ]+$', r'^\\d+$', r'^[a-zA-Z]+$', r'^[!@#$%^&*(),.?\":{}|<>]+$'
        ]
        NGRAM_STOPWORDS.clear()
        NGRAM_STOPWORDS.update({'bigram': [], 'trigram': []})
        MEANINGLESS_AFFIXES.clear()
        MEANINGLESS_AFFIXES.update({'prefix': set(), 'suffix': set(), 'infix': set()})
        CONTEXT_STOPWORDS.clear()
        POS_MIN_LENGTH.clear()
        POS_MIN_LENGTH.update({'Noun': 2, 'ProperNoun': 2, 'Verb': 2, 'Adjective': 2})

# 초기 설정 로드
refresh_all_configs()


# 복합명사 관리 함수들
def add_multi_word_expression(expression):
    """복합명사 표현 추가"""
    config_manager = get_config_manager()
    return config_manager.add_compound_nouns('basic', [expression])

def remove_multi_word_expression(expression):
    """복합명사 표현 제거"""
    config_manager = get_config_manager()
    try:
        # 기존 표현들 조회
        current_expressions = config_manager.get_compound_nouns('basic', use_cache=False)
        if expression in current_expressions:
            updated_expressions = [exp for exp in current_expressions if exp != expression]
            
            # DynamoDB 업데이트
            config_manager.table.update_item(
                Key={
                    'config_type': 'COMPOUND_NOUNS',
                    'config_key': 'basic'
                },
                UpdateExpression='SET expressions = :expressions, updated_at = :timestamp',
                ExpressionAttributeValues={
                    ':expressions': updated_expressions,
                    ':timestamp': config_manager.datetime.now().isoformat()
                }
            )
            
            # 캐시 무효화
            config_manager._clear_cache('compound_nouns_basic')
            MULTI_WORD_EXPRESSIONS[:] = updated_expressions
            return True
        return False
    except Exception as e:
        # print(f"❌ 복합명사 제거 실패: {e}")
        return False

def validate_compound_expression(expression):
    """복합명사 표현 검증"""
    if not expression or not isinstance(expression, str):
        return False, "표현이 비어있거나 문자열이 아닙니다."
    
    if ' ' not in expression:
        return False, "띄어쓰기가 없습니다."
    
    words = expression.split()
    if len(words) < 2:
        return False, "최소 2개 단어가 필요합니다."
    
    if any(len(word) < 2 for word in words):
        return False, "각 단어는 최소 2글자 이상이어야 합니다."
    
    return True, "유효한 복합명사입니다."

def export_compound_summary():
    """복합명사 설정 요약"""
    config_manager = get_config_manager()
    try:
        compound_nouns = config_manager.get_compound_nouns('basic')
        return {
            'version': '1.0.0',
            'last_updated': config_manager.datetime.now().strftime('%Y-%m-%d'),
            'multi_word_expressions_count': len(compound_nouns),
            'compound_patterns_count': len(COMPOUND_NOUN_PATTERNS),
            'separator_token': '_',
            'processing_enabled': True
        }
    except Exception as e:
        return {'error': str(e)}

# print(f"📊 DynamoDB에서 로드된 설정:")
# print(f"   - 불용어: {len(STOPWORDS)}개")
# print(f"   - 도메인 카테고리: {len(DOMAIN_STOPWORDS)}개")
# print(f"   - 복합명사: {len(MULTI_WORD_EXPRESSIONS)}개")
# print(f"   - 형태소 패턴: {len(MORPHEME_PATTERNS)}개")
# print(f"   - 의미론적 클러스터: {len(SEMANTIC_CLUSTERS)}개")