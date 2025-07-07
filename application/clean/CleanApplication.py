from io import StringIO
import json, sys, os, re
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import application.common as cmn
from query import PipeTaskStatus, QueryPipeTaskClean, QueryPipeTaskSearch
from earthling.connector.s3_module import read_file_from_s3
import numpy as np
from collections import Counter
from collections import defaultdict
from application.clean.config_adapter import (
    DOMAIN_STOPWORDS, MORPHEME_PATTERNS, REPEAT_PATTERNS,
    SEMANTIC_CLUSTERS, NGRAM_STOPWORDS, MEANINGLESS_AFFIXES, 
    CONTEXT_STOPWORDS, POS_MIN_LENGTH, MULTI_WORD_EXPRESSIONS, STOPWORDS, 
    load_domain_stopwords, refresh_all_configs
)

from konlpy.tag import Okt
# print("✅ KoNLPy Okt 형태소 분석기 로드 성공")
MORPHEME_ANALYZER_AVAILABLE = True

class OktAnalyzer:
    def __init__(self):
        self.tagger = Okt()
    
    def pos(self, text):
        # Okt 품사 태깅 결과 반환
        return self.tagger.pos(text)

morpheme_analyzer = OktAnalyzer()
    
class CleanApplication:
    def __init__(self):
        # 동적 불용어
        self.dynamic_stopwords = set()
        self.semantic_stopwords = set()
        self.frequency_stopwords = set()
        self.context_stopwords = set()
        
        # 통계 정보
        self.word_stats = {}
        self.pos_stats = {}        
        
    def get_all_active_stopwords(self):
        """현재 활성화된 모든 불용어 반환"""
        base_stopwords = STOPWORDS
        all_active = base_stopwords.copy()
        all_active.update(self.dynamic_stopwords)
        all_active.update(self.frequency_stopwords)
        all_active.update(self.context_stopwords)
        all_active.update(self.semantic_stopwords)
        return all_active  
    
    def detect_domain(self, text_list):
        """텍스트 도메인 자동 감지"""
        domain_scores = {domain: 0 for domain in DOMAIN_STOPWORDS.keys()}
        
        for text in text_list[:100]:  # 샘플링
            text_lower = text.lower()
            for domain, keywords in DOMAIN_STOPWORDS.items():
                for keyword in keywords:
                    if keyword in text_lower:
                        domain_scores[domain] += 1
        
        return max(domain_scores.items(), key=lambda x: x[1])[0] if max(domain_scores.values()) > 0 else 'general'

    def apply_semantic_clustering(self, word_list):
        """의미론적 클러스터 기반 불용어 추출"""
        # print("🧠 의미론적 클러스터 분석 중...")
        
        # 각 클러스터별로 단어 빈도 확인
        for cluster_name, cluster_words in SEMANTIC_CLUSTERS.items():
            cluster_count = sum(1 for word in word_list if word in cluster_words)
            cluster_ratio = cluster_count / len(word_list) if word_list else 0
            
            # 클러스터 단어가 너무 많으면 불용어로 추가
            if cluster_ratio > 0.02:  # 전체의 2% 이상
                self.semantic_stopwords.update(cluster_words)
    
    def apply_ngram_analysis(self, word_list):
        """N-gram 패턴 기반 불용어 분석"""
        # print("📊 N-gram 패턴 분석 중...")
        
        # Bi-gram 분석
        bigrams = [(word_list[i], word_list[i+1]) for i in range(len(word_list)-1)]
        bigram_counts = {}
        for bigram in bigrams:
            bigram_counts[bigram] = bigram_counts.get(bigram, 0) + 1
        
        # NGRAM_STOPWORDS에 정의된 패턴과 매칭되는 단어들 제거
        for bigram_tuple in NGRAM_STOPWORDS.get('bigram', []):
            if bigram_tuple in bigram_counts and bigram_counts[bigram_tuple] > 3:
                self.semantic_stopwords.update(bigram_tuple)
                # print(f"   🎯 고빈도 Bi-gram: {' '.join(bigram_tuple)}")
        
        # Tri-gram 분석
        trigrams = [(word_list[i], word_list[i+1], word_list[i+2]) for i in range(len(word_list)-2)]
        trigram_counts = {}
        for trigram in trigrams:
            trigram_counts[trigram] = trigram_counts.get(trigram, 0) + 1
        
        for trigram_tuple in NGRAM_STOPWORDS.get('trigram', []):
            if trigram_tuple in trigram_counts and trigram_counts[trigram_tuple] > 2:
                self.semantic_stopwords.update(trigram_tuple)
                # print(f"   🎯 고빈도 Tri-gram: {' '.join(trigram_tuple)}")
    
    def extract_contextual_stopwords(self, word_list):
        """문맥 기반 동적 불용어 추출"""
        # 동사 활용형 패턴 감지
        verb_patterns = defaultdict(set)
        for word in word_list:
            for base_verb, variations in CONTEXT_STOPWORDS.items():
                if any(word.startswith(var[:2]) for var in variations):
                    verb_patterns[base_verb].add(word)
        
        # 너무 많은 활용형이 있는 동사는 불용어로 추가
        for base_verb, variations in verb_patterns.items():
            if len(variations) > 5:                
                self.context_stopwords.update(variations)        

    def build_advanced_stopwords(self, word_list, min_freq=2, max_freq_ratio=0.6):
        """모든 패턴을 활용한 고급 불용어 생성"""
        from collections import Counter
        
        word_counts = Counter(word_list)
        total_words = len(word_list)
        
        print(f"🚀 고급 불용어 분석 시작: 전체 {total_words}개 단어, 고유 {len(word_counts)}개")
        
        # 1. 기본 빈도 기반 불용어
        for word, count in word_counts.items():
            freq_ratio = count / total_words
            if count < min_freq:
                self.frequency_stopwords.add(word)
            elif freq_ratio > max_freq_ratio:
                self.frequency_stopwords.add(word)
        
        # 2. 문맥 기반 불용어 (CONTEXT_STOPWORDS 활용)
        self.extract_contextual_stopwords(word_list)
        
        # 3. 의미론적 클러스터 분석 (SEMANTIC_CLUSTERS 활용)
        self.apply_semantic_clustering(word_list)
        
        # 4. N-gram 패턴 분석 (NGRAM_STOPWORDS 활용)
        self.apply_ngram_analysis(word_list)                  
    
    def apply_compound_aware_filtering(self, word, pos):
        """합성어와 복합 명사를 고려한 스마트 필터링"""
        # 기본 길이 체크
        min_length = POS_MIN_LENGTH.get(pos, 2)
        if len(word) < min_length:
            return False
        
        # 기본 불용어 체크 (모든 활성 불용어 포함)
        active_stopwords = self.get_all_active_stopwords()
        if word in active_stopwords:
            return False
        
        # 복합 명사인 경우 보호 (띄어쓰기 포함)
        if ' ' in word:
            # 복합 명사는 기본적으로 보존
            return True
        
        # 미리 정의된 복합 명사 리스트에 있는 경우 보호
        for compound in MULTI_WORD_EXPRESSIONS:
            if word == compound.replace(' ', ''):  # 공백 제거 후 비교
                return True    
        
        for pattern in REPEAT_PATTERNS:
            if re.search(pattern, word):
                return False
        
        # 형태소 기반 패턴 체크 (MORPHEME_PATTERNS)
        for pattern_type, patterns in MORPHEME_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, word):
                    return False
        
        # 6. 의미없는 접두어/접미어 체크 (MEANINGLESS_AFFIXES)
        for prefix in MEANINGLESS_AFFIXES.get('prefix', []):
            if word.startswith(prefix) and len(word) > len(prefix):
                return False
        for suffix in MEANINGLESS_AFFIXES.get('suffix', []):
            if word.endswith(suffix) and len(word) > len(suffix):
                return False
        for infix in MEANINGLESS_AFFIXES.get('infix', []):
            if infix in word and len(word) > len(infix):
                return False
        
        # 한글만 허용
        if not re.match(r'^[가-힣]+$', word):
            return False
        
        # 문자 다양성 체크 (합성어가 아닌 경우 더 엄격하게)
        if len(word) > 2:
            unique_chars = len(set(word))
            total_chars = len(word)
            if unique_chars / total_chars < 0.4:  # 합성어가 아니면 더 엄격
                return False
        
        return True

    def preprocess_compound_nouns(self, text):
        """
        텍스트에서 복합 명사를 식별하고 하나의 토큰으로 연결
        '코카 콜라' → '코카_콜라'로 변환하여 형태소 분석에서 하나의 단어로 처리
        """
        processed_text = text
        found_compounds = []
        
        # 1. 미리 정의된 복합 명사 리스트 적용 (단순 문자열 매칭)
        for compound in MULTI_WORD_EXPRESSIONS:
            if compound in text:
                compound_token = compound.replace(' ', '_')
                processed_text = processed_text.replace(compound, compound_token)
                found_compounds.append(compound)
        
        # 2. 패턴 기반 복합 명사 탐지 및 연결 (기본 패턴만)
        import re
        basic_pattern = r'([가-힣]{2,4})\\s([가-힣]{2,4})'
        matches = re.finditer(basic_pattern, processed_text)
        for match in matches:
            original = match.group()
            # 이미 처리된 복합 명사와 중복되지 않는지 확인
            if not any(compound in original for compound in found_compounds):
                compound_token = original.replace(' ', '_')
                processed_text = processed_text.replace(original, compound_token, 1)
                found_compounds.append(original)
        
        return processed_text
    
    def postprocess_compound_tokens(self, tokens):
        """
        형태소 분석 후 복합 명사 토큰을 원래 형태로 복원
        '코카_콜라' → '코카 콜라'
        """
        processed_tokens = []
        for token, pos in tokens:
            if '_' in token:
                # 언더스코어를 다시 띄어쓰기로 변환
                restored_token = token.replace('_', ' ')
                processed_tokens.append([restored_token, pos])
            else:
                processed_tokens.append([token, pos])
        return processed_tokens

    def execute(self, task):
        # print("🔄 설정 새로고침 시도...")
        refresh_all_configs()
        # print("✅ 설정 새로고침 완료.")

        # print("\n--- 현재 적용된 동적 설정 ---")
        # print(f"  - 반복 패턴 (REPEAT_PATTERNS): {len(REPEAT_PATTERNS)}개")
        # print(f"  - N-gram 불용어 (NGRAM_STOPWORDS): bigrams={len(NGRAM_STOPWORDS.get('bigram', []))}, trigrams={len(NGRAM_STOPWORDS.get('trigram', []))}")
        # print(f"  - 무의미 접사 (MEANINGLESS_AFFIXES): prefixes={len(MEANINGLESS_AFFIXES.get('prefix', []))}, suffixes={len(MEANINGLESS_AFFIXES.get('suffix', []))}, infixes={len(MEANINGLESS_AFFIXES.get('infix', []))}")
        # print(f"  - 문맥 불용어 (CONTEXT_STOPWORDS): {len(CONTEXT_STOPWORDS)}개")
        # print(f"  - 품사별 최소 길이 (POS_MIN_LENGTH): {POS_MIN_LENGTH}")
        # print("--------------------------\n")
          
        search_task_id = task["search_task_id"]
        query = QueryPipeTaskSearch()
        searched = query.get_task_by_id(search_task_id)
        origin_url = searched.s3_url

        query = QueryPipeTaskClean()
        task_no = task["id"]
        query.update_search_status_start_date_to_now(task_no)
        resource_origin = read_file_from_s3(origin_url)

        target_list = []
        resource_list = resource_origin.split("\n")
        for resource in resource_list:
            line = resource.split("\t")            
            if len(line) < 2:
                continue
            title, text = line[0], line[2]
            target_text = f"{title} {text}"
            target_list.append(target_text)
        
        # 도메인 자동 감지 및 도메인별 불용어 추가 (외부 설정 파일 사용)
        detected_domain = self.detect_domain(target_list)
        domain_stopwords = load_domain_stopwords(detected_domain)
        # if domain_stopwords:
        #     print(f"🏷️ 감지된 도메인: {detected_domain} (불용어 {len(domain_stopwords)}개 추가)")
        
        target_pos = {"Noun", "ProperNoun", "Verb", "Adjective"}  # Okt 품사 태그
        
        # 1차: 복합 명사 전처리 및 전체 단어 수집 (동적 불용어 생성용)
        all_words = []
        temp_results = []
        
        for text in target_list:            # 복합 명사 전처리: '코카 콜라' → '코카_콜라'
            preprocessed_text = self.preprocess_compound_nouns(text)
            pos_list = morpheme_analyzer.pos(preprocessed_text)
            # 복합 명사 토큰 후처리: '코카_콜라' → '코카 콜라'
            pos_list = self.postprocess_compound_tokens(pos_list)            
            words_in_text = [word for word, pos in pos_list if pos in target_pos]
            all_words.extend(words_in_text)
            temp_results.append(pos_list)
        
        # 간단한 동적 불용어 생성 → 고급 불용어 생성으로 변경
        self.build_advanced_stopwords(all_words)        
        
        # 2차: 최종 필터링 (모든 고급 패턴 적용)
        pos_filited_list = []
        total_words = 0
        filtered_words = 0
        
        for pos_list in temp_results:
            filtered = []
            for word, pos in pos_list:
                total_words += 1
                if pos in target_pos and self.apply_compound_aware_filtering(word, pos):
                    filtered.append([word, pos])
                    filtered_words += 1
            pos_filited_list.append(filtered)
        
        # print(f"🔍 고급 필터링 결과: {total_words}개 → {filtered_words}개 (제거율: {(1-filtered_words/total_words)*100:.1f}%)")
        morph_data = [[list(pair) for pair in sublist] for sublist in pos_filited_list]
        
        converted = cmn.convert_morph_to_json(morph_data)
        # for cvt in converted:
        #     print(cvt)

        # 샘플 결과 출력 (처음 3개 문서의 단어들)
        # for i, doc in enumerate(morph_data[:3]):
        #   if doc:  # 비어있지 않은 경우만
        #       words = [pair[0] for pair in doc]
              # print(f"   문서 {i+1}: {', '.join(words[:10])}{'...' if len(words) > 10 else ''}")
        
        # 주석 해제하여 실제 저장
        filename = cmn.get_save_filename(cmn.AppType.CLEAN)
        buffer = StringIO()
        json.dump(converted, buffer, ensure_ascii=False, indent=2)
        cmn.save_to_s3_and_update_with_buffer(query, task_no, filename, buffer)
        query.update_state_to_completed(task_no)

        query.update_state_to_pending_about_analysis_task(PipeTaskStatus.FREQUENCY, task_no)
        query.update_state_to_pending_about_analysis_task(PipeTaskStatus.TFIDF, task_no)
        query.update_state_to_pending_about_analysis_task(PipeTaskStatus.CONCOR, task_no)

if __name__ == "__main__":
    app = CleanApplication()
    app.execute({"id": 26, "search_task_id": 64})