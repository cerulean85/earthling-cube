from io import StringIO
import json, sys, os, re
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import application.common as cmn
from query import PipeTaskStatus, QueryPipeTaskClean, QueryPipeTaskSearch
from earthling.connector.s3_module import read_file_from_s3
from mecab import MeCab
import numpy as np
from collections import Counter

# Basic Korean stopwords list
STOPWORDS = {
    # Basic particles
    '이', '가', '을', '를', '에', '에서', '으로', '로', '와', '과', '의', '은', '는', '도', '만', '부터', '까지',
    # Basic endings
    '다', '요', '입니다', '습니다', '하다', '된다', '이다', '있다', '없다', '같다',
    # Basic pronouns
    '그', '이', '저', '그것', '이것', '저것', '여기', '거기', '저기', '누구', '무엇', '언제', '어디', '어떻게', '왜',
    # Basic numbers
    '하나', '둘', '셋', '한', '두', '세', '네', '다섯', '여섯', '일곱', '여덟', '아홉', '열',
    # Basic interjections
    '아', '어', '오', '우', '네', '예', '응', '음',
    # Basic dependent nouns
    '것', '수', '때', '곳', '데', '바', '뿐', '듯', '만큼', '정도',
    # Basic conjunctions/adverbs
    '그리고', '하지만', '그러나', '따라서', '그래서', '그런데', '또한', '및', '등', '또', '역시', '정말', '참', '매우', '아주', '너무',
    # Basic formal nouns
    '분', '번', '개', '명', '가지', '경우', '사람', '사실', '결과', '문제', '방법', '상황', '시간',
    # Basic high-frequency words
    '그냥', '진짜', '정말', '좀', '많이', '조금', '가끔', '보통', '원래', '처음', '나중', '다시', '계속',
    # Basic time expressions
    '오늘', '어제', '내일', '지금', '요즘', '최근', '전에', '후에', '동안', '사이'
}

# Domain-specific stopwords
DOMAIN_STOPWORDS = {
    'web': ['클릭', '링크', '사이트', '홈페이지', '블로그', '검색', '로그인'],
    'social': ['좋아요', '댓글', '공유', '팔로우', '구독', '알림'],
    'time': ['년', '월', '일', '시', '분', '초', '오늘', '어제', '내일']
}

# Basic regex patterns for filtering
BASIC_PATTERNS = [
    r'^[ㄱ-ㅎ]+$',  # Only consonants
    r'^[ㅏ-ㅣ]+$',  # Only vowels  
    r'^.{1}$',       # Single character
    r'^[0-9]+$',     # Only numbers
    r'^[~!@#$%^&*()_+`\-=\[\]{}|;:"<>,.?/]+$',  # Only special characters
    r'^(.{1,2})\1{2,}$',  # Repeated patterns (하하하)
    r'^[가-힣]*[ㅋㅎ]{3,}[가-힣]*$',  # Excessive ㅋㅎ
    r'^[가-힣]*[ㅠㅜ]{3,}[가-힣]*$',  # Excessive ㅠㅜ
]

class CleanApplication:
    def __init__(self):
        """Initialize with simplified stopword processing"""
        # Basic dynamic stopwords for runtime filtering
        self.dynamic_stopwords = set()
        self.frequency_stopwords = set()
        print("CleanApplication initialized with simplified processing")
    
    def detect_domain(self, text_list):
        """Simple domain detection based on keywords"""
        domain_keywords = {
            'web': ['블로그', '포스팅', '댓글', '링크'],
            'social': ['친구', '팔로우', '좋아요', '공유'],
            'news': ['기자', '뉴스', '보도', '취재'],
            'review': ['리뷰', '후기', '평점', '추천']
        }
        
        text_combined = ' '.join(text_list[:100])  # Sample first 100 texts
        
        for domain, keywords in domain_keywords.items():
            if any(keyword in text_combined for keyword in keywords):
                return domain
        
        return 'general'
    
    def is_meaningful_word(self, word, pos):
        """Simplified word meaningfulness check"""
        # Basic length check
        if len(word) < 2:
            return False
        
        # Basic stopwords check
        if word in STOPWORDS:
            return False
        
        # Dynamic stopwords check
        if word in (self.dynamic_stopwords | self.frequency_stopwords):
            return False
        
        # Basic regex patterns check
        for pattern in BASIC_PATTERNS:
            if re.search(pattern, word):
                return False
        
        # Korean characters only
        if not re.match(r'^[가-힣]+$', word):
            return False
        
        # Character diversity check (avoid repetitive patterns)
        unique_chars = len(set(word))
        total_chars = len(word)
        char_diversity = unique_chars / total_chars
        
        if char_diversity < 0.5 and total_chars > 2:
            return False
        
        return True
    
    def build_stopwords(self, word_list, min_freq=2, max_freq_ratio=0.3):
        """Simplified dynamic stopword building"""
        word_counts = Counter(word_list)
        total_words = len(word_list)
        
        print(f"Building dynamic stopwords from {len(set(word_list))} unique words...")
        
        # Frequency-based filtering
        for word, count in word_counts.items():
            freq_ratio = count / total_words
            if count < min_freq or freq_ratio > max_freq_ratio:
                self.frequency_stopwords.add(word)
        
        # Statistical outlier detection
        frequencies = list(word_counts.values())
        if len(frequencies) > 10:
            q75, q25 = np.percentile(frequencies, [75, 25])
            iqr = q75 - q25
            upper_bound = q75 + 1.5 * iqr
            lower_bound = q25 - 1.5 * iqr
            
            for word, count in word_counts.items():
                if count > upper_bound or count < lower_bound:
                    self.dynamic_stopwords.add(word)
        
        print(f"Generated {len(self.dynamic_stopwords)} dynamic stopwords")
        print(f"Generated {len(self.frequency_stopwords)} frequency-based stopwords")

    def execute(self, task):
        """Main execution method with simplified processing"""
        search_task_id = task["search_task_id"]
        query = QueryPipeTaskSearch()
        searched = query.get_task_by_id(search_task_id)
        origin_url = searched.s3_url

        query = QueryPipeTaskClean()
        task_no = task["id"]
        query.update_search_status_start_date_to_now(task_no)
        resource_origin = read_file_from_s3(origin_url)

        # Parse input data
        target_list = []
        resource_list = resource_origin.split("\n")
        for resource in resource_list:
            line = resource.split("\t")
            if len(line) < 2:
                continue
            
            title, text = line[0], line[2]
            target_text = f"{title} {text}"
            target_list.append(target_text)
        
        # Domain detection and domain-specific stopwords
        detected_domain = self.detect_domain(target_list)
        if detected_domain in DOMAIN_STOPWORDS:
            STOPWORDS.update(DOMAIN_STOPWORDS[detected_domain])
            print(f"Detected domain: {detected_domain}")
        
        # MeCab morphological analysis
        tagger = MeCab()
        target_pos = {"NNG", "NNP", "NNB", "VV", "VA"}
        
        # First pass: collect all words for dynamic stopword generation
        all_words = []
        temp_results = []
        for text in target_list:
            pos_list = tagger.pos(text)
            words_in_text = [word for word, pos in pos_list if pos in target_pos]
            all_words.extend(words_in_text)
            temp_results.append(pos_list)
        
        # Build dynamic stopwords
        self.build_stopwords(all_words)
        
        # Second pass: final filtering with all stopwords
        pos_filtered_list = []
        for pos_list in temp_results:
            filtered = [
                [word, pos] for word, pos in pos_list 
                if pos in target_pos and self.is_meaningful_word(word, pos)
            ]
            pos_filtered_list.append(filtered)
        
        # Convert to required format
        morph_data = [[list(pair) for pair in sublist] for sublist in pos_filtered_list]
        converted = cmn.convert_morph_to_json(morph_data)
        
        # Save to S3
        filename = cmn.get_save_filename(cmn.AppType.CLEAN)
        buffer = StringIO()
        json.dump(converted, buffer, ensure_ascii=False, indent=2)
        cmn.save_to_s3_and_update_with_buffer(query, task_no, filename, buffer)
        query.update_state_to_completed(task_no)

        # Update task statuses
        query.update_state_to_pending_about_analysis_task(PipeTaskStatus.FREQUENCY, task_no)
        query.update_state_to_pending_about_analysis_task(PipeTaskStatus.TFIDF, task_no)
        query.update_state_to_pending_about_analysis_task(PipeTaskStatus.CONCOR, task_no)

        print(f"Cleaning completed. Processed {len(target_list)} texts")

if __name__ == "__main__":
    app = CleanApplication()
    app.execute({"id": 1, "search_task_id": 3})
