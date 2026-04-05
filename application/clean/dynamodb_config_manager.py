#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DynamoDB 기반 NLP 설정 관리 시스템

이 모듈은 불용어, 복합명사, 패턴 등의 설정을 DynamoDB에서 관리합니다.
기존의 하드코딩된 설정을 완전히 동적인 클라우드 기반 시스템으로 전환합니다.
"""

import boto3
import json
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Set, Any, Optional, Tuple
from botocore.exceptions import ClientError
import os


class DynamoDBConfigManager:
    """DynamoDB 기반 NLP 설정 관리자"""

    def __init__(
        self,
        table_name: str = "stock-dev-nlp",
        region_name: str = "ap-northeast-2",
        cache_duration: int = 300,
    ):  # 5분 캐시
        """
        DynamoDB 설정 관리자 초기화

        Args:
            table_name: DynamoDB 테이블 이름
            region_name: AWS 리전
            cache_duration: 로컬 캐시 유지 시간 (초)
        """
        self.table_name = table_name
        self.region_name = region_name
        self.cache_duration = cache_duration

        # DynamoDB 클라이언트 초기화
        try:
            self.dynamodb = boto3.resource("dynamodb", region_name=region_name)
            response = self.dynamodb.meta.client.list_tables()
            table_names = response["TableNames"]
            # print(table_names)
            if self.table_name in table_names:
                # print("✅ 테이블이 존재합니다.")
                self.table = self.dynamodb.Table(table_name)
                self._table_exists = True
                # print(f"✅ DynamoDB 테이블 '{table_name}' 연결 성공")
            else:
                # print("❌ 테이블이 존재하지 않습니다.")
                self._table_exists = False
        except Exception as e:
            print(f"❌ DynamoDB 연결 실패: {e}")
            self._table_exists = False

        self.create_table_if_not_exists()

        # 로컬 캐시
        self._cache = {}
        self._cache_timestamps = {}

        # 설정 타입 정의
        self.CONFIG_TYPES = {
            "STOPWORDS": "stopwords",
            "COMPOUND_NOUNS": "compound_nouns",
            "PATTERNS": "patterns",
            "SEMANTIC_CLUSTERS": "semantic_clusters",
            "DOMAIN_STOPWORDS": "domain_stopwords",
        }

    def create_table_if_not_exists(self):
        """DynamoDB 테이블이 없으면 생성"""
        if not self._table_exists:
            # print("Table이 없어서 생성중...")
            try:
                # 테이블 스키마 정의
                table_schema = {
                    "TableName": self.table_name,
                    "KeySchema": [
                        {
                            "AttributeName": "config_type",
                            "KeyType": "HASH",
                        },  # Partition Key
                        {"AttributeName": "config_key", "KeyType": "RANGE"},  # Sort Key
                    ],
                    "AttributeDefinitions": [
                        {"AttributeName": "config_type", "AttributeType": "S"},
                        {"AttributeName": "config_key", "AttributeType": "S"},
                        {"AttributeName": "category", "AttributeType": "S"},
                        {"AttributeName": "priority", "AttributeType": "N"},
                        {"AttributeName": "active", "AttributeType": "S"},
                    ],
                    "GlobalSecondaryIndexes": [
                        {
                            "IndexName": "category-priority-index",
                            "KeySchema": [
                                {"AttributeName": "category", "KeyType": "HASH"},
                                {"AttributeName": "priority", "KeyType": "RANGE"},
                            ],
                            "Projection": {"ProjectionType": "ALL"},
                        },
                        {
                            "IndexName": "active-type-index",
                            "KeySchema": [
                                {"AttributeName": "active", "KeyType": "HASH"},
                                {"AttributeName": "config_type", "KeyType": "RANGE"},
                            ],
                            "Projection": {"ProjectionType": "ALL"},
                        },
                    ],
                    "Tags": [
                        {"Key": "Environment", "Value": "production"},
                        {"Key": "Service", "Value": "nlp-config"},
                        {"Key": "CreatedBy", "Value": "DynamoDBConfigManager"},
                    ],
                }

                # print(f"🔨 DynamoDB 테이블 '{self.table_name}' 생성 중...")
                table = self.dynamodb.create_table(
                    **table_schema,
                    BillingMode="PAY_PER_REQUEST",
                )
                table.wait_until_exists()

                self.table = table
                self._table_exists = True
                # print(f"✅ DynamoDB 테이블 '{self.table_name}' 생성 완료")

            except Exception as e:
                print(f"❌ DynamoDB 테이블 생성 실패: {e}")
                raise

    def _is_cache_valid(self, cache_key: str) -> bool:
        """캐시 유효성 확인"""
        if cache_key not in self._cache_timestamps:
            return False

        timestamp = self._cache_timestamps[cache_key]
        return (datetime.now() - timestamp).seconds < self.cache_duration

    def _get_from_cache(self, cache_key: str) -> Optional[Any]:
        """캐시에서 데이터 조회"""
        if self._is_cache_valid(cache_key):
            return self._cache.get(cache_key)
        return None

    def _set_cache(self, cache_key: str, data: Any):
        """캐시에 데이터 저장"""
        self._cache[cache_key] = data
        self._cache_timestamps[cache_key] = datetime.now()

    def _clear_cache(self, cache_key: str = None):
        """캐시 삭제"""
        if cache_key:
            self._cache.pop(cache_key, None)
            self._cache_timestamps.pop(cache_key, None)
        else:
            self._cache.clear()
            self._cache_timestamps.clear()

    def get_stopwords(
        self, category: str = "basic", use_cache: bool = True
    ) -> Set[str]:
        """불용어 조회"""
        cache_key = f"stopwords_{category}"

        # 캐시 확인
        if use_cache:
            cached_data = self._get_from_cache(cache_key)
            if cached_data is not None:
                return cached_data

        try:
            # DynamoDB에서 조회
            response = self.table.get_item(
                Key={"config_type": "STOPWORDS", "config_key": category}
            )

            if "Item" in response:
                item = response["Item"]
                if item.get("active", True):
                    stopwords = set(item.get("words", []))
                    self._set_cache(cache_key, stopwords)
                    return stopwords

            # print(f"⚠️ 불용어 카테고리 '{category}'를 찾을 수 없습니다.")
            return set()

        except Exception as e:
            print(f"❌ 불용어 조회 실패: {e}")
            return self._get_fallback_stopwords(category)

    def get_all_stopwords(self, use_cache: bool = True) -> Set[str]:
        """모든 활성 불용어 조회"""
        cache_key = "all_stopwords"

        if use_cache:
            cached_data = self._get_from_cache(cache_key)
            if cached_data is not None:
                return cached_data

        try:
            # 활성 불용어 모두 조회
            response = self.table.query(
                IndexName="active-type-index",
                KeyConditionExpression="active = :active AND config_type = :type",
                ExpressionAttributeValues={":active": "true", ":type": "STOPWORDS"},
            )

            all_stopwords = set()
            for item in response.get("Items", []):
                words = item.get("words", [])
                all_stopwords.update(words)

            self._set_cache(cache_key, all_stopwords)
            return all_stopwords

        except Exception as e:
            print(f"❌ 전체 불용어 조회 실패: {e}")
            return self._get_fallback_stopwords("basic")

    def get_compound_nouns(
        self, category: str = "basic", use_cache: bool = True
    ) -> List[str]:
        """복합명사 조회"""
        cache_key = f"compound_nouns_{category}"

        if use_cache:
            cached_data = self._get_from_cache(cache_key)
            if cached_data is not None:
                return cached_data

        try:
            response = self.table.get_item(
                Key={"config_type": "COMPOUND_NOUNS", "config_key": category}
            )

            if "Item" in response:
                item = response["Item"]
                if item.get("active", True):
                    expressions = item.get("expressions", [])
                    self._set_cache(cache_key, expressions)
                    return expressions

            return []

        except Exception as e:
            print(f"❌ 복합명사 조회 실패: {e}")
            return self._get_fallback_compound_nouns()

    def add_stopwords(
        self, category: str, words: List[str], description: str = "", priority: int = 1
    ) -> bool:
        """불용어 추가"""
        try:
            # 기존 불용어 조회
            existing_words = self.get_stopwords(category, use_cache=False)

            # 새 단어 추가
            updated_words = list(existing_words.union(set(words)))

            # DynamoDB에 저장
            item = {
                "config_type": "STOPWORDS",
                "config_key": category,
                "category": category,
                "words": updated_words,
                "description": description,
                "priority": priority,
                "active": "true",
                "updated_at": datetime.now().isoformat(),
                "updated_by": "system",
                "version": self._generate_version_hash(updated_words),
            }

            self.table.put_item(Item=item)

            # 캐시 무효화
            self._clear_cache(f"stopwords_{category}")
            self._clear_cache("all_stopwords")

            # print(f"✅ 불용어 {len(words)}개를 '{category}' 카테고리에 추가했습니다.")
            return True

        except Exception as e:
            print(f"❌ 불용어 추가 실패: {e}")
            return False

    def remove_stopwords(self, category: str, words: List[str]) -> bool:
        """불용어 제거"""
        try:
            # 기존 불용어 조회
            existing_words = self.get_stopwords(category, use_cache=False)

            # 단어 제거
            updated_words = list(existing_words - set(words))

            # DynamoDB 업데이트
            self.table.update_item(
                Key={"config_type": "STOPWORDS", "config_key": category},
                UpdateExpression="SET words = :words, updated_at = :timestamp, version = :version",
                ExpressionAttributeValues={
                    ":words": updated_words,
                    ":timestamp": datetime.now().isoformat(),
                    ":version": self._generate_version_hash(updated_words),
                },
            )

            # 캐시 무효화
            self._clear_cache(f"stopwords_{category}")
            self._clear_cache("all_stopwords")

            # print(f"✅ 불용어 {len(words)}개를 '{category}' 카테고리에서 제거했습니다.")
            return True

        except Exception as e:
            print(f"❌ 불용어 제거 실패: {e}")
            return False

    def add_compound_nouns(
        self,
        category: str,
        expressions: List[str],
        description: str = "",
        priority: int = 1,
    ) -> bool:
        """복합명사 추가"""
        try:
            # 기존 복합명사 조회
            existing_expressions = self.get_compound_nouns(category, use_cache=False)

            # 새 표현 추가
            updated_expressions = list(set(existing_expressions + expressions))

            # DynamoDB에 저장
            item = {
                "config_type": "COMPOUND_NOUNS",
                "config_key": category,
                "category": category,
                "expressions": updated_expressions,
                "description": description,
                "priority": priority,
                "active": "true",
                "updated_at": datetime.now().isoformat(),
                "updated_by": "system",
                "version": self._generate_version_hash(updated_expressions),
            }

            self.table.put_item(Item=item)

            # 캐시 무효화
            self._clear_cache(f"compound_nouns_{category}")

            # print(f"✅ 복합명사 {len(expressions)}개를 '{category}' 카테고리에 추가했습니다.")
            return True

        except Exception as e:
            print(f"❌ 복합명사 추가 실패: {e}")
            return False

    def get_config_summary(self) -> Dict[str, Any]:
        """전체 설정 요약 조회"""
        try:
            # 모든 설정 아이템 조회
            response = self.table.scan(
                FilterExpression="active = :active",
                ExpressionAttributeValues={":active": "true"},
            )

            summary = {
                "total_configs": len(response.get("Items", [])),
                "by_type": {},
                "by_category": {},
                "last_updated": None,
                "cache_status": {
                    "cached_items": len(self._cache),
                    "cache_duration": self.cache_duration,
                },
            }

            latest_update = None

            for item in response.get("Items", []):
                config_type = item["config_type"]
                category = item.get("category", "unknown")
                updated_at = item.get("updated_at")

                # 타입별 집계
                if config_type not in summary["by_type"]:
                    summary["by_type"][config_type] = 0
                summary["by_type"][config_type] += 1

                # 카테고리별 집계
                if category not in summary["by_category"]:
                    summary["by_category"][category] = 0
                summary["by_category"][category] += 1

                # 최신 업데이트 시간
                if updated_at and (not latest_update or updated_at > latest_update):
                    latest_update = updated_at

            summary["last_updated"] = latest_update
            return summary

        except Exception as e:
            print(f"❌ 설정 요약 조회 실패: {e}")
            return {"error": str(e)}

    def _generate_version_hash(self, data: Any) -> str:
        """데이터의 버전 해시 생성"""
        data_str = json.dumps(data, sort_keys=True, ensure_ascii=False)
        return hashlib.md5(data_str.encode()).hexdigest()[:8]

    def _get_fallback_stopwords(self, category: str) -> Set[str]:
        """DynamoDB 실패 시 fallback 불용어"""
        fallback_stopwords = {
            "이",
            "가",
            "을",
            "를",
            "에",
            "의",
            "은",
            "는",
            "도",
            "만",
            "그",
            "이",
            "저",
            "것",
            "수",
            "때",
            "곳",
            "데",
            "바",
            "하다",
            "되다",
            "있다",
            "없다",
            "같다",
            "그리고",
            "하지만",
        }
        print(f"⚠️ DynamoDB 실패로 fallback 불용어 사용: {len(fallback_stopwords)}개")
        return fallback_stopwords

    def _get_fallback_compound_nouns(self) -> List[str]:
        """DynamoDB 실패 시 fallback 복합명사"""
        fallback_compounds = [
            "코카 콜라",
            "삼성 갤럭시",
            "스타벅스 커피",
            "맥도날드 햄버거",
            "아이폰 프로",
        ]
        print(f"⚠️ DynamoDB 실패로 fallback 복합명사 사용: {len(fallback_compounds)}개")
        return fallback_compounds


# 글로벌 인스턴스 (싱글톤 패턴)
_config_manager = None


def get_config_manager(**kwargs) -> DynamoDBConfigManager:
    """설정 관리자 싱글톤 인스턴스 반환"""
    global _config_manager
    if _config_manager is None:
        _config_manager = DynamoDBConfigManager(**kwargs)
    return _config_manager


# 편의 함수들 (기존 API 호환성)
def get_all_stopwords(use_cache: bool = True) -> Set[str]:
    """모든 불용어 조회 (호환성 함수)"""
    manager = get_config_manager()
    return manager.get_all_stopwords(use_cache=use_cache)


def load_domain_stopwords(domain: str, use_cache: bool = True) -> Set[str]:
    """도메인별 불용어 조회 (호환성 함수)"""
    manager = get_config_manager()
    return manager.get_stopwords(f"domain_{domain}", use_cache=use_cache)


def add_stopword_to_file(word: str, category: str = "custom"):
    """불용어 추가 (호환성 함수)"""
    manager = get_config_manager()
    return manager.add_stopwords(category, [word])


def remove_stopword_from_file(word: str, category: str = "custom"):
    """불용어 제거 (호환성 함수)"""
    manager = get_config_manager()
    return manager.remove_stopwords(category, [word])


def export_stopwords_summary():
    """설정 요약 반환 (호환성 함수)"""
    manager = get_config_manager()
    return manager.get_config_summary()
