from dataclasses import dataclass
from typing import Optional, Dict, Any
from datetime import datetime, date
import uuid
import json

@dataclass
class ReleaseItem:
    release_item_id: Optional[uuid.UUID] = None
    feature_name: Optional[str] = None
    release_date: Optional[date] = None
    release_type: Optional[str] = None
    release_type_value: Optional[int] = None
    vso_item: Optional[str] = None
    release_status: Optional[str] = None
    release_status_value: Optional[int] = None
    release_semester: Optional[str] = None
    product_id: Optional[uuid.UUID] = None
    product_name: Optional[str] = None
    is_publish_externally: Optional[bool] = None
    feature_description: Optional[str] = None

    @staticmethod
    def _parse_uuid(value):
        if not value:
            return None
        try:
            return uuid.UUID(value) if not isinstance(value, uuid.UUID) else value
        except Exception:
            v = str(value).strip('{} ')
            try:
                return uuid.UUID(v)
            except Exception:
                return None

    @staticmethod
    def _parse_date(value):
        if not value:
            return None
        if isinstance(value, date):
            return value
        for fmt in ('%m/%d/%Y', '%Y-%m-%d'):
            try:
                return datetime.strptime(value, fmt).date()
            except Exception:
                pass
        try:
            return datetime.fromisoformat(value).date()
        except Exception:
            return None

    @staticmethod
    def _parse_bool(value):
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        v = str(value).strip().lower()
        if v in ('1', 'true', 'yes', 'y', 't'):
            return True
        if v in ('0', 'false', 'no', 'n', 'f'):
            return False
        return None

    @staticmethod
    def _parse_int(value):
        if value is None or value == '':
            return None
        try:
            return int(value)
        except Exception:
            return None

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'ReleaseItem':
        return cls(
            release_item_id = cls._parse_uuid(d.get('ReleaseItemID')),
            feature_name = d.get('FeatureName'),
            release_date = cls._parse_date(d.get('ReleaseDate')),
            release_type = d.get('ReleaseType'),
            release_type_value = cls._parse_int(d.get('ReleaseTypeValue')),
            vso_item = d.get('VSOItem'),
            release_status = d.get('ReleaseStatus'),
            release_status_value = cls._parse_int(d.get('ReleaseStatusValue')),
            release_semester = d.get('ReleaseSemester'),
            product_id = cls._parse_uuid(d.get('ProductID')),
            product_name = d.get('ProductName'),
            is_publish_externally = cls._parse_bool(d.get('isPublishExternally')),
            feature_description = d.get('FeatureDescription'),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            'ReleaseItemID': str(self.release_item_id) if self.release_item_id else None,
            'FeatureName': self.feature_name,
            'ReleaseDate': self.release_date.strftime('%m/%d/%Y') if self.release_date else None,
            'ReleaseType': self.release_type,
            'ReleaseTypeValue': self.release_type_value,
            'VSOItem': self.vso_item,
            'ReleaseStatus': self.release_status,
            'ReleaseStatusValue': self.release_status_value,
            'ReleaseSemester': self.release_semester,
            'ProductID': str(self.product_id) if self.product_id else None,
            'ProductName': self.product_name,
            'isPublishExternally': ('true' if self.is_publish_externally else 'false')
                                   if self.is_publish_externally is not None else None,
            'FeatureDescription': self.feature_description,
        }

    def __repr__(self) -> str:
        return f"<ReleaseItem {self.feature_name!r} ({self.release_item_id})>"