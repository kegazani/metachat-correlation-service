from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, timezone
import numpy as np
from scipy import stats
import structlog

logger = structlog.get_logger()


class CorrelationAnalyzer:
    def __init__(self, min_data_days: int = 14, min_entries: int = 10, min_biometric_coverage: float = 0.7):
        self.min_data_days = min_data_days
        self.min_entries = min_entries
        self.min_biometric_coverage = min_biometric_coverage
    
    def analyze_correlations(
        self, mood_entries: List[Dict[str, Any]], biometric_entries: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        if not mood_entries or not biometric_entries:
            return []
        
        if len(mood_entries) < self.min_entries:
            logger.debug("Not enough mood entries", count=len(mood_entries))
            return []
        
        correlations = []
        
        heart_rate_correlation = self._analyze_heart_rate_correlation(mood_entries, biometric_entries)
        if heart_rate_correlation:
            correlations.append(heart_rate_correlation)
        
        sleep_correlation = self._analyze_sleep_correlation(mood_entries, biometric_entries)
        if sleep_correlation:
            correlations.append(sleep_correlation)
        
        activity_correlation = self._analyze_activity_correlation(mood_entries, biometric_entries)
        if activity_correlation:
            correlations.append(activity_correlation)
        
        return correlations
    
    def _analyze_heart_rate_correlation(
        self, mood_entries: List[Dict[str, Any]], biometric_entries: List[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        mood_values = []
        hr_values = []
        
        for mood_entry in mood_entries:
            mood_timestamp = self._parse_timestamp(mood_entry.get("timestamp") or mood_entry.get("created_at"))
            if not mood_timestamp:
                continue
            
            valence = mood_entry.get("payload", {}).get("valence") or mood_entry.get("valence")
            if valence is None:
                continue
            
            for bio_entry in biometric_entries:
                bio_timestamp = self._parse_timestamp(bio_entry.get("timestamp") or bio_entry.get("recorded_at"))
                if not bio_timestamp:
                    continue
                
                heart_rate = bio_entry.get("payload", {}).get("heart_rate") or bio_entry.get("heart_rate")
                if heart_rate is None:
                    continue
                
                time_diff = abs((mood_timestamp - bio_timestamp).total_seconds())
                if time_diff <= 3600:
                    mood_values.append(valence)
                    hr_values.append(heart_rate)
                    break
        
        if len(mood_values) < self.min_entries:
            return None
        
        correlation_coef, p_value = stats.pearsonr(mood_values, hr_values)
        
        if abs(correlation_coef) < 0.3 or p_value > 0.05:
            return None
        
        return {
            "correlation_type": "heart_rate_valence",
            "correlation_score": float(correlation_coef),
            "p_value": float(p_value),
            "sample_size": len(mood_values),
            "mood_data": {
                "mean_valence": float(np.mean(mood_values)),
                "std_valence": float(np.std(mood_values))
            },
            "biometric_data": {
                "mean_heart_rate": float(np.mean(hr_values)),
                "std_heart_rate": float(np.std(hr_values))
            }
        }
    
    def _analyze_sleep_correlation(
        self, mood_entries: List[Dict[str, Any]], biometric_entries: List[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        mood_values = []
        sleep_values = []
        
        for mood_entry in mood_entries:
            mood_timestamp = self._parse_timestamp(mood_entry.get("timestamp") or mood_entry.get("created_at"))
            if not mood_timestamp:
                continue
            
            arousal = mood_entry.get("payload", {}).get("arousal") or mood_entry.get("arousal")
            if arousal is None:
                continue
            
            mood_date = mood_timestamp.date()
            
            for bio_entry in biometric_entries:
                bio_timestamp = self._parse_timestamp(bio_entry.get("timestamp") or bio_entry.get("recorded_at"))
                if not bio_timestamp:
                    continue
                
                sleep_data = bio_entry.get("payload", {}).get("sleep_data") or bio_entry.get("sleep_data")
                if not sleep_data or not isinstance(sleep_data, dict):
                    continue
                
                sleep_hours = sleep_data.get("total_hours") or sleep_data.get("duration_hours")
                if sleep_hours is None:
                    continue
                
                bio_date = bio_timestamp.date()
                
                if mood_date == bio_date or (mood_date - bio_date).days == 1:
                    mood_values.append(arousal)
                    sleep_values.append(sleep_hours)
                    break
        
        if len(mood_values) < self.min_entries:
            return None
        
        correlation_coef, p_value = stats.pearsonr(mood_values, sleep_values)
        
        if abs(correlation_coef) < 0.3 or p_value > 0.05:
            return None
        
        return {
            "correlation_type": "sleep_arousal",
            "correlation_score": float(correlation_coef),
            "p_value": float(p_value),
            "sample_size": len(mood_values),
            "mood_data": {
                "mean_arousal": float(np.mean(mood_values)),
                "std_arousal": float(np.std(mood_values))
            },
            "biometric_data": {
                "mean_sleep_hours": float(np.mean(sleep_values)),
                "std_sleep_hours": float(np.std(sleep_values))
            }
        }
    
    def _analyze_activity_correlation(
        self, mood_entries: List[Dict[str, Any]], biometric_entries: List[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        mood_values = []
        activity_values = []
        
        for mood_entry in mood_entries:
            mood_timestamp = self._parse_timestamp(mood_entry.get("timestamp") or mood_entry.get("created_at"))
            if not mood_timestamp:
                continue
            
            valence = mood_entry.get("payload", {}).get("valence") or mood_entry.get("valence")
            if valence is None:
                continue
            
            mood_date = mood_timestamp.date()
            
            for bio_entry in biometric_entries:
                bio_timestamp = self._parse_timestamp(bio_entry.get("timestamp") or bio_entry.get("recorded_at"))
                if not bio_timestamp:
                    continue
                
                activity = bio_entry.get("payload", {}).get("activity") or bio_entry.get("activity")
                if not activity or not isinstance(activity, dict):
                    continue
                
                steps = activity.get("steps") or activity.get("total_steps")
                if steps is None:
                    continue
                
                bio_date = bio_timestamp.date()
                
                if mood_date == bio_date:
                    mood_values.append(valence)
                    activity_values.append(steps)
                    break
        
        if len(mood_values) < self.min_entries:
            return None
        
        correlation_coef, p_value = stats.pearsonr(mood_values, activity_values)
        
        if abs(correlation_coef) < 0.3 or p_value > 0.05:
            return None
        
        return {
            "correlation_type": "activity_valence",
            "correlation_score": float(correlation_coef),
            "p_value": float(p_value),
            "sample_size": len(mood_values),
            "mood_data": {
                "mean_valence": float(np.mean(mood_values)),
                "std_valence": float(np.std(mood_values))
            },
            "biometric_data": {
                "mean_steps": float(np.mean(activity_values)),
                "std_steps": float(np.std(activity_values))
            }
        }
    
    def _parse_timestamp(self, timestamp_str: Any) -> Optional[datetime]:
        if timestamp_str is None:
            return None
        
        if isinstance(timestamp_str, datetime):
            if timestamp_str.tzinfo is None:
                return timestamp_str.replace(tzinfo=timezone.utc)
            return timestamp_str
        
        if isinstance(timestamp_str, str):
            try:
                if timestamp_str.endswith('Z'):
                    timestamp_str = timestamp_str[:-1] + '+00:00'
                dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception as e:
                logger.debug("Failed to parse timestamp", timestamp=timestamp_str, error=str(e))
                return None
        
        return None

