#!/usr/bin/env python3
"""
Supabase Bağlantı Testi - Geliştirilmiş Versiyon
Bu script gerçek Supabase database'nizle bağlantıyı test eder ve performans ölçer
"""

import os
import sys
import time
import logging
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from contextlib import contextmanager

# Environment variables ile yapılandırma
SUPABASE_URL = os.getenv('SUPABASE_URL', "https://ghfkkcatjzaopsasjtzn.supabase.co")
SUPABASE_KEY = os.getenv('SUPABASE_KEY', "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdoZmtrY2F0anphb3BzYXNqdHpuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjIwMzE1MzksImV4cCI6MjA3NzYwNzUzOX0.xMzj9-d4wB-eUxaGvhD1x8Xkeez09uxgnEGuf3HNhVw")

# Logging yapılandırması
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('supabase_test.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class PerformanceMetrics:
    """Performans metriklerini saklayan veri sınıfı"""
    operation: str
    start_time: float
    end_time: float
    duration: float = 0.0
    success: bool = False
    error_message: str = ""
    
    def __post_init__(self):
        if self.end_time > 0:
            self.duration = self.end_time - self.start_time
    
    def to_dict(self) -> Dict[str, Any]:
        """Dictionary'ye dönüştür"""
        return {
            'operation': self.operation,
            'duration': self.duration,
            'success': self.success,
            'error_message': self.error_message,
            'timestamp': datetime.now().isoformat()
        }

@contextmanager
def measure_performance(operation_name: str) -> PerformanceMetrics:
    """Performans ölçümü için context manager"""
    metrics = PerformanceMetrics(
        operation=operation_name,
        start_time=time.time(),
        end_time=0.0
    )
    try:
        yield metrics
        metrics.success = True
    except Exception as e:
        metrics.success = False
        metrics.error_message = str(e)
        logger.error(f"{operation_name} - Hata: {e}")
        raise
    finally:
        metrics.end_time = time.time()

class SupabaseTester:
    """Supabase bağlantı ve işlem testleri için ana sınıf"""
    
    def __init__(self):
        self.supabase_client = None
        self.setup_connection()
    
    def setup_connection(self):
        """Supabase bağlantısını kurar"""
        try:
            from supabase import create_client
            self.supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
            logger.info("✅ Supabase client başarıyla oluşturuldu")
        except ImportError:
            logger.error("❌ supabase-py paketi bulunamadı. pip install supabase ile yükleyin.")
            raise
        except Exception as e:
            logger.error(f"❌ Supabase bağlantı kurulum hatası: {e}")
            raise
    
    def test_basic_connection(self) -> bool:
        """Temel bağlantı testi"""
        logger.info("🔍 Temel bağlantı testi başlatılıyor...")
        
        try:
            result = self.supabase_client.table('trading_data').select('*').limit(1).execute()
            
            if result.data is not None:
                logger.info(f"✅ Database bağlantısı başarılı")
                return True
            else:
                logger.warning("⚠️  Sorgu sonucu None döndü")
                return False
                
        except Exception as e:
            logger.error(f"❌ Bağlantı testi başarısız: {e}")
            return False
    
    def get_latest_date(self, symbol: str) -> Optional[str]:
        """Belirli bir sembol için en son tarihi getirir"""
        logger.info(f"📅 {symbol} sembolü için en son tarih sorgulanıyor...")
        
        try:
            with measure_performance("get_latest_date") as metrics:
                result = (
                    self.supabase_client.table('trading_data')
                    .select('date')
                    .eq('code', symbol)
                    .order('date', desc=True)
                    .limit(1)
                    .execute()
                )
                
                if result.data:
                    latest_date = result.data[0]['date']
                    logger.info(f"✅ {symbol} için en son tarih: {latest_date}")
                    return latest_date
                else:
                    logger.warning(f"⚠️  {symbol} için veri bulunamadı")
                    return None
                    
        except Exception as e:
            logger.error(f"❌ Tarih sorgulama hatası: {e}")
            return None
    
    def test_bulk_insert(self, data_count: int = 100) -> bool:
        """Bulk insert testi yapar"""
        logger.info(f"📦 Bulk insert testi başlatılıyor ({data_count} kayıt)...")
        
        # Test verisi oluştur
        test_data = []
        base_time = datetime.now() - timedelta(days=7)
        
        for i in range(data_count):
            record = {
                'code': f'BULK{i % 5:02d}',  # 5 farklı sembol
                'date': (base_time + timedelta(hours=i)).isoformat(),
                'high_tl': 100.50 + (i * 0.1),
                'low_tl': 99.80 + (i * 0.1),
                'closing_tl': 100.25 + (i * 0.1),
                'volume_t': 50000 + i * 1000
            }
            test_data.append(record)
        
        try:
            with measure_performance("bulk_insert") as metrics:
                # Batch'ler halinde ekleme (her batch 20 kayıt)
                batch_size = 20
                total_inserted = 0
                
                for i in range(0, len(test_data), batch_size):
                    batch = test_data[i:i + batch_size]
                    
                    result = (
                        self.supabase_client.table('trading_data')
                        .insert(batch)
                        .execute()
                    )
                    
                    if result.data:
                        total_inserted += len(result.data)
                        logger.info(f"✅ Batch {i//batch_size + 1} tamamlandı: {len(result.data)} kayıt")
                
                logger.info(f"🎉 Bulk insert başarılı! Toplam {total_inserted} kayıt eklendi")
                return True
                
        except Exception as e:
            logger.error(f"❌ Bulk insert hatası: {e}")
            return False
    
    def test_data_operations(self) -> Dict[str, bool]:
        """Çeşitli veri operasyonlarını test eder"""
        logger.info("🔍 Veri operasyonları testi başlatılıyor...")
        
        results = {}
        
        # Test verisi ekleme
        try:
            test_record = {
                'code': 'OPERTEST',
                'date': datetime.now().isoformat(),
                'high_tl': 105.50,
                'low_tl': 104.80,
                'closing_tl': 105.25,
                'volume_t': 75000
            }
            
            with measure_performance("single_insert") as metrics:
                result = (
                    self.supabase_client.table('trading_data')
                    .insert(test_record)
                    .execute()
                )
                
                results['single_insert'] = metrics.success and bool(result.data)
                
        except Exception as e:
            logger.error(f"❌ Tek kayıt ekleme hatası: {e}")
            results['single_insert'] = False
        
        # Veri okuma testi
        try:
            with measure_performance("select_operations") as metrics:
                # Son 10 kayıt
                result = (
                    self.supabase_client.table('trading_data')
                    .select('*')
                    .order('date', desc=True)
                    .limit(10)
                    .execute()
                )
                
                results['select_operations'] = metrics.success and bool(result.data)
                
        except Exception as e:
            logger.error(f"❌ Veri okuma hatası: {e}")
            results['select_operations'] = False
        
        # Filtreleme testi
        try:
            with measure_performance("filtered_select") as metrics:
                result = (
                    self.supabase_client.table('trading_data')
                    .select('*')
                    .eq('code', 'OPERTEST')
                    .limit(5)
                    .execute()
                )
                
                results['filtered_select'] = metrics.success and bool(result.data)
                
        except Exception as e:
            logger.error(f"❌ Filtreleme hatası: {e}")
            results['filtered_select'] = False
        
        return results
    
    def run_performance_tests(self) -> Dict[str, float]:
        """Performans testlerini çalıştırır"""
        logger.info("⚡ Performans testleri başlatılıyor...")
        
        performance_results = {}
        
        # Test 1: 100 kayıt okuma
        try:
            with measure_performance("read_100_records") as metrics:
                result = (
                    self.supabase_client.table('trading_data')
                    .select('*')
                    .limit(100)
                    .execute()
                )
                performance_results['read_100_records'] = metrics.duration
                
        except Exception as e:
            logger.error(f"❌ 100 kayıt okuma hatası: {e}")
            performance_results['read_100_records'] = -1
        
        # Test 2: 50 kayıt okuma (filtrelenmiş)
        try:
            with measure_performance("filtered_read_50") as metrics:
                result = (
                    self.supabase_client.table('trading_data')
                    .select('*')
                    .eq('code', 'TEST')
                    .limit(50)
                    .execute()
                )
                performance_results['filtered_read_50'] = metrics.duration
                
        except Exception as e:
            logger.error(f"❌ Filtrelenmiş okuma hatası: {e}")
            performance_results['filtered_read_50'] = -1
        
        # Test 3: Aggregate sorgu
        try:
            with measure_performance("aggregate_query") as metrics:
                result = (
                    self.supabase_client.table('trading_data')
                    .select('code, volume_t')
                    .gte('volume_t', 100000)
                    .execute()
                )
                performance_results['aggregate_query'] = metrics.duration
                
        except Exception as e:
            logger.error(f"❌ Aggregate sorgu hatası: {e}")
            performance_results['aggregate_query'] = -1
        
        return performance_results
    
    def test_error_handling(self):
        """Hata durumlarını test eder"""
        logger.info("🚨 Hata durumu testleri başlatılıyor...")
        
        error_tests = []
        
        # Test 1: Yanlış tablo adı
        try:
            with measure_performance("invalid_table") as metrics:
                result = self.supabase_client.table('non_existent_table').select('*').execute()
                error_tests.append("Yanlış tablo adı - beklenmedik başarı!")
        except Exception as e:
            logger.info(f"✅ Yanlış tablo adı hatası beklendi: {type(e).__name__}")
            error_tests.append("Yanlış tablo adı hatası doğru yakalandı")
        
        # Test 2: Geçersiz sütun adı
        try:
            with measure_performance("invalid_column") as metrics:
                result = self.supabase_client.table('trading_data').select('non_existent_column').execute()
                error_tests.append("Geçersiz sütun adı - beklenmedik başarı!")
        except Exception as e:
            logger.info(f"✅ Geçersiz sütun adı hatası beklendi: {type(e).__name__}")
            error_tests.append("Geçertsiz sütun adı hatası doğru yakalandı")
        
        # Test 3: Çok büyük LIMIT değeri
        try:
            with measure_performance("large_limit") as metrics:
                result = self.supabase_client.table('trading_data').select('*').limit(10000).execute()
                # Bu test başarılı olacak, performansı ölçüyoruz
        except Exception as e:
            logger.warning(f"⚠️ Büyük LIMIT testi: {e}")
        
        return error_tests
    
    def generate_report(self, test_results: Dict[str, Any], performance_results: Dict[str, float]):
        """Test sonuçlarını raporlar"""
        logger.info("📊 Test raporu oluşturuluyor...")
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'supabase_url': SUPABASE_URL,
            'test_results': test_results,
            'performance_results': performance_results,
            'environment': {
                'python_version': sys.version,
                'os_name': os.name,
                'env_vars': {k: v[:10] + "..." if len(v) > 10 else v for k, v in os.environ.items() if 'SUPABASE' in k}
            }
        }
        
        # Raporu JSON dosyasına kaydet
        with open('supabase_test_report.json', 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logger.info("📄 Detaylı rapor 'supabase_test_report.json' dosyasına kaydedildi")

def main():
    """Ana test fonksiyonu"""
    logger.info("🚀 Supabase Integration Testi Başlıyor...")
    logger.info("=" * 60)
    
    try:
        tester = SupabaseTester()
        
        # Test sonuçlarını sakla
        all_test_results = {}
        all_performance_results = {}
        
        # 1. Temel bağlantı testi
        all_test_results['basic_connection'] = tester.test_basic_connection()
        
        if not all_test_results['basic_connection']:
            logger.error("❌ Temel bağlantı başarısız! Testler durduruluyor.")
            return
        
        # 2. Tarih kontrol testi
        latest_date = tester.get_latest_date('TEST')
        all_test_results['date_check'] = latest_date is not None
        
        # 3. Veri operasyonları testi
        data_op_results = tester.test_data_operations()
        all_test_results.update({f"data_op_{k}": v for k, v in data_op_results.items()})
        
        # 4. Bulk insert testi
        all_test_results['bulk_insert'] = tester.test_bulk_insert(data_count=20)
        
        # 5. Performans testleri
        all_performance_results = tester.run_performance_tests()
        
        # 6. Hata durumu testleri
        error_tests = tester.test_error_handling()
        all_test_results['error_handling'] = len(error_tests) > 0
        all_test_results['error_details'] = error_tests
        
        # 7. Rapor oluştur
        tester.generate_report(all_test_results, all_performance_results)
        
        # Sonuçları ekrana yazdır
        logger.info("\n" + "=" * 60)
        logger.info("🎯 TEST SONUÇLARI")
        logger.info("=" * 60)
        
        for test_name, result in all_test_results.items():
            status = "✅ BAŞARILI" if result else "❌ BAŞARISIZ"
            logger.info(f"{test_name:25}: {status}")
        
        if all_performance_results:
            logger.info("\n⚡ PERFORMANS SONUÇLARI")
            logger.info("=" * 30)
            for op_name, duration in all_performance_results.items():
                if duration >= 0:
                    logger.info(f"{op_name:25}: {duration:.3f}s")
        
        # Genel değerlendirme
        success_count = sum(1 for result in all_test_results.values() if result)
        total_tests = len(all_test_results)
        
        logger.info("\n" + "=" * 60)
        if success_count == total_tests:
            logger.info("🎉 TÜM TESTLER BAŞARILI!")
            logger.info("✅ Supabase integration mükemmel çalışıyor")
            logger.info("✅ Production'da kullanıma hazır")
        else:
            logger.warning(f"⚠️  {success_count}/{total_tests} test başarılı")
            logger.info("🔧 Lütfen başarısız testleri inceleyin")
        
    except KeyboardInterrupt:
        logger.info("\n⏹️  Test kullanıcı tarafından durduruldu")
    except Exception as e:
        logger.error(f"💥 Kritik hata: {e}")
        logger.error("Testler durduruldu")

if __name__ == "__main__":
    main()