#!/usr/bin/env python3
"""
TradingView Data Fetcher to Supabase

Bu script TradingView'den finansal verileri çeker ve Supabase veritabanına yükler.
Production-ready bir script olarak tasarlanmıştır ve şu özellikler içerir:
- Environment variable desteği
- TradingView entegrasyonu (tvDatafeed library)
- Supabase upsert operasyonları
- Incremental vs full refresh logic
- Error handling with tenacity retry
- ThreadPoolExecutor ile paralel işleme
- Kapsamlı logging
- Data validation ve transformation
- --full-refresh command line argument desteği
- GitHub Actions için JSON summary output

Author: Production Team
Version: 1.0.0
Date: 2025-11-02
"""

import os
import sys
import json
import time
import argparse
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
import calendar

# Supabase and TV Datafeed imports
try:
    from supabase import create_client, Client
    from tvDatafeed import TvDatafeed, Interval
except ImportError as e:
    print(f"❌ Gerekli kütüphane bulunamadı: {e}")
    print("Lütfen şu komutları çalıştırın:")
    print("pip install supabase tvdatafeed pandas tenacity")
    sys.exit(1)

# Tenacity for retry logic
try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
except ImportError:
    print("❌ tenacity kütüphanesi bulunamadı.")
    print("pip install tenacity")
    sys.exit(1)

# Suppress warnings
warnings.filterwarnings("ignore")


class TradingViewSupabaseFetcher:
    """
    TradingView'den veri çeken ve Supabase'e yükleyen ana sınıf.
    
    Bu sınıf TradingView verilerini çekmek, işlemek ve Supabase veritabanına
    yüklemek için gerekli tüm fonksiyonları sağlar.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        TradingViewSupabaseFetcher sınıfını başlatır.
        
        Args:
            config (Dict[str, Any]): Konfigürasyon parametreleri
        """
        self.config = config
        self.logger = self._setup_logging()
        self.tv_client = None
        self.supabase_client = None
        self.symbols = []
        self.execution_stats = {
            'total_symbols': 0,
            'successful_fetches': 0,
            'failed_fetches': 0,
            'total_records': 0,
            'new_records': 0,
            'updated_records': 0,
            'errors': []
        }
        
    def _setup_logging(self) -> logging.Logger:
        """
        Logging sistemini kurar.
        
        Returns:
            logging.Logger: Yapılandırılmış logger nesnesi
        """
        logger = logging.getLogger('tv_data_fetcher')
        logger.setLevel(logging.DEBUG)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # File handler
        log_file = f"tv_data_fetcher_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)
        
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
        
        return logger
    
    def _load_symbols(self) -> List[str]:
        """
        Sembol listesini yükler.
        
        Returns:
            List[str]: Sembol listesi
            
        Raises:
            FileNotFoundError: Sembol dosyası bulunamazsa
        """
        symbols_path = self.config.get('SYMBOL_LIST_PATH')
        
        if symbols_path and Path(symbols_path).exists():
            self.logger.info(f"Sembol listesi dosyası yükleniyor: {symbols_path}")
            with open(symbols_path, 'r', encoding='utf-8') as f:
                symbols = [line.strip() for line in f if line.strip()]
        else:
            # Default Turkish stocks
            symbols = [
                'A1CAP', 'A1YEN', 'AEFES', 'AGESA', 'AGHOL', 'AGYO', 'AHGAZ', 'AKBNK',
                'AKFGY', 'AKGRT', 'AKMGY', 'AKSEN', 'AKSUE', 'ALBRK', 'ALCAR', 'ALKA',
                'ALTIN', 'ANHYT', 'ANSGR', 'ARASE', 'ARDYZ', 'ASELS', 'ASTOR', 'ATAGY',
                'ATATP', 'AVGYO', 'AYDEM', 'AYEN', 'AYGAZ', 'BAGFS', 'BAKAB', 'BASGZ',
                'BESLR', 'BEYAZ', 'BIGCH', 'BIMAS', 'BNTAS', 'BOSSA', 'BRKSN', 'BRLSM',
                'BRSAN', 'BRYAT', 'CCOLA', 'CEMTS', 'CIMSA', 'CLEBI', 'CRDFA', 'CWENE',
                'DAPGM', 'DERIM', 'DESA', 'DESPC', 'DGATE', 'DOCO', 'DOFER', 'DOHOL',
                'EBEBK', 'ECZYT', 'EDATA', 'EGEPO', 'EGGUB', 'EGPRO', 'EKGYO', 'ELITE',
                'EMKEL', 'ENERY', 'ENJSA', 'ENKAI', 'EREGL', 'EUPWR', 'EUREN', 'FMIZP',
                'FORTE', 'FROTO', 'FZLGY', 'GARAN', 'GARFA', 'GEDZA', 'GENIL', 'GENTS',
                'GESAN', 'GIPTA', 'GLCVY', 'GLDTR', 'GLRMK', 'GLYHO', 'GMSTR', 'GMTAS',
                'GOKNR', 'GRSEL', 'GRTHO', 'GUBRF', 'GWIND', 'HALKB', 'HLGYO', 'HTTBT',
                'HUNER', 'INDES', 'ISCTR', 'ISDMR', 'ISFIN', 'ISGSY', 'ISGYO', 'ISKPL',
                'ISMEN', 'KATMR', 'KCAER', 'KCHOL', 'KLKIM', 'KLMSN', 'KLSYN', 'KOZAA',
                'KOZAL', 'KRDMA', 'KRDMD', 'KRONT', 'KRPLS', 'KRSTL', 'LIDER', 'LIDFA',
                'LILAK', 'LINK', 'LKMNH', 'LOGO', 'LYDYE', 'MACKO', 'MAGEN', 'MAKTK',
                'MARBL', 'MAVI', 'MERIT', 'METUR', 'MGROS', 'MIATK', 'MNDRS', 'MOBTL',
                'MPARK', 'MRGYO', 'MTRKS', 'NTGAZ', 'NTHOL', 'NUHCM', 'OBASE', 'ODAS',
                'OFSYM', 'ONCSM', 'ORGE', 'OTKAR', 'OYAKC', 'OYYAT', 'OZGYO', 'OZSUB',
                'PAGYO', 'PAPIL', 'PASEU', 'PATEK', 'PETUN', 'PGSUS', 'PINSU', 'PLTUR',
                'PNLSN', 'PRKME', 'PSDTC', 'QUAGR', 'RNPOL', 'RYGYO', 'RYSAS', 'SAHOL',
                'SANEL', 'SAYAS', 'SDTTR', 'SELGD', 'SISE', 'SKBNK', 'SMART', 'SRVGY',
                'SUNTK', 'SUWEN', 'TABGD', 'TARKM', 'TATGD', 'TAVHL', 'TBORG', 'TCELL',
                'TEZOL', 'THYAO', 'TLMAN', 'TMPOL', 'TNZTP', 'TRCAS', 'TRGYO', 'TSKB',
                'TTKOM', 'TUKAS', 'TUPRS', 'TURSG', 'ULKER', 'ULUUN', 'VAKBN', 'VERUS',
                'YGGYO', 'YKBNK', 'YUNSA', 'YYLGD', 'ZRGYO'
            ]
            self.logger.info(f"Varsayılan sembol listesi kullanılıyor: {len(symbols)} sembol")
            
        self.symbols = symbols
        self.execution_stats['total_symbols'] = len(symbols)
        return symbols
    
    def _initialize_clients(self) -> None:
        """
        TradingView ve Supabase client'larını başlatır.
        
        Raises:
            Exception: Client başlatma başarısız olursa
        """
        try:
            # TradingView client
            username = self.config.get('TV_USERNAME')
            password = self.config.get('TV_PASSWORD')
            
            if username and password:
                self.tv_client = TvDatafeed(username, password)
                self.logger.info("TradingView'e başarıyla giriş yapıldı")
            else:
                self.tv_client = TvDatafeed()
                self.logger.warning("TradingView guest modunda çalışıyor")
                
        except Exception as e:
            self.logger.error(f"TradingView client başlatılamadı: {e}")
            raise
            
        try:
            # Supabase client
            url = self.config.get('SUPABASE_URL')
            key = self.config.get('SUPABASE_ANON_KEY')
            
            if not url or not key:
                raise ValueError("SUPABASE_URL ve SUPABASE_ANON_KEY gerekli")
                
            self.supabase_client = create_client(url, key)
            self.logger.info("Supabase client başarıyla başlatıldı")
            
        except Exception as e:
            self.logger.error(f"Supabase client başlatılamadı: {e}")
            raise
    
    def _is_weekend_or_holiday(self) -> bool:
        """
        Bugünün hafta sonu veya tatil olup olmadığını kontrol eder.
        holiday_dates.txt dosyasından resmi tatilleri okur.
        
        Returns:
            bool: Hafta sonu/tatil ise True, değilse False
        """
        today = datetime.now()
        
        # Hafta sonu kontrolü (Saturday=5, Sunday=6)
        if today.weekday() >= 5:  # Saturday or Sunday
            self.logger.info(f"Hafta sonu olduğu için işlem atlanıyor: {today.strftime('%A %Y-%m-%d')}")
            return True
            
        # Holiday dates dosyasını kontrol et
        try:
            holiday_file = Path("holiday_dates.txt")
            if holiday_file.exists():
                today_str = today.strftime("%Y-%m-%d")
                
                with open(holiday_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        # Yorum satırlarını ve boş satırları atla
                        if not line or line.startswith('#'):
                            continue
                        
                        # Format: YYYY-MM-DD, Aciklama
                        if ',' in line:
                            date_part = line.split(',')[0].strip()
                            if date_part == today_str:
                                self.logger.info(f"Resmi tatil olduğu için işlem atlanıyor: {today_str} - {line.split(',', 1)[1].strip()}")
                                return True
            else:
                self.logger.warning("holiday_dates.txt dosyası bulunamadı, fallback to hardcoded holidays")
        except Exception as e:
            self.logger.warning(f"Holiday dosyası okunamadı: {e}, fallback to hardcoded holidays")
        
        # Fallback: Temel tatil günleri (2025 için güncel listesi)
        holidays = [
            (1, 1, "Yeni Yıl Tatili"),
            (3, 29, "Ramazan Bayramı Arefesi"),
            (3, 30, "Ramazan Bayramı 1. Gün"),
            (3, 31, "Ramazan Bayramı 2. Gün"),
            (4, 1, "Ramazan Bayramı 3. Gün"),
            (4, 23, "Ulusal Egemenlik ve Çocuk Bayramı"),
            (5, 1, "Emek ve Dayanışma Günü"),
            (5, 19, "Atatürk'ü Anma Gençlik ve Spor Bayramı"),
            (6, 5, "Kurban Bayramı Arefesi"),
            (6, 6, "Kurban Bayramı 1. Gün"),
            (6, 7, "Kurban Bayramı 2. Gün"),
            (6, 8, "Kurban Bayramı 3. Gün"),
            (6, 9, "Kurban Bayramı 4. Gün"),
            (7, 15, "Demokrasi ve Milli Birlik Günü"),
            (8, 30, "Zafer Bayramı"),
            (10, 28, "Cumhuriyet Bayramı Arefesi"),
            (10, 29, "Cumhuriyet Bayramı"),
        ]
        
        for month, day, name in holidays:
            if today.month == month and today.day == day:
                self.logger.info(f"Resmi tatil olduğu için işlem atlanıyor: {today.strftime('%Y-%m-%d')} - {name}")
                return True
                
        return False
    
    def _check_if_new_data_needed(self, symbol: str) -> bool:
        """
        Supabase'den son kaydedilen tarihi kontrol eder ve yeni veri gerekip gerekmediğini belirler.
        
        Args:
            symbol (str): Sembol adı
            
        Returns:
            bool: Yeni veri gerekiyorsa True, değilse False
        """
        try:
            table_name = self.config.get('TABLE_NAME', 'trading_data')
            today = datetime.now().date()
            
            # Bugünkü veri var mı kontrol et
            result = self.supabase_client.table(table_name).select('date').eq('code', symbol).gte('date', str(today)).execute()
            
            if result.data and len(result.data) > 0:
                self.logger.info(f"Bugün için veri mevcut: {symbol} - {today}")
                return False
                
            # Son kaydedilen tarihi getir
            result = self.supabase_client.table(table_name).select('date').eq('code', symbol).order('date', desc=True).limit(1).execute()
            
            if not result.data:
                # İlk kez veri çekilecek
                self.logger.info(f"İlk veri çekimi: {symbol}")
                return True
                
            last_date = datetime.strptime(result.data[0]['date'], '%Y-%m-%d').date()
            days_diff = (today - last_date).days
            
            # 1 günden fazla fark varsa yeni veri gerekli
            if days_diff >= 1:
                self.logger.info(f"Yeni veri gerekli: {symbol} - Son veri: {last_date}, Bugün: {today}, Fark: {days_diff} gün")
                return True
            else:
                self.logger.info(f"Veri güncel: {symbol} - Son veri: {last_date}, Bugün: {today}")
                return False
                
        except Exception as e:
            self.logger.warning(f"Tarih kontrolü yapılamadı {symbol}: {e} - Yeni veri çekilecek")
            return True
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(Exception)
    )
    def _fetch_symbol_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """
        Tek bir sembol için veri çeker.
        
        Args:
            symbol (str): Sembol adı
            
        Returns:
            Optional[pd.DataFrame]: Çekilen veri veya None
            
        Raises:
            Exception: Veri çekme başarısız olursa
        """
        try:
            # Yeni veri kontrolü - sadece gerekliyse API çağrısı yap
            if not self.config.get('FULL_REFRESH', False):
                incremental_fetch = os.getenv('INCREMENTAL_FETCH_BARS', 'true').lower() == 'true'
                if incremental_fetch and not self._check_if_new_data_needed(symbol):
                    self.logger.info(f"Veri güncel olduğu için atlanıyor: {symbol}")
                    return None
            
            self.logger.info(f"📡 TradingView API çağrısı başlatılıyor: {symbol}")
            
            # TradingView parametreleri
            exchange = 'BIST'
            interval = Interval.in_daily
            
            # INCREMENTAL_FETCH_BARS ayarına göre n_bars belirle
            if self.config.get('FULL_REFRESH', False):
                n_bars = self.config.get('FULL_REFRESH_N_BARS', 5000)
                self.logger.debug(f"Full refresh modu: {n_bars} bar çekilecek")
            else:
                n_bars = self.config.get('INCREMENTAL_FETCH_BARS', 100)
                self.logger.debug(f"Incremental modu: {n_bars} bar çekilecek")
            
            # Veri çekme
            start_time = time.time()
            data = self.tv_client.get_hist(
                symbol=symbol,
                exchange=exchange,
                interval=interval,
                n_bars=n_bars
            )
            api_call_time = time.time() - start_time
            
            if data is None or data.empty:
                self.logger.warning(f"⚠️ Sembol için veri bulunamadı: {symbol}")
                return None
                
            # DataFrame'i işle
            df_processed = self._process_dataframe(data, symbol)
            self.logger.info(f"✅ Veri başarıyla işlendi: {symbol} - {len(df_processed)} kayıt ({api_call_time:.2f}s)")
            
            return df_processed
            
        except Exception as e:
            self.logger.error(f"❌ Sembol verisi çekilemedi {symbol}: {e}")
            self.execution_stats['errors'].append(f"{symbol}: {e}")
            raise
    
    def _process_dataframe(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        DataFrame'i standart formata çevirir.
        
        Args:
            df (pd.DataFrame): Ham DataFrame
            symbol (str): Sembol adı
            
        Returns:
            pd.DataFrame: İşlenmiş DataFrame
        """
        try:
            # Index'i sütuna çevir
            df_reset = df.reset_index()
            
            # Gerekli sütunları seç
            df_clean = df_reset[['datetime', 'high', 'low', 'close', 'volume']].copy()
            
            # Sembol sütunu ekle
            df_clean['code'] = symbol.upper()
            
            # Sütunları yeniden adlandır
            df_clean.rename(columns={
                'datetime': 'date',
                'high': 'high_tl',
                'low': 'low_tl',
                'close': 'closing_tl',
                'volume': 'volume_t'
            }, inplace=True)
            
            # Veri tiplerini düzenle
            df_clean['date'] = pd.to_datetime(df_clean['date'])
            df_clean['high_tl'] = pd.to_numeric(df_clean['high_tl'], errors='coerce')
            df_clean['low_tl'] = pd.to_numeric(df_clean['low_tl'], errors='coerce')
            df_clean['closing_tl'] = pd.to_numeric(df_clean['closing_tl'], errors='coerce')
            df_clean['volume_t'] = pd.to_numeric(df_clean['volume_t'], errors='coerce')
            
            # Null değerleri temizle
            df_clean.dropna(inplace=True)
            
            # Tarih sıralaması
            df_clean.sort_values('date', inplace=True)
            
            return df_clean
            
        except Exception as e:
            self.logger.error(f"DataFrame işlenemedi {symbol}: {e}")
            raise
    
    def _get_existing_dates(self, symbol: str) -> set:
        """
        Supabase'de mevcut tarihleri getirir.
        
        Args:
            symbol (str): Sembol adı
            
        Returns:
            set: Mevcut tarihlerin set'i
        """
        try:
            table_name = self.config.get('TABLE_NAME', 'trading_data')
            
            result = self.supabase_client.table(table_name)\
                .select('date')\
                .eq('code', symbol)\
                .execute()
                
            dates = {row['date'] for row in result.data}
            self.logger.debug(f"Mevcut tarihler: {symbol} - {len(dates)} adet")
            
            return dates
            
        except Exception as e:
            self.logger.warning(f"Mevcut tarihler alınamadı {symbol}: {e}")
            return set()
    
    def _upsert_data(self, df: pd.DataFrame, symbol: str) -> Tuple[int, int]:
        """
        DataFrame'i Supabase'e upsert eder.
        
        Args:
            df (pd.DataFrame): Upsert edilecek veri
            symbol (str): Sembol adı
            
        Returns:
            Tuple[int, int]: (yeni kayıt sayısı, güncellenen kayıt sayısı)
        """
        try:
            table_name = self.config.get('TABLE_NAME', 'trading_data')
            
            if self.config.get('FULL_REFRESH', False):
                # Full refresh - tüm verileri sil ve yeniden ekle
                self.supabase_client.table(table_name)\
                    .delete()\
                    .eq('code', symbol)\
                    .execute()
                
                result = self.supabase_client.table(table_name)\
                    .insert(df.to_dict('records'))\
                    .execute()
                    
                new_records = len(result.data) if result.data else len(df)
                return new_records, 0
                
            else:
                # Incremental update
                existing_dates = self._get_existing_dates(symbol)
                
                # Yeni verileri filtrele
                df_new = df[~df['date'].isin(existing_dates)].copy()
                
                if df_new.empty:
                    self.logger.info(f"Yeni veri yok: {symbol}")
                    return 0, 0
                
                # Upsert işlemi
                result = self.supabase_client.table(table_name)\
                    .upsert(df_new.to_dict('records'), on_conflict='code,date')\
                    .execute()
                    
                new_records = len(df_new)
                return new_records, 0
                
        except Exception as e:
            self.logger.error(f"Upsert başarısız {symbol}: {e}")
            raise
    
    def _process_symbol(self, symbol: str) -> bool:
        """
        Tek bir sembolü işler (veri çekme + upsert).
        
        Args:
            symbol (str): Sembol adı
            
        Returns:
            bool: İşlem başarılı ise True
        """
        try:
            # Veri çekme
            df = self._fetch_symbol_data(symbol)
            if df is None or df.empty:
                return False
            
            # Upsert
            new_records, updated_records = self._upsert_data(df, symbol)
            
            # İstatistikleri güncelle
            self.execution_stats['successful_fetches'] += 1
            self.execution_stats['total_records'] += len(df)
            self.execution_stats['new_records'] += new_records
            self.execution_stats['updated_records'] += updated_records
            
            self.logger.info(f"İşlem tamamlandı: {symbol} - {len(df)} toplam, {new_records} yeni")
            
            return True
            
        except Exception as e:
            self.execution_stats['failed_fetches'] += 1
            self.logger.error(f"Sembol işlenemedi {symbol}: {e}")
            return False
    
    def run(self) -> Dict[str, Any]:
        """
        Ana işlem fonksiyonu. Tüm sembolleri işler.
        
        Returns:
            Dict[str, Any]: İşlem sonuçları
        """
        try:
            self.logger.info("🚀 TradingView veri çekme işlemi başlatılıyor...")
            
            # Hafta sonu/tatil kontrolü - sadece force_run değilse
            force_run = os.getenv('FORCE_RUN', 'false').lower() == 'true'
            if self._is_weekend_or_holiday() and not force_run:
                self.logger.info("📅 Hafta sonu/tatil olduğu için işlem durduruluyor")
                self.logger.info("💡 Hafta sonu çalışması için 'force_run: true' parametresiyle yeniden çalıştırın")
                self.execution_stats['execution_time_seconds'] = 0
                self.execution_stats['completion_time'] = datetime.now().isoformat()
                return self.execution_stats
            elif force_run:
                self.logger.info("🔄 Force run modu aktif - hafta sonu/tatil kontrolü atlanıyor")
            
            # Setup
            self._load_symbols()
            self._initialize_clients()
            
            start_time = time.time()
            mode = "FULL REFRESH" if self.config.get('FULL_REFRESH', False) else "INCREMENTAL"
            incremental_fetch = os.getenv('INCREMENTAL_FETCH_BARS', 'true').lower() == 'true'
            
            self.logger.info(f"📊 Çalışma modu: {mode}")
            self.logger.info(f"🔧 Incremental fetch: {incremental_fetch}")
            self.logger.info(f"📈 Toplam {len(self.symbols)} sembol işlenecek")
            
            # Parallel processing
            max_workers = self.config.get('MAX_WORKERS', 5)
            self.logger.info(f"👥 Paralel işçi sayısı: {max_workers}")
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all tasks
                future_to_symbol = {
                    executor.submit(self._process_symbol, symbol): symbol 
                    for symbol in self.symbols
                }
                
                # Process completed tasks
                completed = 0
                for future in as_completed(future_to_symbol):
                    symbol = future_to_symbol[future]
                    completed += 1
                    
                    try:
                        success = future.result()
                        if success:
                            self.logger.info(f"✅ Tamamlandı ({completed}/{len(self.symbols)}): {symbol}")
                        else:
                            self.logger.warning(f"⚠️ Başarısız/Atlandı ({completed}/{len(self.symbols)}): {symbol}")
                    except Exception as e:
                        self.logger.error(f"❌ Hata ({completed}/{len(self.symbols)}): {symbol} - {e}")
            
            # İstatistikleri tamamla
            end_time = time.time()
            execution_time = end_time - start_time
            
            self.execution_stats['execution_time_seconds'] = execution_time
            self.execution_stats['completion_time'] = datetime.now().isoformat()
            
            self.logger.info(f"🏁 İşlem tamamlandı. Süre: {execution_time:.2f} saniye")
            self.logger.info(f"📊 İstatistikler: {self.execution_stats}")
            
            return self.execution_stats
            
        except Exception as e:
            self.logger.error(f"💥 Kritik hata: {e}")
            self.execution_stats['errors'].append(f"Critical: {e}")
            return self.execution_stats


def load_config() -> Dict[str, Any]:
    """
    Environment variables'dan konfigürasyonu yükler.
    
    Returns:
        Dict[str, Any]: Konfigürasyon dictionary'si
    """
    config = {
        'SUPABASE_URL': os.getenv('SUPABASE_URL'),
        'SUPABASE_ANON_KEY': os.getenv('SUPABASE_ANON_KEY'),
        'TV_USERNAME': os.getenv('TV_USERNAME'),
        'TV_PASSWORD': os.getenv('TV_PASSWORD'),
        'SYMBOL_LIST_PATH': os.getenv('SYMBOL_LIST_PATH'),
        'MAX_WORKERS': int(os.getenv('MAX_WORKERS', '5')),
        'INCREMENTAL_FETCH_BARS': int(os.getenv('INCREMENTAL_FETCH_BARS', '100')),
        'FULL_REFRESH_N_BARS': int(os.getenv('FULL_REFRESH_N_BARS', '5000')),
        'TABLE_NAME': os.getenv('TABLE_NAME', 'trading_data'),
        'INCREMENTAL_FETCH_BARS_ENABLED': os.getenv('INCREMENTAL_FETCH_BARS', 'true').lower() == 'true',
        'FULL_REFRESH': False
    }
    
    # Validate required config
    required = ['SUPABASE_URL', 'SUPABASE_ANON_KEY']
    missing = [key for key in required if not config[key]]
    
    if missing:
        raise ValueError(f"Eksik environment variables: {missing}")
    
    return config


def save_summary_report(stats: Dict[str, Any], output_path: str = 'execution_summary.json') -> None:
    """
    GitHub Actions için JSON summary raporu kaydeder.
    
    Args:
        stats (Dict[str, Any]): İstatistik verileri
        output_path (str): Çıkış dosyası yolu
    """
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"📊 Summary raporu kaydedildi: {output_path}")
        
    except Exception as e:
        print(f"❌ Summary raporu kaydedilemedi: {e}")


def main():
    """Ana fonksiyon."""
    parser = argparse.ArgumentParser(
        description='TradingView verilerini çeker ve Supabase\'e yükler',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Örnek kullanım:
  python tv_data_supabase.py
  python tv_data_supabase.py --full-refresh
  python tv_data_supabase.py --workers 10
  
Environment Variables:
  SUPABASE_URL          Supabase project URL
  SUPABASE_ANON_KEY     Supabase anon key
  TV_USERNAME           TradingView kullanıcı adı
  TV_PASSWORD           TradingView şifresi
  SYMBOL_LIST_PATH      Sembol listesi dosya yolu
  MAX_WORKERS           Paralel işlem sayısı (varsayılan: 5)
  INCREMENTAL_FETCH_BARS Incremental çekme kontrolü (true/false, varsayılan: true)
  FULL_REFRESH_N_BARS   Full refresh bar sayısı
  TABLE_NAME            Tablo adı (varsayılan: trading_data)
  FORCE_RUN             Hafta sonu kontrolünü atla (true/false, varsayılan: false)

Command Line Options:
  --full-refresh        Tüm verileri yeniden yükle (incremental yerine)
  --workers N           Paralel işlem sayısı (varsayılan: 5)
  --disable-incremental Incremental fetch kontrolünü devre dışı bırak
  --output FILE         Çıkış dosyası adı (varsayılan: execution_summary.json)
        """
    )
    
    parser.add_argument(
        '--full-refresh',
        action='store_true',
        help='Tüm verileri yeniden yükle (incremental yerine)'
    )
    
    parser.add_argument(
        '--workers',
        type=int,
        default=5,
        help='Paralel işlem sayısı (varsayılan: 5)'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        default='execution_summary.json',
        help='Çıkış dosyası adı (varsayılan: execution_summary.json)'
    )
    
    parser.add_argument(
        '--disable-incremental',
        action='store_true',
        help='Incremental fetch kontrolünü devre dışı bırak (tüm semboller için veri çek)'
    )
    
    args = parser.parse_args()
    
    try:
        # Config yükle
        config = load_config()
        config['MAX_WORKERS'] = args.workers
        config['FULL_REFRESH'] = args.full_refresh
        
        if args.disable_incremental:
            os.environ['INCREMENTAL_FETCH_BARS'] = 'false'
            config['INCREMENTAL_FETCH_BARS_ENABLED'] = False
        
        # Mode bilgisini yazdır
        mode = "FULL REFRESH" if args.full_refresh else "INCREMENTAL"
        incremental_status = "AKTİF" if config['INCREMENTAL_FETCH_BARS_ENABLED'] else "PASİF"
        print(f"🚀 Çalışma modu: {mode}")
        print(f"🔧 Incremental fetch: {incremental_status}")
        print(f"👥 Paralel işçi sayısı: {args.workers}")
        
        # Fetcher'ı başlat
        fetcher = TradingViewSupabaseFetcher(config)
        
        # İşlemi çalıştır
        stats = fetcher.run()
        
        # Summary raporu kaydet
        save_summary_report(stats, args.output)
        
        # Sonuçları yazdır
        print("\n" + "="*50)
        print("📈 İŞLEM SONUÇLARI")
        print("="*50)
        print(f"✅ Başarılı semboller: {stats['successful_fetches']}/{stats['total_symbols']}")
        print(f"❌ Başarısız semboller: {stats['failed_fetches']}")
        print(f"📊 Toplam kayıt: {stats['total_records']}")
        print(f"🆕 Yeni kayıtlar: {stats['new_records']}")
        print(f"🔄 Güncellenen kayıtlar: {stats['updated_records']}")
        print(f"⏱️  Çalışma süresi: {stats.get('execution_time_seconds', 0):.2f} saniye")
        
        if stats['errors']:
            print(f"\n⚠️ HATALAR ({len(stats['errors'])}):")
            for error in stats['errors'][:10]:  # İlk 10 hatayı göster
                print(f"   - {error}")
        
        # Exit code
        if stats['failed_fetches'] > 0:
            print(f"\n⚠️ Bazı işlemler başarısız oldu")
            sys.exit(1)
        else:
            print(f"\n🎉 Tüm işlemler başarıyla tamamlandı!")
            sys.exit(0)
            
    except KeyboardInterrupt:
        print("\n⏹️ İşlem kullanıcı tarafından durduruldu")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ Kritik hata: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()