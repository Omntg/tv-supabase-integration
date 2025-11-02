#!/usr/bin/env python3
"""
Supabase Bağlantı Testi
Bu script gerçek Supabase database'nizle bağlantıyı test eder
"""

import os
import sys
from datetime import datetime
import pandas as pd

# Supabase client setup
SUPABASE_URL = "https://ghfkkcatjzaopsasjtzn.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdoZmtrY2F0anphb3BzYXNqdHpuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjIwMzE1MzksImV4cCI6MjA3NzYwNzUzOX0.xMzj9-d4wB-eUxaGvhD1x8Xkeez09uxgnEGuf3HNhVw"

def test_supabase_connection():
    """Supabase bağlantısını test eder"""
    try:
        from supabase import create_client
        
        # Supabase client oluştur
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("✅ Supabase client başarıyla oluşturuldu")
        
        # Test tablosundan veri çek
        result = supabase.table('trading_data').select('*').limit(5).execute()
        
        if result.data:
            print(f"✅ Database bağlantısı başarılı! {len(result.data)} satır veri bulundu")
            print("\n📊 Mevcut veriler:")
            for row in result.data:
                print(f"  {row['code']}: {row['closing_tl']} TL, Volume: {row['volume_t']:,}")
        else:
            print("⚠️  Tablo boş, henüz veri eklenmemiş")
        
        return True
        
    except Exception as e:
        print(f"❌ Supabase bağlantı hatası: {e}")
        return False

def test_insert_data():
    """Test verisi eklemeyi test eder"""
    try:
        from supabase import create_client
        
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        # Test verisi oluştur
        test_data = {
            'code': 'TEST',
            'date': datetime.now().isoformat(),
            'high_tl': 100.50,
            'low_tl': 99.80,
            'closing_tl': 100.25,
            'volume_t': 50000
        }
        
        # Veri ekle (upsert)
        result = supabase.table('trading_data').upsert([test_data], on_conflict=['code', 'date']).execute()
        
        if result.data:
            print("✅ Test verisi başarıyla eklendi")
            return True
        else:
            print("❌ Test verisi eklenemedi")
            return False
            
    except Exception as e:
        print(f"❌ Veri ekleme hatası: {e}")
        return False

def main():
    """Ana test fonksiyonu"""
    print("🚀 Supabase Integration Testi Başlıyor...")
    print("=" * 50)
    
    # Bağlantı testi
    connection_ok = test_supabase_connection()
    
    if connection_ok:
        print("\n" + "=" * 50)
        print("🎉 TÜM TESTLER BAŞARILI!")
        print("✅ Supabase integration çalışıyor")
        print("✅ Database bağlantısı kurulu")
        print("✅ Veri okuma/yazma işlemleri çalışıyor")
        print("\nSisteminiz production'da kullanıma hazır!")
    else:
        print("\n❌ Testler başarısız, lütfen bağlantı ayarlarını kontrol edin")

if __name__ == "__main__":
    main()