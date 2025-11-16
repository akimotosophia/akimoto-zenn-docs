import csv
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import os

def generate_random_names_batch(count, min_length=5, max_length=20):
    """ランダムな名前をバッチ生成（完全ベクトル化）"""
    chars = np.array(list('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'))

    # 最大長で一度に生成し、後で切り詰める
    random_chars = np.random.choice(chars, size=(count, max_length))
    lengths = np.random.randint(min_length, max_length + 1, count)

    # 各行の文字列を生成（NumPy配列操作で高速化）
    names = [''.join(row[:length]) for row, length in zip(random_chars, lengths)]
    return names

def generate_random_dates_batch(count):
    """ランダムな日付をバッチ生成（文字列操作で高速化）"""
    # 日付の範囲を計算
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2025, 12, 31)
    days_range = (end_date - start_date).days

    # ランダムな日数を一度に生成
    random_days = np.random.randint(0, days_range + 1, count)

    # numpy のベクトル演算で日付を計算
    base_timestamp = start_date.timestamp()
    timestamps = base_timestamp + random_days * 86400  # 86400秒 = 1日

    # タイムスタンプから日付文字列に変換
    dates = [datetime.fromtimestamp(ts).strftime('%Y/%m/%d') for ts in timestamps]
    return dates

def calculate_rows_for_size(target_size_mb):
    """目標サイズに必要な行数を計算"""
    avg_row_size = 45
    target_bytes = target_size_mb * 1024 * 1024
    return int(target_bytes / avg_row_size)

def generate_csv(filename, size_mb, error_type=None, chunk_size=1000000):
    """CSVファイルを生成（numpy/pandas使用で最大高速化）

    Args:
        filename: 出力ファイル名
        size_mb: ファイルサイズ（MB）
        error_type: エラータイプ ('no_null', 'name_invalid', 'date_invalid', None)
        chunk_size: 一度に生成する行数（メモリと速度のバランス）
    """
    rows = calculate_rows_for_size(size_mb)
    print(f"Generating {filename} ({size_mb}MB, approx {rows:,} rows)...")

    # ヘッダーを書き込み
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['no', 'name', 'created_date'])

    # チャンク単位でデータを生成・書き込み
    for start_idx in range(0, rows, chunk_size):
        end_idx = min(start_idx + chunk_size, rows)
        current_chunk_size = end_idx - start_idx

        # バッチでデータ生成
        nos = np.arange(start_idx + 1, end_idx + 1)
        names = generate_random_names_batch(current_chunk_size)
        dates = generate_random_dates_batch(current_chunk_size)

        # エラーパターンを混入（ベクトル化）
        if error_type == 'no_null':
            # 1000行ごとにnoをnullに
            error_mask = (np.arange(start_idx, end_idx) % 1000) == 0
            nos = nos.astype(object)
            nos[error_mask] = ''
        elif error_type == 'name_invalid':
            # 1000行ごとに21文字以上の名前
            chars = list('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789')
            for i in range(current_chunk_size):
                if (start_idx + i) % 1000 == 0:
                    names[i] = ''.join(np.random.choice(chars, np.random.randint(21, 31)))
        elif error_type == 'date_invalid':
            # 1000行ごとに不正な日付フォーマット
            error_indices = np.where((np.arange(start_idx, end_idx) % 1000) == 0)[0]
            for idx in error_indices:
                dates[idx] = '2025-01-01'

        # DataFrameを作成
        df = pd.DataFrame({
            'no': nos,
            'name': names,
            'created_date': dates
        })

        # CSVに追記（高速化オプション）
        df.to_csv(
            filename,
            mode='a',
            index=False,
            header=False,
            encoding='utf-8',
            chunksize=50000  # 書き込みもチャンク化
        )

        # 進捗表示
        print(f"  Progress: {end_idx:,} / {rows:,} rows ({end_idx / rows * 100:.1f}%)")

    # 実際のファイルサイズを表示
    actual_size_mb = os.path.getsize(filename) / (1024 * 1024)
    print(f"  Completed: {filename} ({actual_size_mb:.2f}MB)")

if __name__ == '__main__':
    os.makedirs('test-data', exist_ok=True)

    # 正常データ
    print("\n=== Generating normal data ===")
    generate_csv('test-data/normal_10mb.csv', 10)
    generate_csv('test-data/normal_1gb.csv', 1024)
    generate_csv('test-data/normal_10gb.csv', 10240)
    generate_csv('test-data/normal_30gb.csv', 30720)

    # 異常データ
    print("\n=== Generating error data ===")
    generate_csv('test-data/error_no_null.csv', 10, 'no_null')
    generate_csv('test-data/error_name_invalid.csv', 10, 'name_invalid')
    generate_csv('test-data/error_date_invalid.csv', 10, 'date_invalid')

    print("\n=== All test data generated successfully! ===")
