import json
import os
import boto3
from boto3.s3.transfer import TransferConfig
import csv
from io import BytesIO, StringIO
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import islice
import threading

# S3転送設定（100MBレベルのファイル用に最適化）
transfer_config = TransferConfig(
    multipart_threshold=50 * 1024 * 1024,     # 50MB以上でマルチパート転送
    max_concurrency=10,                        # 並列スレッド数（Lambda vCPU数に応じて調整）
    multipart_chunksize=10 * 1024 * 1024,     # 10MBチャンク（100MBファイルなら10並列）
    num_download_attempts=5,                   # リトライ回数
    max_io_queue=1000,                         # I/Oキューサイズ
    io_chunksize=256 * 1024,                   # I/Oチャンクサイズ（256KB）
    use_threads=True                           # スレッドプールを使用
)

s3_client = boto3.client('s3')

# 並列処理設定
MAX_WORKERS = 4  # I/O待ち活用のためのスレッド数
BATCH_SIZE = 1000  # バッチサイズ（一度に処理する行数）

# スレッドセーフな書き込み用ロック
write_lock = threading.Lock()


def validate_csv_row(line):
    """
    CSVの1行をバリデーション

    バリデーションルール:
    - no: intであること、nullでないこと
    - name: 20字以内のVARCHAR
    - created_date: YYYY/MM/DD形式
    """
    try:
        # CSVパース
        reader = csv.reader([line])
        row = next(reader)

        if len(row) != 3:
            return {'valid': False, 'error': 'Invalid column count', 'line': line}

        no, name, created_date = row

        # noのバリデーション（nullチェック、int型チェック）
        if not no or no.strip() == '':
            return {'valid': False, 'error': 'no is null', 'line': line}

        try:
            int(no)
        except ValueError:
            return {'valid': False, 'error': 'no is not int', 'line': line}

        # nameのバリデーション（20字以内）
        if len(name) > 20:
            return {'valid': False, 'error': 'name exceeds 20 characters', 'line': line}

        # created_dateのバリデーション（YYYY/MM/DD形式）
        date_pattern = r'^\d{4}/\d{2}/\d{2}$'
        if not re.match(date_pattern, created_date):
            return {'valid': False, 'error': 'created_date is not YYYY/MM/DD format', 'line': line}

        # 日付として有効かチェック
        try:
            datetime.strptime(created_date, '%Y/%m/%d')
        except ValueError:
            return {'valid': False, 'error': 'created_date is invalid date', 'line': line}

        return {'valid': True, 'line': line}

    except Exception as e:
        return {'valid': False, 'error': str(e), 'line': line}


def process_batch(lines):
    """
    バッチ単位で行を処理（スレッドプールで並列実行される）

    Args:
        lines: 処理する行のリスト

    Returns:
        (valid_lines, error_lines) のタプル
    """
    valid_lines = []
    error_lines = []

    for line in lines:
        result = validate_csv_row(line)
        if result['valid']:
            valid_lines.append(result['line'])
        else:
            error_lines.append(result['line'])

    return valid_lines, error_lines


def batch_generator(iterator, batch_size):
    """
    イテレータからバッチ単位でデータを取り出すジェネレータ

    Args:
        iterator: データソース
        batch_size: バッチサイズ

    Yields:
        batch_sizeごとのリスト
    """
    while True:
        batch = list(islice(iterator, batch_size))
        if not batch:
            break
        yield batch


def lambda_handler(event, context):
    """
    大容量CSVファイルのクレンジング処理を行うLambda関数（マルチスレッド版）

    I/O待ちを活用してバリデーション処理を並列化
    """
    try:
        # S3イベントから情報を取得
        if 'Records' in event:
            bucket = event['Records'][0]['s3']['bucket']['name']
            key = event['Records'][0]['s3']['object']['key']
        else:
            bucket = event.get('bucket', os.environ.get('SOURCE_BUCKET'))
            key = event.get('key')

        dest_bucket = os.environ.get('DEST_BUCKET')

        print(f"Processing file: s3://{bucket}/{key}")

        # ファイルサイズを取得
        response = s3_client.head_object(Bucket=bucket, Key=key)
        file_size_mb = response['ContentLength'] / (1024 * 1024)
        print(f"File size: {file_size_mb:.2f} MB")

        # ストリーミング処理でファイルをダウンロード
        response = s3_client.get_object(Bucket=bucket, Key=key)
        body = response['Body']

        # 出力用のストリーム（正常データとエラーデータ）
        valid_stream = BytesIO()
        error_stream = BytesIO()

        line_count = 0
        valid_count = 0
        error_count = 0
        is_header = True

        # ヘッダー処理
        lines_iter = body.iter_lines()
        header_line = next(lines_iter, None)
        if header_line:
            decoded_header = header_line.decode('utf-8')
            valid_stream.write((decoded_header + '\n').encode('utf-8'))
            error_stream.write((decoded_header + '\n').encode('utf-8'))
            line_count += 1

        print(f"Starting parallel validation with {MAX_WORKERS} workers, batch size: {BATCH_SIZE}")

        # デコード済みの行を生成するジェネレータ
        def decoded_lines():
            for line in lines_iter:
                if line:
                    yield line.decode('utf-8')

        # バッチ生成
        batches = batch_generator(decoded_lines(), BATCH_SIZE)

        # スレッドプールで並列処理
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # バッチを並列で処理
            futures = {}
            batch_count = 0

            # 最初のいくつかのバッチを投入
            for _ in range(MAX_WORKERS * 2):  # バッファリング
                try:
                    batch = next(batches)
                    future = executor.submit(process_batch, batch)
                    futures[future] = batch_count
                    batch_count += 1
                except StopIteration:
                    break

            # 結果を受け取りながら新しいバッチを投入
            while futures:
                # 完了したタスクを取得
                for future in as_completed(futures):
                    batch_id = futures.pop(future)
                    valid_lines, error_lines = future.result()

                    # スレッドセーフに書き込み
                    with write_lock:
                        for line in valid_lines:
                            valid_stream.write((line + '\n').encode('utf-8'))
                            valid_count += 1
                            line_count += 1

                        for line in error_lines:
                            error_stream.write((line + '\n').encode('utf-8'))
                            error_count += 1
                            line_count += 1

                    # 進捗表示
                    if line_count % 10000 == 0:
                        print(f"Progress: {line_count:,} lines processed")

                    # 新しいバッチを投入
                    try:
                        batch = next(batches)
                        new_future = executor.submit(process_batch, batch)
                        futures[new_future] = batch_count
                        batch_count += 1
                    except StopIteration:
                        pass  # バッチが尽きた

                    break  # as_completedのループを抜けて再チェック

        # 正常データをS3にアップロード（TransferConfig適用）
        print(f"Uploading valid data ({valid_count:,} rows)...")
        valid_stream.seek(0)
        valid_key = f"cleansed/{key}"
        s3_client.upload_fileobj(
            valid_stream,
            dest_bucket,
            valid_key,
            Config=transfer_config
        )
        print(f"Valid data uploaded: s3://{dest_bucket}/{valid_key}")

        # エラーデータをS3にアップロード（TransferConfig適用）
        print(f"Uploading error data ({error_count:,} rows)...")
        error_stream.seek(0)
        error_key = f"error/{key}"
        s3_client.upload_fileobj(
            error_stream,
            dest_bucket,
            error_key,
            Config=transfer_config
        )
        print(f"Error data uploaded: s3://{dest_bucket}/{error_key}")

        print(f"Completed. Total: {line_count}, Valid: {valid_count}, Error: {error_count}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Success',
                'source': f"s3://{bucket}/{key}",
                'valid_output': f"s3://{dest_bucket}/{valid_key}",
                'error_output': f"s3://{dest_bucket}/{error_key}",
                'file_size_mb': round(file_size_mb, 2),
                'total_lines': line_count,
                'valid_lines': valid_count,
                'error_lines': error_count,
                'workers': MAX_WORKERS,
                'batch_size': BATCH_SIZE
            })
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
