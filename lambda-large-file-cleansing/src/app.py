import json
import os
import boto3
from boto3.s3.transfer import TransferConfig
import csv
from io import BytesIO, StringIO
import re
from datetime import datetime

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

def lambda_handler(event, context):
    """
    大容量CSVファイルのクレンジング処理を行うLambda関数
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

        # ストリーミング処理でファイルをクレンジング
        response = s3_client.get_object(Bucket=bucket, Key=key)
        body = response['Body']

        # 出力用のストリーム（正常データとエラーデータ）
        valid_stream = BytesIO()
        error_stream = BytesIO()

        valid_writer = csv.writer(StringIO())
        error_writer = csv.writer(StringIO())

        line_count = 0
        valid_count = 0
        error_count = 0
        is_header = True

        # 行単位でストリーミング処理
        for line in body.iter_lines():
            if line:
                line_count += 1
                decoded_line = line.decode('utf-8')

                # ヘッダー行
                if is_header:
                    valid_stream.write((decoded_line + '\n').encode('utf-8'))
                    error_stream.write((decoded_line + '\n').encode('utf-8'))
                    is_header = False
                    continue

                # バリデーション処理
                validation_result = validate_csv_row(decoded_line)

                if validation_result['valid']:
                    valid_stream.write((decoded_line + '\n').encode('utf-8'))
                    valid_count += 1
                else:
                    error_stream.write((decoded_line + '\n').encode('utf-8'))
                    error_count += 1

        # 正常データをS3にアップロード（TransferConfig適用）
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
                'error_lines': error_count
            })
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        raise

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
            return {'valid': False, 'error': 'Invalid column count'}

        no, name, created_date = row

        # noのバリデーション（nullチェック、int型チェック）
        if not no or no.strip() == '':
            return {'valid': False, 'error': 'no is null'}

        try:
            int(no)
        except ValueError:
            return {'valid': False, 'error': 'no is not int'}

        # nameのバリデーション（20字以内）
        if len(name) > 20:
            return {'valid': False, 'error': 'name exceeds 20 characters'}

        # created_dateのバリデーション（YYYY/MM/DD形式）
        date_pattern = r'^\d{4}/\d{2}/\d{2}$'
        if not re.match(date_pattern, created_date):
            return {'valid': False, 'error': 'created_date is not YYYY/MM/DD format'}

        # 日付として有効かチェック
        try:
            datetime.strptime(created_date, '%Y/%m/%d')
        except ValueError:
            return {'valid': False, 'error': 'created_date is invalid date'}

        return {'valid': True}

    except Exception as e:
        return {'valid': False, 'error': str(e)}
