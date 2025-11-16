import json
import os
import boto3
import duckdb
import tempfile

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """
    大容量CSVファイルのクレンジング処理を行うLambda関数（DuckDB版）

    従来のPython行単位処理に比べ、DuckDBのSQL並列処理により大幅に高速化
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

        # 一時ファイルにダウンロード（DuckDBは直接ファイルを読むと高速）
        with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.csv') as tmp_input:
            input_path = tmp_input.name
            print(f"Downloading from S3 to {input_path}...")
            s3_client.download_fileobj(
                Bucket=bucket,
                Key=key,
                Fileobj=tmp_input
            )

        print(f"Downloaded to: {input_path}")

        # DuckDBで処理
        conn = duckdb.connect(':memory:')

        # CSVを読み込み（型推論を無効にして全て文字列として読む）
        print("Loading CSV into DuckDB...")
        conn.execute(f"""
            CREATE TABLE raw_data AS
            SELECT * FROM read_csv_auto('{input_path}',
                header=true,
                all_varchar=true,
                parallel=true,
                ignore_errors=false
            )
        """)

        total_count = conn.execute("SELECT COUNT(*) FROM raw_data").fetchone()[0]
        print(f"Total rows loaded: {total_count:,}")

        # バリデーション付きでデータを分割
        # 正常データ: すべてのバリデーションをパスするデータ
        print("Validating data with DuckDB SQL...")
        conn.execute("""
            CREATE TABLE valid_data AS
            SELECT no, name, created_date
            FROM raw_data
            WHERE
                -- noのバリデーション（nullでない、intであること）
                no IS NOT NULL
                AND TRIM(no) != ''
                AND TRY_CAST(no AS INTEGER) IS NOT NULL
                -- nameのバリデーション（20文字以内）
                AND LENGTH(name) <= 20
                -- created_dateのバリデーション（YYYY/MM/DD形式、有効な日付）
                AND REGEXP_MATCHES(created_date, '^[0-9]{4}/[0-9]{2}/[0-9]{2}$')
                AND TRY_CAST(REPLACE(created_date, '/', '-') AS DATE) IS NOT NULL
        """)

        # エラーデータ: 正常データ以外
        conn.execute("""
            CREATE TABLE error_data AS
            SELECT no, name, created_date
            FROM raw_data
            WHERE
                NOT (
                    no IS NOT NULL
                    AND TRIM(no) != ''
                    AND TRY_CAST(no AS INTEGER) IS NOT NULL
                    AND LENGTH(name) <= 20
                    AND REGEXP_MATCHES(created_date, '^[0-9]{4}/[0-9]{2}/[0-9]{2}$')
                    AND TRY_CAST(REPLACE(created_date, '/', '-') AS DATE) IS NOT NULL
                )
        """)

        valid_count = conn.execute("SELECT COUNT(*) FROM valid_data").fetchone()[0]
        error_count = conn.execute("SELECT COUNT(*) FROM error_data").fetchone()[0]

        print(f"Valid rows: {valid_count:,}, Error rows: {error_count:,}")

        # 一時ファイルに出力
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as tmp_valid:
            valid_path = tmp_valid.name

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as tmp_error:
            error_path = tmp_error.name

        # DuckDBから直接CSVに書き出し（超高速）
        print("Exporting valid data...")
        conn.execute(f"""
            COPY valid_data TO '{valid_path}' (HEADER, DELIMITER ',')
        """)

        print("Exporting error data...")
        conn.execute(f"""
            COPY error_data TO '{error_path}' (HEADER, DELIMITER ',')
        """)

        conn.close()

        # S3にアップロード
        print("Uploading valid data to S3...")
        valid_key = f"cleansed/{key}"
        with open(valid_path, 'rb') as f:
            s3_client.upload_fileobj(f, dest_bucket, valid_key)
        print(f"Valid data uploaded: s3://{dest_bucket}/{valid_key}")

        print("Uploading error data to S3...")
        error_key = f"error/{key}"
        with open(error_path, 'rb') as f:
            s3_client.upload_fileobj(f, dest_bucket, error_key)
        print(f"Error data uploaded: s3://{dest_bucket}/{error_key}")

        # 一時ファイル削除
        os.unlink(input_path)
        os.unlink(valid_path)
        os.unlink(error_path)

        print(f"Completed. Total: {total_count}, Valid: {valid_count}, Error: {error_count}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Success',
                'source': f"s3://{bucket}/{key}",
                'valid_output': f"s3://{dest_bucket}/{valid_key}",
                'error_output': f"s3://{dest_bucket}/{error_key}",
                'file_size_mb': round(file_size_mb, 2),
                'total_lines': total_count,
                'valid_lines': valid_count,
                'error_lines': error_count
            })
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
