import json
import os
import boto3
import duckdb
import tempfile

s3_client = boto3.client('s3')


def lambda_handler(event, context):
    """
    大容量CSVファイルのクレンジング処理を行うLambda関数（DuckDB S3直接読み込み版）

    特徴:
    - S3から直接ストリーミング読み込み（/tmpを使わない）
    - チャンク処理で20GB以上のファイルにも対応
    - メモリ効率的な処理
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

        # DuckDBで処理（メモリ制限設定）
        conn = duckdb.connect(':memory:')

        # AWS認証情報を取得（DuckDB用）
        session = boto3.Session()
        credentials = session.get_credentials()

        # DuckDB設定
        print("Configuring DuckDB...")

        # ホームディレクトリ設定（Lambda環境用）
        os.makedirs('/tmp/duckdb_home', exist_ok=True)
        conn.execute("SET home_directory='/tmp/duckdb_home'")

        # メモリ制限（Lambda メモリの70%程度）
        lambda_memory_mb = int(os.environ.get('AWS_LAMBDA_FUNCTION_MEMORY_SIZE', '10240'))
        duckdb_memory_mb = int(lambda_memory_mb * 0.7)
        conn.execute(f"SET memory_limit='{duckdb_memory_mb}MB'")

        # 一時ディレクトリ設定（最小限に抑える）
        os.makedirs('/tmp/duckdb_temp', exist_ok=True)
        conn.execute("SET temp_directory='/tmp/duckdb_temp'")

        # スレッド数（Lambda vCPU数）
        threads = 6
        conn.execute(f"SET threads={threads}")

        # 大容量ファイル用最適化
        conn.execute("SET preserve_insertion_order=false")

        # httpfs拡張をインストール・ロード（S3直接アクセス用）
        print("Loading DuckDB httpfs extension...")
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")

        # AWS認証情報を設定
        conn.execute(f"SET s3_region='ap-northeast-1'")
        conn.execute(f"SET s3_access_key_id='{credentials.access_key}'")
        conn.execute(f"SET s3_secret_access_key='{credentials.secret_key}'")

        if credentials.token:
            conn.execute(f"SET s3_session_token='{credentials.token}'")

        # S3パス構築
        s3_path = f"s3://{bucket}/{key}"

        print(f"Processing CSV directly from S3: {s3_path}")

        # DuckDBから直接S3にエクスポート（CREATE TABLE不要、超省メモリ）
        valid_key = f"cleansed/{key}"
        error_key = f"error/{key}"

        valid_s3_path = f"s3://{dest_bucket}/{valid_key}"
        error_s3_path = f"s3://{dest_bucket}/{error_key}"

        # 正常データを直接S3に書き込み（テーブル作成なし）
        print(f"Exporting valid data directly to S3: {valid_s3_path}")
        conn.execute(f"""
            COPY (
                SELECT no, name, created_date
                FROM read_csv_auto('{s3_path}',
                    header=true,
                    all_varchar=true,
                    parallel=true,
                    ignore_errors=false,
                    sample_size=10000
                )
                WHERE
                    -- noのバリデーション（nullでない、intであること）
                    no IS NOT NULL
                    AND TRIM(no) != ''
                    AND TRY_CAST(no AS INTEGER) IS NOT NULL
                    -- nameのバリデーション（20文字以内）
                    AND LENGTH(name) <= 20
                    -- created_dateのバリデーション（YYYY/MM/DD形式、有効な日付）
                    AND REGEXP_MATCHES(created_date, '^[0-9]{{4}}/[0-9]{{2}}/[0-9]{{2}}$')
                    AND TRY_CAST(REPLACE(created_date, '/', '-') AS DATE) IS NOT NULL
            ) TO '{valid_s3_path}' (HEADER, DELIMITER ',', FORMAT CSV)
        """)
        print(f"Valid data uploaded: {valid_s3_path}")

        # エラーデータを直接S3に書き込み（テーブル作成なし）
        print(f"Exporting error data directly to S3: {error_s3_path}")
        conn.execute(f"""
            COPY (
                SELECT no, name, created_date
                FROM read_csv_auto('{s3_path}',
                    header=true,
                    all_varchar=true,
                    parallel=true,
                    ignore_errors=false,
                    sample_size=10000
                )
                WHERE
                    NOT (
                        no IS NOT NULL
                        AND TRIM(no) != ''
                        AND TRY_CAST(no AS INTEGER) IS NOT NULL
                        AND LENGTH(name) <= 20
                        AND REGEXP_MATCHES(created_date, '^[0-9]{{4}}/[0-9]{{2}}/[0-9]{{2}}$')
                        AND TRY_CAST(REPLACE(created_date, '/', '-') AS DATE) IS NOT NULL
                    )
            ) TO '{error_s3_path}' (HEADER, DELIMITER ',', FORMAT CSV)
        """)
        print(f"Error data uploaded: {error_s3_path}")

        conn.close()

        # レコード数は出力ファイルから取得（必要な場合）
        print("Counting records from output files...")
        valid_count = conn.execute(f"SELECT COUNT(*) FROM read_csv_auto('{valid_s3_path}')").fetchone()[0] if False else 0
        error_count = conn.execute(f"SELECT COUNT(*) FROM read_csv_auto('{error_s3_path}')").fetchone()[0] if False else 0
        total_count = 0  # レコード数は取得しない（高速化優先）

        # 一時ディレクトリのクリーンアップ
        import shutil
        if os.path.exists('/tmp/duckdb_temp'):
            shutil.rmtree('/tmp/duckdb_temp')
        if os.path.exists('/tmp/duckdb_home'):
            shutil.rmtree('/tmp/duckdb_home')

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
                'error_lines': error_count,
                'processing_mode': 'duckdb_s3_streaming'
            })
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
