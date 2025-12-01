---
title: "AWS Lambdaでどこまで大容量ファイルのクレンジングができるか試してみた"
emoji: "🚀"
type: "tech" # tech: 技術記事 / idea: アイデア
topics: ["AWS", "Lambda", "サーバーレス", "データ処理"]
published: true
---

## はじめに

AWS Lambdaはサーバーレスでコードを実行できるコンピューティングサービスです。そのため、ちょっとしたファイルのETL処理の実行基盤としても採用されることがあります。しかし、Lambdaにはいくつか制限があり、データサイズやスループットなどの非機能要件を把握せずLambdaを採用してしまうと落とし穴にはまることがあります。

この記事ではLambdaでどこまでのファイル、処理ができるのかを検証してみようと思います。

## AWS Lambdaの制限について

AWS Lambdaには以下のような制限があります：

- **実行時間**: 最大15分
- **メモリ**: 128MB〜10,240MB（10GB）
- **一時ストレージ(/tmp)**: 最大10GB
- **デプロイパッケージサイズ**: 圧縮時50MB、展開時250MB

これらの制限の中で、どこまで大容量のファイル処理が可能なのかを検証します。

## やってみたこと

### 検証環境の準備

1. Lambda関数の設定
   - メモリ:10GB
   - 一時ストレージ:5GB
   - タイムアウト:15分
   - ランタイム:python3.12
   - レイヤー:DuckDB([PyPI](https://pypi.org/project/duckdb/#files) 圧縮時18.5MB ※2025年11月12日時点)

2. テストデータの準備
   - ファイルサイズ:24GB
   - データ形式:csv
   - カラム:no(int),name(str),created_by(date)

### クレンジング処理の実装

DuckDBの並列処理・チャンク処理を活かし、S3から直接読み込みS3に直接書き込む形で実装してみました。

::: details 実装例
```python
# クレンジング処理のコード例
import json
import os
import boto3
from boto3.s3.transfer import TransferConfig
import duckdb
import tempfile

# S3転送設定
transfer_config = TransferConfig(
    multipart_threshold=50 * 1024 * 1024,
    max_concurrency=10,
    multipart_chunksize=10 * 1024 * 1024,
    num_download_attempts=5,
    max_io_queue=1000,
    io_chunksize=256 * 1024,
    use_threads=True
)

s3_client = boto3.client('s3')


def lambda_handler(event, context):
    """
    大容量CSVファイルのクレンジング処理を行うLambda関数（DuckDB S3直接読み込み版）
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

        # 一時ディレクトリ設定
        os.makedirs('/tmp/duckdb_temp', exist_ok=True)
        conn.execute("SET temp_directory='/tmp/duckdb_temp'")

        # スレッド数（Lambda vCPU数）
        threads = 6
        conn.execute(f"SET threads={threads}")

        # 順序保証なしにし、並列性アップ
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

        # DuckDBから直接S3にエクスポート
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
        # レコード数は取得しない（高速化優先）
        print("Counting records from output files...")
        #valid_count = conn.execute(f"SELECT COUNT(*) FROM read_csv_auto('{valid_s3_path}')")
        #error_count = conn.execute(f"SELECT COUNT(*) FROM read_csv_auto('{error_s3_path}')")
        #total_count = valid_count + error_count

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
```
:::

## 結果

- 実行時間:12分33秒（うち、正常データ出力が6分24秒）
- メモリ使用量:726MB

結果のように、24GBのデータも1GB以下のメモリ、5GB以下の一次領域（実際にはもっと少ない可能性もあり）で処理を完了することができました。

### 注意点

- Lambda内部に全データを保持せず、ストリーミング的に処理しているため、ランダムアクセスが発生するような処理には適用が難しいと思います。
  - 今回はレコード数のカウントを省略したのも、出力ファイルから再度読み込む必要がありLambdaの実行時間の制約上に引っかかるためです。
- 重複排除やグルーピングなどをする場合もtmp領域が十分にないとDisk容量不足エラーになる可能性があります。EFSの使用を検討しましょう。
- Lambdaのメモリ数を下げてしまうと、vCPUも下がってしまい並列処理ができなくなり遅くなります。最大の6vCPUを維持するには9GBくらいは割り当てないといけないようです。
- 安定した処理を優先するならそもそもFargateを検討しましょう。

## 参考記事

- [Lambda クォータ](https://docs.aws.amazon.com/ja_jp/lambda/latest/dg/gettingstarted-limits.html)
