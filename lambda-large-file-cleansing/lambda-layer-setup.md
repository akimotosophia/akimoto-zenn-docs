# DuckDB Lambda Layer セットアップ

## Lambda Layer ARN

DuckDB用のプリビルドLambda Layerを使用します。

### Python 3.12 (x86_64) - ap-northeast-1 (東京リージョン)

```
arn:aws:lambda:ap-northeast-1:911510765542:layer:duckdb-python312-x86_64:1
```

**DuckDBバージョン**: 0.10.1

## セットアップ手順

### 1. Lambda関数にレイヤーを追加（AWS CLI）

```bash
aws lambda update-function-configuration \
  --function-name your-function-name \
  --layers arn:aws:lambda:ap-northeast-1:911510765542:layer:duckdb-python312-x86_64:1
```

### 2. Lambda関数にレイヤーを追加（Terraform）

```hcl
resource "aws_lambda_function" "cleansing_function" {
  function_name = "csv-cleansing-function"
  runtime       = "python3.12"
  handler       = "app_duckdb.lambda_handler"

  # DuckDB Lambda Layerを追加
  layers = [
    "arn:aws:lambda:ap-northeast-1:911510765542:layer:duckdb-python312-x86_64:1"
  ]

  # メモリとタイムアウトを増やす（DuckDBは大量データ処理に最適）
  memory_size = 3008  # 3GB（最大vCPU性能）
  timeout     = 900   # 15分

  ephemeral_storage {
    size = 10240  # 10GB（大容量CSVファイル用）
  }
}
```

### 3. Lambda関数にレイヤーを追加（AWS Console）

1. Lambda関数を開く
2. 「Layers」セクションで「Add a layer」をクリック
3. 「Specify an ARN」を選択
4. ARNを入力: `arn:aws:lambda:ap-northeast-1:911510765542:layer:duckdb-python312-x86_64:1`
5. 「Verify」→「Add」をクリック

## 他のリージョン・アーキテクチャのARN

完全なARNリストは以下で確認できます：
https://github.com/bengeois/aws-layer-duckdb-python/blob/main/data/arns.json

### 主要リージョン (Python 3.12, x86_64)

| リージョン | ARN |
|-----------|-----|
| us-east-1 | `arn:aws:lambda:us-east-1:911510765542:layer:duckdb-python312-x86_64:1` |
| us-west-2 | `arn:aws:lambda:us-west-2:911510765542:layer:duckdb-python312-x86_64:1` |
| ap-northeast-1 | `arn:aws:lambda:ap-northeast-1:911510765542:layer:duckdb-python312-x86_64:1` |

## 推奨設定

DuckDBを使った大容量ファイル処理には以下の設定を推奨：

- **メモリ**: 3008MB（最大vCPU性能を引き出す）
- **タイムアウト**: 900秒（15分）
- **エフェメラルストレージ**: 10GB（一時ファイル用）
- **アーキテクチャ**: x86_64（DuckDBの最適化が進んでいる）

## 参考

- GitHub Repository: https://github.com/bengeois/aws-layer-duckdb-python
- DuckDB公式ドキュメント: https://duckdb.org/docs/
