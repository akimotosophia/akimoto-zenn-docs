---
title: "EventBridge Schedulerのcontext属性でLambdaの安全な再処理を実現する方法"
emoji: "🔄"
type: "tech" # tech: 技術記事 / idea: アイデア
topics: ["AWS", "EventBridge", "Scheduler", "再処理", "アーキテクチャ"]
published: true
---

## TL;DR

EventBridge SchedulerでLambdaの安全な再処理を実現するには、**SQSとcontext属性のexecution_idを活用してDynamoDBで実行状態を管理する**のが最もシンプルで確実な方法です。

- ✅ AWSが提供するexecution_idで一意性を担保
- ✅ DLQからの再処理でもIDが変わらず安全
- ✅ 重複実行や競合状態を自動回避
- ✅ 追跡可能性とモニタリングが向上

## はじめに

EventBridge Schedulerを使った定期処理やバッチ処理で、「処理が失敗した時に安全に再実行したい」「重複実行を防ぎたい」という要件は多くのプロジェクトで発生します。

従来は「Lambda側で無理やり一意キーを生成する」「タイムスタンプベースのIDを作る」など、独自の仕組みを実装していたケースも多いのではないでしょうか。

EventBridge Schedulerのcontext属性を活用することで、AWSが提供する仕組みを使って、よりシンプルで確実な再処理アーキテクチャを構築できます。

## EventBridge Schedulerのcontext属性とは

EventBridge Schedulerのcontext属性は、スケジュール実行時に自動的に付与される実行コンテキスト情報です。特に重要なのが`execution_id`です。

### execution_idの特徴

- **実行毎に一意**：同じスケジュールでも実行のたびに異なるIDが発行される
- **再処理時は不変**：DLQからの再処理でも同じIDが維持される
- **AWS管理**：開発者が生成ロジックを実装する必要がない

```json
{
  "aws.scheduler.execution-id": "5068e532-bca5-4efc-be63-c93601fe6344",
  "aws.scheduler.schedule-arn": "arn:aws:scheduler:...",
  "aws.scheduler.scheduled-time": ""2025-10-07T15:33:16Z"
}
```

## アーキテクチャ概要

### システム構成図

<!-- 概念図をdraw.ioで作成予定 -->
![アーキテクチャ概要図](/images/eventbridge_retry.drawio.png)

### 処理フロー

1. EventBridge Schedulerがcontext属性付きでSQSにメッセージ送信
2. LambdaがSQSメッセージを受信
3. execution_idをキーにDynamoDBで実行状態をチェック
4. 未実行または失敗状態の場合のみ処理を実行
5. 処理結果をDynamoDBに記録

## 実装例：EventBridge + SQS + Lambda

### CloudFormationテンプレート(DynamoDB部分は未検証)

```yaml
AWSTemplateFormatVersion: '2010-09-09'
Description: 'EventBridge Scheduler with context attributes for safe reprocessing'

Resources:
  # SQS Queue (Main Queue)
  ProcessingQueue:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: processing-queue
      VisibilityTimeoutSeconds: 300
      MessageRetentionPeriod: 604800  # 7 days
      RedrivePolicy:
        deadLetterTargetArn: !GetAtt ProcessingDLQ.Arn
        maxReceiveCount: 3

  # SQS DLQ
  ProcessingDLQ:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: processing-dlq
      MessageRetentionPeriod: 1209600  # 14 days

  # EventBridge Scheduler Role
  SchedulerRole:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              Service: scheduler.amazonaws.com
            Action: sts:AssumeRole
      Policies:
        - PolicyName: SQSSendMessagePolicy
          PolicyDocument:
            Version: '2012-10-17'
            Statement:
              - Effect: Allow
                Action:
                  - sqs:SendMessage
                Resource: !GetAtt ProcessingQueue.Arn

  # EventBridge Schedule
  ProcessingSchedule:
    Type: AWS::Scheduler::Schedule
    Properties:
      Name: processing-schedule
      ScheduleExpression: 'rate(1 hour)'  # Every hour
      FlexibleTimeWindow:
        Mode: 'OFF'
      Target:
        Arn: !GetAtt ProcessingQueue.Arn
        RoleArn: !GetAtt SchedulerRole.Arn
        Input: |
          {
            "context": {
              "execution_id": "<aws.scheduler.execution-id>",
              "schedule_arn": "<aws.scheduler.schedule-arn>",
              "scheduled_time": "<aws.scheduler.scheduled-time>"
            },
            "payload": {
              "taskType": "data-processing"
            }
          }

  # Lambda Execution Role
  LambdaExecutionRole:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              Service: lambda.amazonaws.com
            Action: sts:AssumeRole
      ManagedPolicyArns:
        - arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
      Policies:
        - PolicyName: SQSAndDynamoDBAccess
          PolicyDocument:
            Version: '2012-10-17'
            Statement:
              - Effect: Allow
                Action:
                  - sqs:ReceiveMessage
                  - sqs:DeleteMessage
                  - sqs:GetQueueAttributes
                Resource:
                  - !GetAtt ProcessingQueue.Arn
                  - !GetAtt ProcessingDLQ.Arn
              - Effect: Allow
                Action:
                  - dynamodb:GetItem
                  - dynamodb:PutItem
                  - dynamodb:UpdateItem
                Resource: !GetAtt ExecutionStateTable.Arn

  # DynamoDB Table for execution state
  ExecutionStateTable:
    Type: AWS::DynamoDB::Table
    Properties:
      TableName: execution-state
      BillingMode: PAY_PER_REQUEST
      AttributeDefinitions:
        - AttributeName: execution_id
          AttributeType: S
      KeySchema:
        - AttributeName: execution_id
          KeyType: HASH
      TimeToLiveSpecification:
        AttributeName: ttl
        Enabled: true

  # Lambda Function
  ProcessingFunction:
    Type: AWS::Lambda::Function
    Properties:
      FunctionName: processing-function
      Runtime: python3.9
      Handler: index.handler
      Role: !GetAtt LambdaExecutionRole.Arn
      Environment:
        Variables:
          EXECUTION_STATE_TABLE: !Ref ExecutionStateTable
      Code:
        ZipFile: |
          def handler(event, context):
              # Lambda function implementation is out of scope
              # Implement idempotency control logic using execution_id
              return {'statusCode': 200}

  # SQS Trigger
  SQSEventSourceMapping:
    Type: AWS::Lambda::EventSourceMapping
    Properties:
      EventSourceArn: !GetAtt ProcessingQueue.Arn
      FunctionName: !Ref ProcessingFunction
      BatchSize: 1
      MaximumBatchingWindowInSeconds: 0

Outputs:
  QueueURL:
    Description: 'SQS Queue URL'
    Value: !Ref ProcessingQueue

  DLQUrl:
    Description: 'DLQ URL'
    Value: !Ref ProcessingDLQ

  ExecutionStateTableName:
    Description: 'DynamoDB Table Name for execution state'
    Value: !Ref ExecutionStateTable
```

## 運用での活用方法

### 再処理の判断フロー

1. **正常終了済み**：DynamoDBに`SUCCESS`状態で記録済み → スキップ
2. **実行中**：DynamoDBに`RUNNING`状態で記録済み → スキップ（重複実行防止）
3. **失敗状態**：DynamoDBに`FAILED`状態で記録済み → 再処理実行
4. **未実行**：DynamoDBに記録なし → 初回実行

### DLQとの連携

- DLQから手動で再処理する場合も、同じexecution_idが維持される
- Lambda側で状態をチェックして適切に処理される
- 何度DLQから再実行しても安全

### 万が一の重複配信への対応

SQSの重複配信やEventBridgeの重複実行が発生しても：
- 同じexecution_idで複数のメッセージが来る
- 最初の処理が`RUNNING`状態を記録
- 後続の重複メッセージは自動的にスキップされる

## 追跡可能性とモニタリング

execution_idを活用することで、以下の運用価値を得られます：

### 実行履歴の追跡

- **初回実行時刻**：いつ最初に実行されたか
- **再処理回数**：何回再処理されたか
- **異なるメッセージ数**：今までで何回異なるメッセージを実行したか

### アラート設定

- 同じexecution_idで複数回`FAILED`が記録される
- 処理時間が異常に長い（`RUNNING`状態が長時間継続）
- DLQにメッセージが蓄積される

## よくある失敗パターンと対処

### 1. context属性のタイプミス

**❌ 失敗例：**
```yaml
Input: |
  {
    "execution_id": "<aws.scheduler.execution_id>"  # アンダースコアの使用
  }
```

**結果：** 毎回同じ文字列リテラル`<aws.scheduler.execution_id>`が設定され、すべての実行で同じキーになる

**✅ 正しい記述：**
```yaml
Input: |
  {
    "execution_id": "<aws.scheduler.execution-id>"  # ハイフンの使用
  }
```

### 2. SQSメッセージIDを重複キーに使用

**❌ 失敗例：**
```python
# Lambda内でSQSメッセージIDを重複チェックのキーに使用
message_id = event['Records'][0]['messageId']
# DLQからの再実行で新しいメッセージIDが生成される
```

**問題：**
- DLQからの再実行で新しいメッセージIDが生成される
- SQSの重複配信でも異なるメッセージIDが付与される
- 結果として重複検知ができない

**✅ 正しい実装：**
```python
# context属性のexecution_idを使用
execution_id = json.loads(event['Records'][0]['body'])['context']['execution_id']
# DLQからの再実行でも同じIDが維持される
```

### 3. DynamoDBの条件付き書き込みを使わない

**❌ 失敗例：**
```python
# 単純なPutItem（競合状態のリスク）
dynamodb.put_item(
    TableName='execution-state',
    Item={'execution_id': execution_id, 'status': 'RUNNING'}
)
```

**✅ 正しい実装：**
```python
# ConditionExpressionで競合回避
dynamodb.put_item(
    TableName='execution-state',
    Item={'execution_id': execution_id, 'status': 'RUNNING'},
    ConditionExpression='attribute_not_exists(execution_id)'
)
```

## まとめ

EventBridge Schedulerのcontext属性を活用することで、以下を実現できます：

### 技術的メリット
- AWSが提供する一意キー（execution_id）で確実性を担保
- Lambda側での複雑な一意キー生成ロジックが不要
- DynamoDBのConditionExpressionで競合状態を自動回避

### 運用的メリット
- DLQからの再処理が安全に実行可能
- 重複実行や競合状態を自動防止
- 実行履歴の追跡とモニタリングが容易

### 注意点
- context属性のプレースホルダーは正確に記述する
- SQSメッセージIDではなくexecution_idを使用する
- DynamoDBでは必ず条件付き書き込みを使用する

この方法により、EventBridge Schedulerを使った堅牢で運用しやすい再処理アーキテクチャを構築できます。

## 補足
1分間隔で動かしてみたんですが、execution_idの2セクション目が地味に変わっただけでした。UUIDのフォーマット的に前半はUNIX時間らしいです。時間でソートできそうですね。
- ![alt text](/images/eventbridge_retry_example1.png)
- ![alt text](/images/eventbridge_retry_example2.png)

## 参考記事

- [EventBridge スケジューラでのコンテキスト属性の追加](https://docs.aws.amazon.com/ja_jp/scheduler/latest/UserGuide/managing-schedule-context-attributes.html)
- [UUID](https://ja.wikipedia.org/wiki/UUID)
