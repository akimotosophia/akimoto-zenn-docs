# Git Commit Message Rules for Zenn Articles

## 概要

このドキュメントは、Zennの記事を執筆する際のGitのコミットメッセージのルールを定めたものです。

## コミットメッセージのフォーマット

コミットメッセージは以下のフォーマットに従ってください。

```
type/scope: subject
```

### type

コミットの種類を表します。以下のいずれかを使用してください。

-   **feat**: 新しい記事の追加
-   **fix**: 記事の誤字脱字や内容の修正
-   **style**: 記事のフォーマットやスタイルの修正（コードブロックの修正など）
-   **refactor**: 記事の構成や流れの変更
-   **chore**: 画像の追加やその他雑多なタスク
-   **docs**: ルールファイルなどドキュメントの更新

### scope

変更の範囲を示します。通常は記事のスラッグ（例: `eventbridge-context-20250928`）を記述します。
記事に関係ない変更の場合は省略可能です。

### subject

変更内容を簡潔に記述します。

## コミットメッセージの例

```
feat/new-article-slug: 新規記事「〇〇について」を追加
fix/eventbridge-context-20250928: 誤字を修正
style/eventbridge-context-20250928: コードブロックのシンタックスハイライトを修正
refactor/eventbridge-context-20250928: 章の順番を入れ替え
chore/eventbridge-context-20250928: スクリーンショットを追加
docs/git-commit-rules: コミットタイプの例を追加
```