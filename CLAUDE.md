# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## プロジェクト概要

このプロジェクトは、Zenn（技術記事プラットフォーム）での記事執筆を管理するリポジトリです。Zenn CLIを使用して記事の作成、プレビュー、管理を行います。

## 開発コマンド

### Zenn CLI コマンド
- `npx zenn preview` - 記事のブラウザプレビュー
- `npx zenn new:article --slug <記事タイトルの英語名>-<YYYYMMDD>` - 新しい記事作成
- `npx zenn list:articles` - 記事一覧表示
- `npx zenn new:book` - 新しい本を追加
- `npx zenn list:books` - 本の一覧表示

## プロジェクト構成

- `articles/` - 記事ファイル（Markdown）
- `templates/` - 記事テンプレート
  - `introduction-template.md` - 入門記事テンプレート
  - `bug-fix-template.md` - バグ修正記事テンプレート
  - `tried-it-out-template.md` - 試してみた記事テンプレート
- `rules/` - プロジェクト運用ルール
  - `zenn-writing-rules.md` - 記事執筆ルール
  - `git-commit-rules.md` - コミットメッセージルール

## 記事作成の重要なルール

### 記事メタデータフォーマット
```yaml
---
title: "SEO対策が見込める内容に即したキャッチーなタイトル"
emoji: "🐡" # コンテンツの内容を最もわかりやすく表す絵文字
type: "tech" # tech: 技術記事 / idea: アイデア
topics: [] # コンテンツの内容をもっと反映する一般的によく使われているタグ
published: false
---
```

### コミットメッセージフォーマット
`type/scope: subject`

- **type**: `feat`, `fix`, `style`, `refactor`, `chore`, `docs`
- **scope**: 記事のスラッグ（例: `eventbridge-context-20250928`）
- **subject**: 変更内容を簡潔に記述

例:
- `feat/new-article-slug: 新規記事「〇〇について」を追加`
- `fix/eventbridge-context-20250928: 誤字を修正`

### 記事執筆スタイル
- ですます調で統一
- 専門用語には注釈を入れる
- 読者が再現可能な手順を記載
- 参考記事やドキュメントへのリンクを記載
- コードブロックには言語を指定
- 絵文字を適切に使用してエンゲージメントを高める