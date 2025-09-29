--- 
title: "AI駆動でZenn執筆環境を構築してみた"
emoji: "🤖"
type: "tech"
topics: ["AI", "Zenn", "GitHub", "CLI"]
published: false
---

## はじめに

こんにちは、AtomikKuireです。
今回は、AIアシスタント（Google Gemini）と対話しながら、Zennで技術ブログを執筆するためのプロジェクト環境を構築するという、少し変わった試みをしてみました。
CLIツールとして統合されたAIを使い、ディレクトリ作成からルール整備、Git操作までをどこまで自動化できるのかを探ります。

## やってみたこと

### 1. プロジェクトの初期化とAIとの対話開始

まず、ローカルマシンにプロジェクトディレクトリ `akimoto-zenn-docs` を作成しました。
ここから先は、すべてAIアシスタントとの対話形式で進めていきます。

### 2. 執筆ルールとテンプレートの作成

AIに以下の指示を出しました。

- `rules` フォルダにZennの執筆ルールファイルを作成
- `templates` フォルダに「バグフィックス」「やってみた系」「紹介系」の3種類の記事テンプレートを作成

AIはこれらの指示を理解し、以下のようなファイルとフォルダを生成しました。

```
D:\Projects\akimoto-zenn-docs\
├───rules\
│   └───zenn-writing-rules.md
└───templates\
    ├───bug-fix-template.md
    ├───introduction-template.md
    └───tried-it-out-template.md
```

さらに、テンプレートに「参考記事」のセクションを追加するように依頼し、これもAIが正確に反映してくれました。

### 3. Gitコミットルールの策定

次に、Gitの運用ルールを固めるため、コミットメッセージのルールを作成するように指示しました。
ここでのポイントは、一般的な開発ルールではなく、「Zennの記事執筆」に特化したルールにしてもらうことです。

最終的に、`type/scope: subject` という形式で、以下のようなルールファイルが作成されました。

- `rules/git-commit-rules.md`

### 4. AIによるGit操作

ここからが本番です。作成したファイル群をGitで管理していきます。

1.  **ブランチ名の変更**:
    AIに「ルールに沿ったブランチ名に変更して」と依頼。しかし、最初のコミット前だったためエラーが発生。AI自身が `git status` を実行して原因を突き止め、「先にコミットが必要です」と提案してくれました。

2.  **最初のコミット**:
    AIが提案した `feat/project: initial setup with rules and templates` というコミットメッセージで最初のコミットを実行。

3.  **ブランチ名の変更（再）**:
    コミット後、再度ブランチ名変更を指示し、`docs/rules-and-templates` への変更に成功。

4.  **リモートへのプッシュ**:
    最後に「プッシュして」と指示。AIは `git remote -v` でリモートリポジトリの存在を確認してから、`git push -u origin docs/rules-and-templates` を実行し、すべての変更をGitHubに反映しました。

## 結果

AIとの対話のみで、Zennの執筆環境が整い、GitHubリポジトリにその内容がプッシュされました。
最終的なプロジェクト構成は以下の通りです。

```
D:\Projects\akimoto-zenn-docs\
├───.git/
├───.gitignore
├───articles/
├───books/
├───node_modules/
├───package.json
├───package-lock.json
├───README.md
├───rules/
│   ├───git-commit-rules.md
│   └───zenn-writing-rules.md
└───templates\
    ├───bug-fix-template.md
    ├───introduction-template.md
    └───tried-it-out-template.md
```

一連の操作はすべてAIが生成したコマンドによって実行され、私は指示を出しただけです。

## 参考記事

- [Zenn CLIで記事・本を管理する方法](https://zenn.dev/zenn/articles/zenn-cli-guide)

## まとめ

AIアシスタントをCLIツールとして利用することで、プロジェクトのセットアップや定型的なGit操作を大幅に効率化できることが分かりました。特に、ルールに基づいたファイル生成や、エラー発生時に自己解決を試みる姿は、単なるコマンド実行ツール以上の可能性を感じさせます。

一方で、こちらの意図を正確に伝えるための指示の出し方（プロンプト）には工夫が必要な場面もありました。

これからの時代、AIをいかに「優秀なアシスタント」として使いこなすかが、開発体験を向上させる鍵になりそうです。
