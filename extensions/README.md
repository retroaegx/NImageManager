# NIM NovelAI Transfer Extensions

## Chrome
- `extensions/chrome` を「パッケージ化されていない拡張機能」として読み込み

## Firefox Desktop / Android
- `extensions/firefox` を読み込み
- Android Firefox は署名配布前提のため、実機導入方法は通常の AMO / 自前署名フローに合わせてください

## 動作
- 初回インストール時に設定画面を開く
- ドメインと認証情報を設定画面で入力してログイン
- NovelAI の Save / 保存 ボタンの右隣に「転送」ボタンを挿入
- 転送成功時は右上に通知
- トークン失効時は右上に通知し、設定画面を開ける
