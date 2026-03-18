# NIM NovelAI Transfer Extensions

NImageManager に NovelAI の表示中画像を転送するための拡張です。

## 配布物

- `extensions/chrome/`
  - Chrome 用の展開済みソースです。
  - ローカル導入時はこのフォルダをそのまま読み込みます。
- `extensions/chrome.zip`
  - Chrome Web Store 提出・配布用に固めた zip です。
- `extensions/firefox/`
  - Firefox 用のソースです。
  - デスクトップで一時読み込みして確認したい場合に使います。
- `extensions/firefox_local/`
  - Firefox のローカル確認用ソースです。
  - 必要なければ通常は使いません。
- `extensions/nim_firefox_addons.xpi`
  - Firefox 用の配布パッケージです。
  - Firefox Desktop / Android Firefox へ入れる場合はこれを使います。

## Chrome の入れ方

1. Chrome で `chrome://extensions` を開きます。
2. 右上の「デベロッパー モード」を有効にします。
3. 「パッケージ化されていない拡張機能を読み込む」を押します。
4. `extensions/chrome/` フォルダを選びます。

## Firefox Desktop の入れ方

### 一時読み込み

1. Firefox で `about:debugging#/runtime/this-firefox` を開きます。
2. 「一時的なアドオンを読み込む」を押します。
3. `extensions/firefox/manifest.json` を選びます。

※ 一時読み込みは Firefox 再起動で消えます。

### 通常インストール

1. `extensions/nim_firefox_addons.xpi` を用意します。
2. Firefox に `.xpi` をドラッグ＆ドロップするか、ファイルとして開きます。
3. 確認ダイアログで追加します。

## Android Firefox の入れ方

1. `extensions/nim_firefox_addons.xpi` を端末に保存します。
2. Android Firefox で「設定」→「Firefox について」を開きます。
3. Firefox ロゴを 5 回連続でタップします。
4. 「設定」に戻り、「Install Extension from File」を開きます。
5. 保存した `.xpi` を選択します。
6. 確認ダイアログで追加します。

※ Android Firefox でファイルから導入する場合は、署名済み `.xpi` が必要です。

## 初回設定

1. 拡張インストール後に設定画面を開きます。
2. NImageManager のドメインを入力します。
   - API のフルパスではなく、`https://example.com` のようにドメインだけ入力してください。
3. NImageManager のユーザー名とパスワードでログインします。
4. `Settings` と `Login` が成功表示になれば準備完了です。

## 動作

- NovelAI の画像ページで、下部アクション列の保存ボタン付近に転送ボタンを追加します。
- 転送ボタンを押すと、現在表示中の画像を NImageManager に送信します。
- 転送成功時は右上に通知を表示します。
- ログインが切れている場合は再ログインを促します。

## 注意

- 画像転送はユーザー操作時のみ行います。
- 接続先は NImageManager 側が HTTPS で公開されている構成を推奨します。
- Firefox の配布物を更新した場合は、新しい `.xpi` に入れ替えてください。
