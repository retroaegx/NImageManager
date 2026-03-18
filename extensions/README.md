# NIM NovelAI Transfer Extensions

NovelAI の画像ページで表示中の画像を、NImageManager に転送して登録するための拡張機能です。

## 同梱物

- `extensions/chrome/`
  - Chrome / Chromium 系ブラウザ向けの unpacked 拡張
- `extensions/chrome.zip`
  - Chrome 配布・提出用のパッケージ素材
- `extensions/firefox/`
  - Firefox 提出・署名用のソース一式
- `extensions/firefox_local/`
  - Firefox のローカル検証向け一式
- `extensions/nim_firefox_addons.xpi`
  - Firefox 用のインストール済みパッケージ
- `extensions/nim_addon.zip`
  - Firefox 提出用 zip
- `extensions/nim_addon_local.zip`
  - Firefox ローカル検証用 zip

## Chrome の入れ方

1. Chrome で `chrome://extensions` を開く
2. 右上の「デベロッパー モード」を ON にする
3. 「パッケージ化されていない拡張機能を読み込む」を押す
4. `extensions/chrome/` フォルダを選ぶ

※ `chrome.zip` はそのまま Chrome にインストールするためのものではありません。

## Firefox の入れ方

### デスクトップ Firefox

署名済みの `extensions/nim_firefox_addons.xpi` を使います。

- `about:addons` を開く
- 右上の歯車メニューから「ファイルからアドオンをインストール...」を選ぶ
- `extensions/nim_firefox_addons.xpi` を指定する

または、`.xpi` を Firefox ウィンドウへドラッグ＆ドロップしても導入できます。

### Android Firefox

署名済みの `extensions/nim_firefox_addons.xpi` を端末へ配置し、Firefox でその `.xpi` を開いてインストールします。

## 初回設定

インストール直後は設定画面を開きます。

設定画面では次を入力します。

- NImageManager のドメインまたはベース URL
- ログインに使うユーザー名
- パスワード

保存後にログインが成功すると、設定画面に成功状態が表示されます。

## 動作

- NovelAI の画像ページで動作します
- 表示中画像の下部アクション列に転送アイコンを追加します
- 転送アイコンを押すと、現在表示中の画像を NImageManager に送信します
- 転送成功時は画面右上に通知を表示します
- ログイン切れ時は再ログインが必要であることを通知します

## 備考

- 画像転送はユーザー操作時のみ実行します
- 設定値とログイン状態は拡張のストレージに保存します
- Firefox を再提出する場合は、`firefox/` 側の内容を元に署名・パッケージ更新してください
