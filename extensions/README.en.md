# NIM NovelAI Transfer Extensions

These extensions transfer the image currently shown on NovelAI to NImageManager.

## Packages

- `extensions/chrome/`
  - unpacked source for Chrome
  - use this folder when loading the extension locally
- `extensions/chrome.zip`
  - packaged zip used for Chrome Web Store submission or distribution
- `extensions/firefox/`
  - source for Firefox
  - suitable for temporary desktop loading during development
- `extensions/firefox_local/`
  - local Firefox verification source
  - normally unnecessary unless you specifically need that layout
- `extensions/nim_firefox_addons.xpi`
  - packaged Firefox add-on
  - use this for Firefox Desktop and Android Firefox

## Install on Chrome

1. Open `chrome://extensions` in Chrome.
2. Enable Developer mode.
3. Click `Load unpacked`.
4. Select `extensions/chrome/`.

## Install on Firefox Desktop

### Temporary load

1. Open `about:debugging#/runtime/this-firefox` in Firefox.
2. Click `Load Temporary Add-on`.
3. Select `extensions/firefox/manifest.json`.

Temporary add-ons disappear after Firefox restarts.

### Normal install

1. Prepare `extensions/nim_firefox_addons.xpi`.
2. Drag and drop the `.xpi` into Firefox, or open it as a file.
3. Accept the confirmation dialog.

## Install on Android Firefox

1. Save `extensions/nim_firefox_addons.xpi` on the device.
2. Open Settings → About Firefox.
3. Tap the Firefox logo 5 times.
4. Return to Settings and open `Install Extension from File`.
5. Select the saved `.xpi`.
6. Accept the confirmation dialog.

A signed `.xpi` is required when installing from a file on Android Firefox.

## Initial setup

1. Open the extension settings page after installation.
2. Enter the NImageManager domain.
   - Enter only the domain, such as `https://example.com`. Do not enter the full API path.
3. Log in with the NImageManager username and password.
4. Setup is complete when `Settings` and `Login` both show success.

## Behavior

- On NovelAI image pages, the extension adds a transfer button near the save button in the bottom action row.
- Pressing the transfer button sends the currently visible image to NImageManager.
- A notification appears at the top right after a successful transfer.
- If the session is expired, the extension asks the user to log in again.

## Notes

- Image transfer runs only after direct user action.
- Using HTTPS on the NImageManager server is recommended.
- If you update the Firefox package, replace the old `.xpi` with the new one.
