# NImage Manager

## Installing Python

This application uses Python 3.12 or 3.13.  
For a new setup, **Python 3.13.12** is recommended.

Check whether Python 3.13 is already installed.

- Windows: open Command Prompt and run `py -3.13 --version`
- macOS / Linux: open Terminal and run `python3.13 --version`

If it is not installed, install **Python 3.13.12** first.

- Windows: download and install it from the official Python 3.13.12 release page. Do not use the Python install manager. Download the Windows installer (64-bit) near the bottom of the page.  
  https://www.python.org/downloads/release/python-31312/
- macOS (Homebrew): `brew install python@3.13`
- Ubuntu / Debian: build and install Python 3.13.12 from the official source package

```bash
sudo apt update
sudo apt install -y build-essential wget libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev libncursesw5-dev libffi-dev libgdbm-dev liblzma-dev tk-dev uuid-dev
cd /tmp
wget https://www.python.org/ftp/python/3.13.12/Python-3.13.12.tgz
tar -xzf Python-3.13.12.tgz
cd Python-3.13.12
./configure --enable-optimizations
make -j"$(nproc)"
sudo make altinstall
```

Verify the result after installation.

- Windows: `py -3.13 --version`
- macOS / Linux: `python3.13 --version`

If you see `Python 3.13.12`, the setup is fine.  
On Windows, enable `Add Python to PATH` during installation.  
At startup, this application prefers **Python 3.13**, and falls back to **3.12** if 3.13 is not available. It then creates the virtual environment.  
If neither Python 3.12 nor 3.13 is installed, startup stops with an error.

## Starting the application
- Windows: `run.bat`
- macOS / Linux: `sh run.sh`

Before doing anything else, start the application once.  
The first startup may take some time because Python packages are installed and initial files are created.

After startup, the console shows URLs such as:
- Local: `http://localhost:32287`
- Tunnel: `https://...trycloudflare.com` when enabled

Cloudflare Quick Tunnel is intended only for test and development use. Do not rely on it as a permanent public endpoint. For regular use, move to a fixed Cloudflare setup, transfer to your own domain, or keep the app private.

First open the `Local` URL from the same PC that started the app.  
To access it from another device on the same network, find the local IP address of the PC running the app and open `http://<that-ip>:32287`.  
Example: `http://192.168.1.25:32287`

### How to find the local IP address of the PC that started the app

#### Windows
1. Press `Win + R`, type `cmd`, and press Enter.
2. Run `ipconfig`.
3. Check the `IPv4 Address` of the adapter currently in use.
4. If it is `192.168.1.25`, open `http://192.168.1.25:32287`.

#### macOS
1. Open the current network details from System Settings → Wi‑Fi or Network.
2. Check the current IP address there.

#### Linux
1. Open Terminal.
2. Run either `hostname -I` or `ip addr`.
3. Look for the LAN address such as `192.168.x.x` or `10.x.x.x`.
4. If it is `192.168.1.25`, open `http://192.168.1.25:32287`.

Notes:
- `127.0.0.1` only works on the same device.
- If multiple addresses are shown, use the one for the currently active Wi‑Fi or wired LAN.

### If another device cannot connect
- Confirm the other device is on the same Wi‑Fi or local network.
- Confirm the URL is `http://<server-ip>:32287`.
- Confirm the firewall on the PC running the app is not blocking port `32287`.
- Confirm no startup error is shown in the console.

### About fixing the local IP address
On a home network, the local IP address of the PC may change after reboot or over time.  
If you want to keep using the same address, configure a DHCP reservation on your router.

## Initial login
After startup, create the first account by entering an account name and password.  
This is the master account, and several operations are only available to it. Do not lose that password.

## Use a private base URL when the app is only for local network / local PC use

If the app is only used inside your local network or on the same PC, set the following in `.env` after the first startup creates the file:

```env
NAI_IM_PUBLIC_BASE_URL=http://localhost:32287
```

## Use a fixed public address for phone, remote access, or sharing with others

By default, a Quick Tunnel can expose a public address, but the address changes each time the app starts.  
One workable approach is to combine Cloudflare Tunnels with a free DDNS service and delegate the domain. The exact method depends on your environment.

This document uses DDNS Now as an example, but services such as `MyDNS.JP`, `ClouDNS`, or `DigitalPlat (qzz.io)` can serve the same role.

### 1. Register a domain on DDNS Now
Go to [DDNS Now](ddns.kuku.lu) and create a new entry from the left menu.

- Desired subdomain: your preferred value using letters, numbers, `-`, and similar characters
- Password: at least 6 characters

### 2. Delegate the domain to Cloudflare
- Go to [Cloudflare](https://dash.cloudflare.com/login) and sign in.
- From the dashboard, start onboarding a domain.
- Enter the domain you registered and continue.
- Choose the Free plan.
- Continue through the activation flow.
- Note the two Cloudflare name server addresses shown in the onboarding flow.
- Open the [DDNS Now control page](https://ddns.kuku.lu/control.php).
- Complete phone verification from the authentication button shown under the NS record area.
- Open the control page again.
- Clear A / AAAA / CNAME / subdomain / wildcard values and save.
- Paste the two Cloudflare name servers into the NS record area, one per line, and save.

If the control page reloads normally, the delegation step is complete.

### 3. Make the tunnel persistent on the PC
- Sign in to Cloudflare again.
- Open DNS records for the delegated domain. If stale records exist, delete them first.
- Go to Network → Tunnels and create a new tunnel.
- Enter any tunnel name you want.
- Follow the install and run steps shown by Cloudflare. When the connection status becomes healthy, continue.
- Open the tunnel you created.
- In the Routes section, add a route.
- Choose `Published application`.
- Confirm the correct domain is selected and set `Service URL` to `http://127.0.0.1:32287`.

After that, access the registered address such as `https://xxxx.f5.si` from the browser.  
Also set that address in `.env`:

```env
NAI_IM_PUBLIC_BASE_URL=https://your-domain.example.com
```

That value is used for startup display, account creation links, and password reset links, so set it correctly.

## NovelAI extension
These extensions add a transfer button to the NovelAI image screen.

### Chrome on desktop
Install either A or B.

A. Install it from the Chrome Web Store:  
https://chromewebstore.google.com/detail/nim-transfer/cnmickkcdgahfcjehhhnokkheaifadci?authuser=0&hl=en

B. Load it locally:
1. Open `chrome://extensions`.
2. Enable Developer mode.
3. Click `Load unpacked`.
4. Select `extensions/chrome/`.

### Firefox on desktop
Install either A or B.

A. Install it from Mozilla Add-ons:  
https://addons.mozilla.org/en-US/firefox/addon/nim-transfer/

B. Install the packaged file:
1. Prepare `extensions/nim_firefox_addons.xpi`.
2. Open it from Firefox extension settings or drag and drop it into Firefox.
3. Accept the confirmation dialog.

### Android Firefox
Install either A or B.

A. Install it from Mozilla Add-ons:  
https://addons.mozilla.org/en-US/firefox/addon/nim-transfer/

B. Install the `.xpi` manually:
1. Save `extensions/nim_firefox_addons.xpi` on the device.
2. Open Settings → About Firefox.
3. Tap the Firefox logo 5 times.
4. Go back to Settings and open `Install Extension from File`.
5. Select the saved `.xpi`.
6. Accept the confirmation dialog.

### iOS or browsers other than Android Firefox
In principle, iOS does not support this extension workflow. Safari may support some extension cases, but this project does not document or support that setup.  
If you still want to try, browsers such as Quetta or Orion may support Chrome or Firefox extension packages, but compatibility is not guaranteed.

### Initial extension setup
1. Open the extension settings page after installation.
2. Enter the NImageManager domain.
   - Enter the domain only, such as `https://example.com`. Do not enter the full API path.
3. Log in with the same username and password you use for NImageManager.
4. When both `Settings` and `Login` show success, setup is complete.

## Data storage location
Database, images, and derived files are stored under `server/data/`.  
To reset the stored data, delete the files in that directory.  
Original uploaded files are also stored there.

## Usage

### Upload image files
Use the upload page to register files. It accepts image files, zip files, and folders containing images.

- Drag and drop an image file or zip file
- Use `Select files` to choose one or more image files or a zip file
- Use `Select folder` to choose a folder that contains images

### Auto-import from a watched folder
If you place `png` or `webp` files in the watched folder, they are automatically imported the same way as normal uploads. The recorded author is the master account. Configure this in `.env`.

- `NAI_IM_DROP_IMPORT_ENABLED`: enable or disable watched-folder import. `1` = enabled, `0` = disabled
- `NAI_IM_DROP_IMPORT_DIR`: watched folder path, relative or absolute
- `NAI_IM_DROP_IMPORT_SETTLE_SEC`: wait time in seconds before import starts after a file appears
- `NAI_IM_DROP_IMPORT_MAX_DEPTH`: folder depth to scan. `1` means the folder itself and one level below it

```env
NAI_IM_DROP_IMPORT_ENABLED=1
NAI_IM_DROP_IMPORT_DIR=./input_image
NAI_IM_DROP_IMPORT_SETTLE_SEC=3
NAI_IM_DROP_IMPORT_MAX_DEPTH=1
```

```env
NAI_IM_DROP_IMPORT_DIR=D:/NImageManager/input_image
```

On success, the original file is removed from the watched folder. On failure, it remains there.  
Restart the application after editing `.env`.

### Upload from the extension
Complete the initial setup for both NIM and the extension first.

- Generate an image in NovelAI.
- Click the transfer button next to the save button.
- When the uploaded message appears, the transfer is complete.

### Browse the image list
Use the preview page to search, filter, sort, and switch display modes.

- Set creator, software, start date, and end date, then run search
- Enter text in tag search, add include or exclude conditions from suggestions, then search
- Filter from the left sidebar using calendar, bookmarks, creator, or software
- Use `Clear` to remove sidebar filters
- Change the sort order
- Toggle main-prompt deduplication
- Switch between grid view and list view

### View image details
Open an item from the preview list to inspect prompts, copy tags, download files, add creators, and save bookmarks.

- Open an image from the list
- Download the original file or potion data
- Review artist, quality, character, other, and negative tag sections
- Copy tags using the copy buttons
- Open metadata to inspect stored values
- Add the creator to the left creator list from the creator add button
- Open your bookmark list from the star button and choose where to save the item
- Create a new bookmark list if needed

### Delete images
Use the preview page to delete selected items, the current page, or the full search target.

- Check the images to delete
- Use `Page bulk` to select everything currently shown
- Use `All targets` to select the full current search result
- Press `Delete`

### Bookmark registration
Bookmarks can be added individually or in bulk.

- Press the star on an unregistered image to add it to the default bookmark list
- Press the star on a registered image to open the bookmark dialog and add or remove it from lists
- For multiple images, select the targets, open bulk bookmark, review each list state, switch only the lists you want to change, then save
- Bookmark list creation, rename, and deletion are also available from the same screen and from personal settings

### Register shared creators and shared bookmarks
You can only reference another user’s works or bookmark lists when that user shares them. Add them from the left side of the preview page.

- Add a shared user from the creator add button
- Add a shared bookmark list from the bookmark add button
- Click an added creator to filter by that creator’s shared works
- Click an added shared bookmark to filter by works inside that shared list
- Remove items you no longer need

### Add accounts
Use the account management page to add other users, issue setup URLs, and remove unnecessary users.

- Enter a username and role, then create the account and issue the setup URL
- Send the issued URL to the target user
- Delete unnecessary users from the list

### Reparse and rebuild statistics
Use the maintenance page to update analysis results and statistics for already registered images.

- Run full reparse for all registered images
- Rebuild statistics
- Retry only failed images from the failed list
- Mark items as skip if they should not be reparsed
