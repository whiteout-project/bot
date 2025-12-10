# Whiteout Survival Discord Bot

Whiteout Survival Discord Bot that supports alliance management, event reminders and attendance tracking, gift code redemption, minister appointment planning and more. This bot is free, open source and self-hosted.

**This is the actively maintained and improved version of the original bot that was created and soon abandoned by Reloisback.**

## 🖥️ System Requirements

The initial release on this repository, `v1.0.0`, is the last version that needs to be patched manually. If you already run this version or later, you can update via the autoupdate system (just restart the bot and answer the prompt if needed).

Starting with `v1.3.0`, the bot uses a custom ONNX model for gift code redemption. This means the bot requirements are low enough to run on most free VPS providers. For a list of known working providers, please see below.

| Prerequisite  | Minimum                                     | Recommended                                   |
|---------------|---------------------------------------------|-----------------------------------------------|
| CPU           | 64-bit AMD/ARM Processor with SSE4.1 Support (2008+) | 64-bit AMD/ARM Processor with AVX/AVX2 Support (2013+)|
| Memory        | 200 MB Free RAM                             | 1 GB for smoother operation                   |
| Disk Space    | 400-500MB (including all required packages)             | 500MB+ on SSD for faster OCR performance                |
| GPU           | None                                        | None                                          |
| Python        | 3.9 (64-bit version required)               | 3.12+                                |

 - If you run your bot non-interactively, for example as a systemd service on Linux, you should run `--autoupdate` to prevent the bot from using the interactive update prompt.

- ⚠️ If you run your bot on Windows, there is a known issue with onnxruntime + an outdated Visual C++ library. To overcome this, install [the latest version of Visual C++](https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist?view=msvc-170) in both 32-bit and 64-bit variants, and then run `main.py` again.

## ☁️ Hosting Providers

**⚠️ This is a self-hosted bot!** That means you will need to run the bot somewhere. You could do this on your own PC if you like, but then it will stop running if you shut the PC down. Luckily there are some other hosting options available which you can find on [our Discord server](https://discord.gg/apYByj6K2m) under the `#host-setup` channel, many of which are free.

We have a list of known working VPS providers below that you could also check out. **Please note that the bot developers are not affiliated with any of these hosting providers (except ikketim) and we do not provide support if your hosting provider has issues.**

| Provider       | URL                                | Notes                                 |
|----------------|------------------------------------|---------------------------------------|
| ikketim        | https://panel.ikketim.nl/          | Free, recommended, and supported by one of our community MVPs. Contact ikketim on Discord to get set up with the hosting. |
| SillyDev       | https://sillydev.co.uk/            | Free tier. Earn credits through ads to maintain. |
| Bot-Hosting    | https://bot-hosting.net/           | Free tier. Requires earning coins though CAPTCHA / ads to maintain. |
| Lunes          | https://lunes.host/                | Free tier with barely enough capacity to run the latest version of the bot. Least recommended host out of the list here. |

If you are aware of any additional free providers that can host the bot, please do let us know.

## 📲 Discord App Setup

**Before following the steps below to install the bot, you should have completed the bot setup on the Discord Application Portal.**

If not, follow these steps first.

1. Go to the [Discord Application Portal](https://discord.com/developers/applications)

2. Click **New Application**, name it, and click **Create**.
Add an App Icon and Description if you like.
This determines how your Bot will appear on your Discord server.

3. On the left, go to **Settings > OAuth2**, and under **OAuth2 URL Generator**, select:
* ✅ bot

4. A **Bot Permissions** window will open below, select:
* ✅ Administrator

    Next to the Generated URL at the bottom of the page, click **Copy** and then paste the URL into your web browser.

5. Select your Discord server and follow the steps to add the bot to the server. Make sure to give the bot Administrator permissions.

6. Go back to the **Discord Application Portal** and make sure your bot is selected.

7. Click on **Bot** on the left settings menu.

8. On the page that opens, under **Privileged Gateway Intents**, enable:

* ✅ Server Members Intent
* ✅ Message Content Intent
* ✅ Presence Intent

9. Click **Reset Token**, confirm, and copy the bot token.

10. Save this token in a text file named `bot_token.txt`. **Keep it safe!** You will also need it later on in the installation instructions.

## 🚀 Installation Steps

### Installing the bot for the first time?

1.  **⬇️ Download the Installer:**
    *   Download the [install.py file](https://github.com/whiteout-project/install/blob/main/install.py)
    *   Place it in a new directory where you want to run the bot

    *Alternatively, if you run on Windows, you could download and run ikketim's [batch script](https://github.com/whiteout-project/install/blob/main/windowsAutoRun.bat) instead.
    Just double-click that and follow the prompts to get set up.*

2.  **▶️ Start the Installer:**
    *   Open a terminal or command prompt **in the new directory you created where install.py is located**.
    *   Run `python install.py` to install the bot. This should automatically pull main.py and all other files into the directory.

3.  **🤖 Start the Bot:**
    *   In your terminal or command prompt **in the same directory you created**, run `python main.py` to start the bot.
    *   When prompted for a Discord bot token, enter your bot token. The bot should now initialize and connect to Discord.

---

### Upgrading or migrating from an older version of the bot?

#### Upgrade Existing Installation
Running `v1.0.0` or higher (from this repository) already:

- If you **already have a working instance**: just restart the bot. It will either update automatically or prompt you, depending on your `--autoupdate` setting.
- If your **instance was previously stuck or broken**: download the latest [main.py](https://github.com/whiteout-project/bot/blob/main/main.py) and overwrite your existing one, then run it. It will handle the upgrade and requirements installation for you.

#### Migrate Existing Installation
* If you simply want to migrate the bot, for example to a new host, all you need is your `bot_token.txt` file and the contents of your `db` folder.
* Follow the steps above to install the bot, then place your bot token into the same directory as main.py and your database files into a new `db` folder on the new host before starting the bot.

#### Upgrading Legacy Installations
Upgrading from the old Relo or Patch Versions such as "V4" or "V1.0.5", which came before our `v1.0.0`:

1.  **🛑 Stop the Bot:** Ensure your Discord bot's `main.py` script is not currently running.

2.  **🗑️ Uninstall old OCR Packages:**
    *   Run this command in your terminal: `pip uninstall -y easyocr torch torchvision torchaudio opencv-python`
    *   If the packages are not found installed, don't worry and proceed to the next step.

3.  **⬇️ Download New Main.py File:**
    *   Download the updated `main.py` file from this repository.
    *   You can find the link here: [Download the patched main.py](https://github.com/whiteout-project/bot/blob/main/main.py)

4.  **🔄 Replace/Add Files:**
    *   Go to your bot's main directory.
    *   Replace the existing `main.py` with the downloaded `main.py`.

5.  **▶️ Restart the Bot:**
    *   Open a terminal or command prompt **in your bot's main directory**.
    *   Run the bot's startup command as you normally would (e.g., `python main.py`).

6.  **🔄 Update the Bot:**
    *   An update prompt to the current version on this repository will show up when starting the bot.
    *   Enter `y` when prompted to update in order to get the new patch.
    *   Observe the console output. This step might take a few minutes, depending on your internet connection.
    *   If the automatic installation completed successfully, the bot should restart on the new version.
    *   If you are running the bot on Windows, you may need to manually restart it with the provided command.

> **If you have any issues with the upgrade**, you can open an issue on Github, or [join our Discord](https://discord.gg/apYByj6K2m) for assistance.

## 🧹 Post-Installation

1.  **🔧 Run `/settings` in Discord:**
    *   Run `/settings` for the bot in Discord for the first time to configure yourself as the global admin.
    *   Run `/settings` again afterwards to access the bot menu and configure it.

2.  **🏰 Set up your Alliance(s):**
    * Once you access the bot menu, you'll want to create one or more Alliance(s) via `Alliance Operations` -> `Add Alliance`.
    * `Control Interval` determines how often the bot will update names and furnace level changes. Once or twice a day should be sufficient.

3.  **👥 Add Members to your Alliance(s):**
    * Add members manually to the alliance(s) you created via `Member Operations` -> `Add Member`.
    * You can set up a channel where members can add themselves via `Other Features` -> `ID Channel` -> `Create Channel`.
    * Members must be added using their in-game ID, found on their in-game profile.
    * There are several ways to get members added to the bot:
      * Subscribe to the [WOSLand website](https://www.wosland.com/) and export the ID List of the alliance (easiest method).
      * Manually collect the IDs from in-game via your members' profiles.
      * Ask all members to post their IDs in your configured ID Channel.

4.  **🤖 Use the Bot as you like...**
  
    With your alliance(s) populated, you can make use of other features. Some examples follow...
    * Assign alliance-specific admins via `Bot Operations` -> `Add Admin`.
    * Configure the `Gift Code Operations` -> `Gift Code Settings` -> `Automatic Redemption` for your alliance(s) to redeem gift codes for all members as soon as they are added/obtained.
    * Set up alerts for your in-game events using `Other Features` -> `Notification System` -> `Set Time`.
    * Keep track of event attendance using the `Other Features` -> `Attendance System` functionality.
    * Organize SvS prep week minister positions using `Other Features` -> `Minister Scheduling`.
    
> If you encounter issues with the bot, you can open an issue on Github or [join our Discord](https://discord.gg/apYByj6K2m) for assistance. We are always happy to help!

## 🚩 Optional Flags
Numerous flags are available that can be used to adjust how the bot runs. These must always added at the end of the startup command, separated by a space, eg. `python main.py --autoupdate`.

| Flag | Purpose |
|-------------|---------------------------------|
| `--autoupdate` | Automatically updates the bot on reboot if an update is found. Useful for headless installs. Used automatically if a container environment is detected.
| `--beta` | Pulls the latest code from the repository on startup (instead of checking for new releases). **This runs unstable code:** Use at your own risk!
| `--no-venv` | Skips the requirement to use a virtual environment. **Dependency conflicts may arise** - you have been warned!
| `--no-update` | Skips the bot's update check, even in container/CI environment. Mutually exclusive with `--autoupdate` and overrides it.
| `--debug` | Additional output for debugging purposes, particularly when requirements installation fails.
| `--verbose` | Same as `--debug` above.
| `--repair` | Attempt to repair the installation by forcing an update to fix any missing files. Works with `--beta` as well.

##  🛠️ Version v1.3.0 (Current)

### 📋 TL;DR Summary
- 🖼️ Gift Operations uses lightweight ONNX-based OCR Model
- 👥 Minister Scheduling system for SvS prep
- 📊 Attendance tracking for all events
- 🔐 Centralized Login Handler for API operations
- ⚡ Alliance and Control systems completely overhauled for speed
- 🔄 Bot now updates directly from release source (no more patch.zip)
- ♻️ Repair option to fix any issues with missing files added
- ⚠️ New beta option to pull the latest repository code directly

### 🔄 Update System Overhaul
- **Added a few new options:**
  - `--beta` flag to pull directly from repository.
  ⚠️ **This runs unstable code:** Use at your own risk!
  - Added `--no-venv` flag for environments that require it.
  ⚠️ **Dependency conflicts may arise** - you have been warned!
  - `--repair` which will attempt to repair by forcing an update to fix any missing files.
- Updates now pull directly from release source instead of separate patch.zip files
- During updates, modified cogs are backed up to `cogs.bak` folder
- Smart update system compares files via SHA hashing - only replaces changed files
- Automatically removes obsolete dependencies (such as ddddocr, opencv-python-headless)
- If requirements or cogs are missing, the bot will load but advise to run `--repair` option

### 🤝 Alliance Improvements

#### Performance & Reliability
- Member add operations are now much faster: **1 member per second** without interruption
- Properly respects API rate limits to prevent delays
- Centralized queue system prevents operation conflicts via new login_handler.py
- Better error handling and user feedback

#### Enhanced Member Management
- Accept FIDs in multiple formats: comma-separated OR newline-separated lists
- Smart validation checks if members already exist before API calls
- Improved progress tracking with cleaner embed updates

### 🎛️ Control System Overhaul

#### Speed Improvements
- Alliance control operations are now much faster: **1 member per second** without interruption
- Removed unnecessary 1-minute delays between manual all alliance checks
- Properly respects API rate limits to prevent delays

#### Logging & Maintenance
- New dedicated log file: `log/alliance_control.txt`
- Automatic log rotation (1MB max size with 1 backup)
- Console output significantly reduced - no more spam!
- Auto-removes invalid FIDs (error 40004) from database
- Tracks all removed FIDs for audit purposes

### 🎁 Gift Operations Upgrade

#### New OCR Engine
- Switched to ONNX Model (thanks bahraini!)
- Similar accuracy to ddddocr with much lower resource usage
- Full Python 3.13+ support
- May even work on Alpine Linux!

#### Interface Improvements
- Reorganized menu with new `Settings` button containing:
  - Channel Management
  - Automatic Redemption
  - Channel History Scan
  - CAPTCHA Settings
- Instant validation for all new gift codes
- Periodic code validation every 2 hours
- Smart priority system for validation FIDs
- Immediate processing of new messages in gift code channels
- On-demand gift code channel history scan (up to 75 messages)
- Extended menu timeouts to 2 hours
- Optimized database transactions
- Clear Redemption Cache option added under CAPTCHA Settings
- Error breakdown added if any errors occurred during redemption
- For more details on errors, please check `log/gift_ops.txt`

### 🔐 Login Handler (New Cog)

#### Centralized API Management
- Controls all Gift API login operations
- Maintains **1 login per second** rate without delays
- Intelligent dual-API support with automatic fallback
- Queue system prevents operation overlap
- Currently used by alliance cogs - more integrations coming!

### 📊 Attendance System (New Cog)
*Enhanced version of Leo's custom cog*

#### Event Tracking Features
- Track attendance for any in-game events (Bear, Foundry, SvS, etc.)
- Automatic history tracking to identify repeat no-shows
- Create and edit attendance reports for any alliance

#### Reporting Options
- **Visual reports:** Matplotlib-based structured reports
- **Text reports:** Clean formatted text
- **Export formats:** CSV, TSV, and HTML

#### Buttons
- `Mark Attendance` - Create or edit attendance reports
- `View Attendance` - Review and export existing reports
- `Settings` - Switch between matplotlib and text reports

### 👥 Minister Scheduling (New Cog)
*Enhanced version of Destrimna's custom cog*

#### SvS Prep Management
- Easy scheduling for Construction, Research, and Training days
- Dual interface: slash commands OR interactive buttons
- Settings menu for global admins with options to clear data/channels

#### Channel Integration
- Dedicated channels for each prep day
- Auto-updating slot availability display
- Comprehensive logging of all minister activities

#### Slash Commands
- `/minister_add` - Book a minister slot
- `/minister_remove` - Cancel a booking
- `/minister_list` - View all appointments
- `/minister_clear_all` - Reset all bookings

## 🛠️ Previous Patch Notes 

### Version v1.2.0
- Implemented a **self-hosted GitLab repo** as a backup in case GitHub fails us again.  
- The bot now checks GitHub first, then falls back to GitLab if needed.
- The bot **automatically creates and manages Python virtual environments**.  
- Prevents dependency conflicts and ensures smooth setup.  
- Fully supports both Windows and Unix-based systems.  
- Automatically skips venv creation inside Docker/Kubernetes/CI environments.
- Better troubleshooting help, including the direct Visual C++ Redistributable x64 link.
- Startup now **auto-reinstalls dependencies** if broken or missing on startup.
- It also **auto-installs missing cogs** from source if they are not found on startup.
- Should reduce install/update issues across the board.
- Web editor? History, at least for now.
- Thanks to @Destrimna, you can now manage all notifications directly via Discord buttons.
- Old notifications (Embed or Message) remain fully editable.
- Added **“Repeat on specific days”** for recurring event scheduling.
- Replaced the crusty old `wosland.com` PHP API with a modern, self-hosted Python version. (it's almost the same, but it smells new)
- Gift codes now **auto-sync across all bot instances** again. Enjoy the easy and efficient shared redeeming!
- Includes solid validation logic to block broken or invalid codes before they break the system.
- Redemptions now follow **alliance order** instead of chaotic parallel execution.  
- Fixed the dreaded “Sign Error” caused by sneaky right-to-left (RTL) marker characters
- Removed dependency on external websites for backup creation.
- Two backup options now available. Sent to you directly via Discord DM or saved manually to the server
- Backups are 100% under your control. Use it wisely. Or don't.
- Removed all remaining **Relo branding** from Support and Startup.
- Startup now proudly shows off **OCR status** like it's something to brag about.
- Removed outdated “Buy me a coffee” link. No coffee for scammers. ☕🚫
- **"Check for Updates"** button works again and compares your version file with the latest release tag on our Github.
- Tons of minor fixes and improvements.
- For the full list of ~~bugs~~ features, visit [GitHub Issues](https://github.com/whiteout-project/bot/issues)

---

### Version v1.1.0

- 💾 **More robust file handling & backups during updates**  
  Now gracefully sidesteps Windows' favorite pastime: locking files for no reason. May your `main.py` live a long, crash-free life.
- ✅ **`ddddocr` installation verification added**  
  Checks that `ddddocr` and its clingy dependencies are *actually* installed. No more “I installed it, I swear” gaslighting.
- 🎯 **Selective `--ignore-requires-python` usage**  
  Only applies the Python rule-bending to `ddddocr`, instead of every package. Because not every package needs special treatment.
- 🐛 **Verbose flag added for package installs**  
  Need to know exactly how the package installation broke? There’s a --verbose flag for that now.
- 🧾 **Added `colorama` and `requests` to requirements**  
  Two more packages join the cult. Because everything is better when you add some color to it.
- 🚫 **Gift code validation delayed to redemption time**  
  Codes added via "Create Gift Code" or the Gift Code Channel are no longer validated immediately. Instead:
  - They are stored in DB with status `pending`
  - On first use (or during periodic validation):
    - If valid: ✅ marked as `valid`
    - If invalid: ❌ redemption stops, status updated to `invalid`
  - Only validated codes hit the giftcode API (once implemented), reducing unnecessary API traffic.
- 🔄 **New “Change Test FID” button for admins**  
  Admins can now swap out the test ID used for validating and testing codes. Default: Relo’s ID. Change it. Or don’t. I’m not your dad.
- 🔙 **Removed Yolo’s favorite back button from CAPTCHA Settings**  
  The back button on CAPTCHA Settings is gone. Yes, really. It’s gone. Are you happy now, @Destrimna?
- 🐞 **Miscellaneous bug fixes**  
  Squashed several minor but persistent gremlins.
- 📣 **Bear Trap notifications now persist even if the channel disappears**  
  Previously, if the channel went poof (due to temporary Discord rate limits, for example), all notifications got disabled. That’s been fixed.

---

### Version v1.0.0

- 🔁 Replaced EasyOCR with ddddocr — Faster, lighter, smarter. Like trading a fax machine for a laser cannon.
- 🛠️ Force-installs ddddocr v1.5.6 with --ignore-requires-python — Because Python 3.13 broke it, but we broke it back.
- 🧠 Optimized gift code redemption loops — Now redeems faster while expertly dodging the rate-limit police.
- 🔥 Removed dusty old GPU config junk — No one needed it, especially not our new friend ddddocr. It’s in a nice farm upstate with the other unused settings.
- 🛡️ Bundled certifi in main.py — Fixes those annoying SSL issues on AWS and friends. Big thanks to @destrimna for reporting, rather than rage-quitting.
- 🧩 Fixed "All Alliances" feature — It works now. Because @destrimna sent in the fix. MVP.
- 📉 Trimmed log file and legacy file bloat — Your hard drive can breathe a bit better.
- 📊 Improved OCR Settings statistics page — More stats. More clarity. Slightly less shame.
- ♻️ Fixed duplicate install checks on startup & updated main.py to work with our new repository and update method. We pray that it works.
- ⬇️ Reset the version numbering to start from 1.0.0 for a clean slate. And better vibes. Mostly for the vibes.
