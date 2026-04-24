<div align="center">
  <h1>🏆 Ultimate Esports Tournament & Scrims Manager Bot</h1>
  <p><strong>A high-performance, async-driven Discord bot built for massive, fast-paced esports ecosystems.</strong></p>

  <img src="https://img.shields.io/badge/Python-3.10%2B-blue?style=for-the-badge&logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/Discord.py-2.0%2B-5865F2?style=for-the-badge&logo=discord&logoColor=white" alt="Discord">
  <img src="https://img.shields.io/badge/SQLite-WAL_Mode-003B57?style=for-the-badge&logo=sqlite&logoColor=white" alt="SQLite">
  <img src="https://img.shields.io/badge/Google_Sheets-API-34A853?style=for-the-badge&logo=googlesheets&logoColor=white" alt="Google Sheets">
</div>

<br/>

## 📖 Overview
Managing large-scale Discord tournaments usually requires heavy manual labor, Google Forms, and tedious role assignments. This bot completely **automates the entire lifecycle of a tournament**: from initial registration via sleek UI Modals, to player verification, dynamic lobby generation, and real-time synchronization with Google Sheets.

With advanced **SQLite Write-Ahead Logging (WAL)** and a dedicated **ThreadPoolExecutor**, this bot handles hundreds of concurrent registrations and button clicks without breaking a sweat.

---

## 🔥 Key Features

### 🎮 Seamless UI Registration Flow
- **3-Step Modals:** Players register via Discord Modals instead of typing messy commands. Captures Team Name, Emails, Player UIDs, In-Game Names, Substitutes, and Socials.
- **Smart Duplicate Handling:** Warns users if they try to register twice and offers a seamless 1-click override option.

### 🛡️ Ironclad Security & Anti-Abuse
- **Ban Evasion Prevention:** Connects directly to the database to catch banned users who leave and rejoin the server to bypass Discord Role bans.
- **Ghost Team Wiping:** Listens to `on_member_remove`. If a Team Owner leaves the server, their team is automatically soft-deleted, and their lobby slot is released instantly.
- **Rate-Limiting (Spam Protection):** Implements localized cooldown algorithms on database-heavy interactions to prevent API lockouts and malicious button spamming.

### 📌 Smart Player Tagging & Verification
- **Strict Validation:** Owners must tag exactly **4 unique players**. The bot verifies that every tagged player actually exists inside the server.
- **Tag Locks:** Once a player is tagged, they are "locked" to that team. They cannot be registered by another team, preventing double-booking.
- **Auto-Expiry:** If a user registers but fails to tag within 2 minutes, their session safely expires, cleaning up temporary roles.

### ⚙️ Automated Lobby Generation (`!setup`)
- Generate up to **50 Lobbies** in seconds.
- Automatically creates Private Discord Categories, Voice/Text Channels, and exclusive Lobby Roles.
- Instantly injects a **Lobby Control Panel** into each text channel for Moderators.

### 🔄 Dynamic Waitlist & Auto-Distribution
- When primary lobbies fill up (20/20), excess teams are placed in a **Waitlist**.
- **FCFS Allocation:** As soon as an Admin opens new lobbies or a team drops out, the bot automatically pushes waitlisted teams into the open slots, grants them roles, and notifies them.

### 📊 Real-Time Google Sheets Sync
- Acts as a two-way synchronization engine using the `gspread` API.
- Every 60 seconds, a background task mirrors the local SQLite DB to your Google Sheets.
- **Zero manual data entry.** Everything from Team Rosters to Ban Logs is perfectly formatted in the cloud.

---

## 🛠️ Tech Architecture
- **Language:** Python 3.10+
- **Library:** `discord.py` 2.0+ (using Slash Commands, Views, and Modals)
- **Database Backend:** `sqlite3` operating in **WAL Mode** (Write-Ahead Logging) combined with `concurrent.futures.ThreadPoolExecutor(max_workers=200)` to handle intense traffic spikes during registration bursts.
- **Cloud Integration:** Google Service Accounts & OAuth2.

---

## 🚀 Installation & Setup

### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/scrims-bot.git
cd scrims-bot
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Setup Environment Variables
Create a `.env` file in the root directory:
```env
BOT_TOKEN=your_discord_bot_token_here
CREDENTIALS_PATH=credentials.json
```

### 4. Google Sheets API Authorization
- Go to the [Google Cloud Console](https://console.cloud.google.com/).
- Enable the **Google Sheets API** and **Google Drive API**.
- Create a **Service Account**, download the JSON key, rename it to `credentials.json`, and place it in the project root.
- **Important:** Share your target Google Sheets with the Service Account email address as an "Editor".

### 5. Server Configuration
Open `main.py` and modify the **CONFIGURATION** block (Line ~100) to match your server's Role IDs, Channel IDs, and Google Spreadsheet IDs:
```python
BOT_ADMIN_ROLE_ID       = 123456789012345678
REGISTRATION_CHANNEL_ID = 123456789012345678
# ... update all IDs accordingly
```

### 6. Run the Bot
```bash
python main.py
```
*(The local SQLite database will generate automatically on the first launch.)*

---

## 👑 Role Hierarchy

The bot strictly enforces permissions based on Discord Roles:
- **Bot Admin (`BOT_ADMIN_ROLE_ID`):** Has ultimate power. Can wipe databases, generate lobbies, ban teams, and view the ID/PW panel.
- **Moderator (`MOD_ROLE_ID`):** Can manage day-to-day operations. Kick teams from lobbies, edit match schedules, and publish slotlists.
- **Lobby Viewer (`LOBBY_VIEWER_ROLE_ID`):** Allowed to see lobby channels without being a participant.
- **Base Player (`TOLI_ROLE_ID`):** Required to interact with the registration panel.

---

## 📱 Command Cheatsheet

### Global Admin Commands
| Command | Permission | Description |
|---|---|---|
| `!panel` | Admin | Publishes the main Registration Embed + Button. |
| `!setup` | Admin | Triggers the multi-lobby creation modal wizard. |
| `!id_panel` | Admin | Posts the Match ID/Password distribution panel. |
| `!delete_all_lobbies`| Admin | ☢️ Deletes ALL tournament lobbies, roles, and categories. |
| `!reset_all_locks` | Admin | ☢️ Wipes all player lobby data for a new tournament phase. |
| `!teams` | Admin | Forces an immediate local `.csv` export of the database to DMs. |

### Moderation & Management
| Command | Permission | Description |
|---|---|---|
| `!delete_lobby` | Mod/Admin | Deletes the specific lobby channel/role you use the cmd in. |
| `!clear @user` | Mod/Admin | Force-clears a user's registration and removes their lock. |
| `!ban_team @user 5h`| Mod/Admin | Bans a user/team for a specified timeframe (s, m, h, d). |
| `!unban_team @user`| Mod/Admin | Manually pardons a user and restores access. |

### Control Panel (In-Lobby Buttons)
Inside each generated Lobby, Moderators have access to:
- 📢 **Publish Slotlist:** Generates a formatted roster of all 20 active teams.
- ✅ **Qualified Teams:** Advances a team to the next phase/bracket.
- ❌ **Remove Team:** Boots a team from the lobby and notifies the owner.
- 🎮 **Manage Matches:** Edit Room IDs, Passwords, and Start Times dynamically.

---

## ⚠️ Important Deployment Notes
- **Do NOT** upload `.env`, `credentials.json`, or `database.db` to public GitHub repositories. They contain highly sensitive keys.
- If migrating servers, executing a fresh `!reset_all_locks` is recommended to clear out old Discord object references.

---

<div align="center">
  <i>Developed for seamless and professional Esports Management.</i>
</div>
