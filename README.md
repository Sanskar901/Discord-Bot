# 🏆 Ultimate Discord Scrims & Tournament Manager Bot

![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)
![Discord.py](https://img.shields.io/badge/discord.py-2.0+-red.svg)
![SQLite](https://img.shields.io/badge/SQLite-Local-green.svg)
![Google Sheets](https://img.shields.io/badge/Google_Sheets-API-yellow.svg)

A fully automated, production-ready Discord bot designed to manage seamless esports tournament registrations, dynamic lobby assignments, and real-time Google Sheets synchronization. Built specifically for high-load, fast-paced Scrims server environments.

---

## ✨ Features

- **Interactive UI Registration:** Elegant 3-step Discord Modals (Team Details, Roster, Subs & Socials).
- **Player Verification & Tagging:** Team owners must tag exactly 4 physical server members in a `#confirm-channel`.
- **Anti-Ghosting & Anti-Evasion:** 
  - Prevents banned players from evading by leaving & rejoining (Database level ban-checks).
  - Auto-wipes ghost teams and frees up tags if an owner leaves the server.
- **Automated Lobby Generation (`!setup`):** Instantly generate up to 50 physical Discord categories, text channels, and exclusive roles.
- **Waitlist Auto-Distribution:** Automatically assigns queued teams to empty lobby slots via FCFS (First Come, First Serve) as soon as new lobbies open.
- **Real-Time Google Sheets Sync:** Two-way sync engine that mirrors the local SQLite database to Google Sheets every 60 seconds.
- **Built-in Rate Limiting:** Spam protection on global interaction buttons to prevent API bans and DB locking.

---

## 📋 Prerequisites

Before running the bot, ensure you have the following installed:
- Python 3.10 or higher
- A Discord Bot Token (from [Discord Developer Portal](https://discord.com/developers/applications))
- Google Cloud Platform Service Account Credentials (for Google Sheets API access)

---

## 🚀 Installation & Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/scrims-bot.git
   cd scrims-bot
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Environment Variables**
   Create a `.env` file in the root directory and add your bot token:
   ```env
   BOT_TOKEN=your_discord_bot_token_here
   CREDENTIALS_PATH=credentials.json
   ```

4. **Add Google Credentials**
   Place your Google Service Account JSON file in the root directory and name it `credentials.json`. Ensure your Service Account has "Editor" access to your target Google Spreadsheets.

5. **Configure Discord Settings**
   Open `main.py` and modify the **CONFIGURATION** block at the top (around line 100) to match your server's specific Role IDs, Channel IDs, and Spreadsheet IDs.

---

## 🏃 Running the Bot

Start the bot by running:
```bash
python main.py
```
*Note: A local `database.db` will be automatically generated upon the first run if it doesn't exist.*

---

## 👑 Role-Based Access Control (RBAC)

The bot operates on a strict access hierarchy:
- **Bot Admin:** Can use global destructive/creation commands (`!setup`, `!delete_all_lobbies`, `!reset_all_locks`, `!ban_team`).
- **Moderator:** Day-to-day lobby controls (clearing players, qualifying teams, removing teams, editing schedules).
- **TOLI Role (Base Player):** Required to see or interact with the Registration panel.

---

## 📱 Command Cheatsheet

| Command | Permission | Description |
|---|---|---|
| `!panel` | Admin | Publishes the main Registration Embed + Button. |
| `!setup` | Admin | Triggers the multi-lobby creation modal wizard. |
| `!id_panel` | Admin | Posts the ID/Password control panel in designated channel. |
| `!clear @user` | Mod/Admin | Force-clears a user's registration and removes their lock. |
| `!delete_lobby` | Mod/Admin | Deletes the specific lobby channel/role you use the cmd in. |
| `!delete_all_lobbies`| Admin | ☢️ Deletes ALL tournament lobbies, roles, and categories. |
| `!reset_all_locks` | Admin | ☢️ Wipes all player lobby data (for a new tournament phase). |
| `!teams` | Admin | DMs a `.csv` file backup of the entire database. |
| `!ban_team @user 5h`| Admin/Mod | Bans a user for specified time (s, m, h, d). |
| `!unban_team @user`| Admin/Mod | Manually pardons a user globally. |

---

## 🛡️ Security Notes
- The `database.db` and `.env` files are added to `.gitignore` to prevent sensitive data or local tournament data from being pushed to public repositories. 
- Avoid directly editing the SQLite file while the bot is actively running to prevent write-locks.

---

*Built for high-performance Esports Management.*
