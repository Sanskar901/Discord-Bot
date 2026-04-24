# 🏆 TournamentMaster Pro (VirelApp Edition)

![Esports Management](https://img.shields.io/badge/Esports-Management-gold?style=for-the-badge&logo=riotgames)
![Python](https://img.shields.io/badge/Python-3.9+-blue?style=for-the-badge&logo=python)
![Discord.py](https://img.shields.io/badge/Discord.py-2.0+-7289DA?style=for-the-badge&logo=discord)
![Database](https://img.shields.io/badge/Database-SQLite_WAL-orange?style=for-the-badge&logo=sqlite)

**TournamentMaster Pro** is an industrial-grade automation engine designed for large-scale esports tournaments. Hardened through 5 security audits, it provides the most stable and secure registration experience for Discord-based competitive gaming.

---

## 🌟 Why TournamentMaster Pro?

In high-stakes tournaments, every second counts. This bot is engineered to eliminate human error, prevent registration exploits, and provide real-time transparency via Google Sheets.

### 🛡️ Hardened Security
*   **Session Tokenization:** Every registration attempt is uniquely signed to prevent "Ghost Deletions."
*   **Concurrency Locks:** Prevents race conditions during heavy traffic spikes.
*   **Anti-Exploit Player Check:** Real-time cross-referencing to block duplicate player entries across teams.

### ⚡ Performance at Scale
*   **SQLite WAL Mode:** Ultra-fast database operations even with 10,000+ entries.
*   **Chunked Mirroring:** Prevents Google Sheets API timeouts by uploading data in optimized batches.
*   **Non-Blocking Logic:** Background task processing ensures the bot never "freezes" during mass operations.

---

## 🔄 The Workflow

### Phase 1: Registration
Teams fill out a professional Discord Modal with their details. Profiles are saved instantly to SQLite.

### Phase 2: Verification
The bot grants a **Tagger Role** and opens a 2-minute window. Players must be tagged and verified against the database.

### Phase 3: Assignment
Verified teams are automatically moved into **Lobby Roles** and **Lobby Channels**. If full, they are placed in a high-precision **Waitlist**.

### Phase 4: Match Day
Admins distribute ID/Pass through the **Admin Dashboard** with a single click, notifying hundreds of players instantly.

---

## 🛠️ Configuration

Create a `.env` file in the root directory:

```env
# Core Bot Config
BOT_TOKEN=your_token_here
MAIN_SPREADSHEET_ID=your_google_sheet_id

# Role & Channel IDs
BOT_ADMIN_ROLE_ID=...
MOD_ROLE_ID=...
BAN_ROLE_ID=...
CONFIRM_CHANNEL_ID=...
ASSIGNMENT_CHANNEL_ID=...
SERVER_LOG_CHANNEL_ID=...
AUDIT_LOG_CHANNEL_ID=...
```

---

## 📋 Staff Command Center

| Command | Category | Usage |
| :--- | :--- | :--- |
| `!panel` | Setup | Post the Team Registration launcher. |
| `!admin` | Dashboard | Open the visual Control Panel for Lobbies. |
| `!clear @user` | Mod | Reset a user's registration and locks. |
| `!ban_team` | Mod | Permanently blacklist a team owner. |
| `!lobby_status` | Audit | View live occupancy of all scrims. |
| `!purge_waitlist`| System | Clear the current queue for a fresh start. |

---

## 🧪 Technical Specifications

- **Backend:** Python 3.13 with `discord.py`
- **Database:** SQLite 3 (WAL Mode Enabled)
- **API:** Google Sheets API v4 (via `gspread`)
- **Concurrency:** `asyncio` + `ThreadPoolExecutor`
- **Audit Logs:** Full tracking of staff actions and database mutations.

---

## 🤝 Support & Maintenance

Developed for **VirelApp**. For technical support or feature requests, contact the development team.

---
*Built for the next generation of esports infrastructure.*
