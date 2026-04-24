# 🏆 TournamentMaster Pro: Industrial-Grade Esports Management System

**TournamentMaster Pro** is a high-performance, enterprise-ready Discord bot engineered specifically for massive esports tournaments and daily competitive scrims. It bridges the gap between Discord community management and structured data handling, providing a seamless experience for both players and administrators.

---

## 💎 Project Overview
In the fast-paced world of esports, manual registration and lobby management are prone to errors and delays. **TournamentMaster Pro** automates the entire lifecycle of a tournament—from team registration and player verification to lobby assignment and real-time data mirroring on Google Sheets.

Designed for **VirelApp**, this system is hardened to handle thousands of concurrent interactions without lag or data corruption.

---

## 🚀 Core Functionalities

### 1. Advanced Registration Workflow
*   **Intuitive Modals:** Sleek, multi-step UI forms for gathering team names, contact info, and social handles.
*   **7-Minute Completion Window:** A safety timer ensures that incomplete sessions are purged, keeping the database lean and preventing stale data entries.
*   **Intelligent Profile Recovery:** Existing teams can "Use Old Profile," allowing them to skip data entry and jump straight to player verification.

### 2. Real-Time Player Tagging & Verification
*   **Active Tagger Role:** Users are granted a temporary "Tagger" role and a dedicated channel to mention their roster.
*   **Conflict Detection:** The bot cross-references the entire database in milliseconds to ensure no player is registered in multiple teams simultaneously.
*   **Dynamic Role Cleanup:** Roles are automatically stripped once verification is complete or if the session times out (2-minute window).

### 3. Automated Lobby & Waitlist Management
*   **Sequential Assignment:** Teams are automatically assigned to available lobby roles (`Lobby-1`, `Lobby-2`, etc.) based on real-time capacity.
*   **Auto-Fill Waitlist:** When a slot opens up in a lobby, the bot instantly "snatches" the next team from the waitlist and moves them in, notifying the players immediately.
*   **Slotlist Auto-Refresh:** As teams move in and out, the official roster message in every lobby channel updates instantly with a polished embed.

### 4. Admin Command Center
*   **Visual Dashboard:** A staff-only interface to control tournament dates, create dozens of lobbies in seconds, and manage ID/Pass distribution.
*   **ID/Pass Management:** Secure distribution of match credentials to specific lobby channels with one-click match reminders.
*   **Ghost Lock Healing:** The system proactively monitors server changes. If an admin deletes a role manually, the bot "heals" the database by clearing stuck registrations.

---

## 🛡️ Technical Hardening (Security & Stability)

*   **Concurrency Locks:** Custom per-lobby locking mechanisms prevent race conditions during high-traffic spikes (e.g., multiple teams trying to join the same slot).
*   **Session Tokenization:** Every tagging session uses a unique Unix-based token to prevent "Ghost Deletions" caused by overlapping registration attempts.
*   **Chunked Mirroring:** Data is pushed to Google Sheets in small, optimized chunks to stay within API rate limits and avoid payload errors.
*   **SQLite WAL Mode:** Uses Write-Ahead Logging for the fastest possible read/write operations, ensuring data integrity during heavy database load.

---

## 🛠️ Configuration & Setup

### Environment Variables (.env)
The system is highly configurable via environment variables:
```env
BOT_TOKEN=...
MAIN_SPREADSHEET_ID=...
BOT_ADMIN_ROLE_ID=...
CONFIRM_CHANNEL_ID=...
ASSIGNMENT_CHANNEL_ID=...
SERVER_LOG_CHANNEL_ID=...
```

### Installation Guide
1.  **Environment:** Requires Python 3.9+ and `discord.py`.
2.  **Dependencies:** Run `pip install -r requirements.txt`.
3.  **Database:** Automatically initializes `database.db` on the first run.
4.  **Google Cloud:** Requires a `service_account.json` file for Sheets API access.

---

## 📋 Staff Commands Reference

| Category | Command | Utility |
| :--- | :--- | :--- |
| **System** | `!admin` | Launch the visual Management Dashboard. |
| **Registration**| `!panel` | Post the registration launcher in any channel. |
| **Moderation** | `!clear @user` | Instant registration and lobby lock removal. |
| **Moderation** | `!ban_team` | Blacklist a team from participating globally. |
| **Cleanup** | `!delete_all_lobbies` | Complete purge of tournament roles and channels. |
| **Audit** | `!lobby_status` | View a detailed occupancy report of all lobbies. |

---

## 🤝 Developed & Maintained by
**VirelApp**
*Building the future of esports infrastructure.*
