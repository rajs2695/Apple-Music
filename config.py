import re
from os import getenv
# ------------------------------------
# ------------------------------------
from dotenv import load_dotenv
from pyrogram import filters
# ------------------------------------
# ------------------------------------
load_dotenv()
# ------------------------------------
# -----------------------------------------------------
API_ID = int(getenv("API_ID", "10284859"))
API_HASH = getenv("API_HASH", "b0ad58eb8b845ba0003e0d9ce5fc2196")
# ------------------------------------------------------
BOT_TOKEN = getenv("BOT_TOKEN", "2096983652:AAHz41orhz9RrWscwg_WwSCp0_OhP-zLmDY")
# -------------------------------------------------------
OWNER_USERNAME = getenv("OWNER_USERNAME","rajeshrakis")
# --------------------------------------------------------
BOT_USERNAME = getenv("BOT_USERNAME" , "thedakkidaikathaval_bot")
# --------------------------------------------------------
BOT_NAME = getenv("BOT_NAME" , "𝞖𝘌𝘈𝘙𝘛𝞑𝘌𝘈𝘛𝂬♡𝂬𝘿𝘙𝘜𝘎𝘡")
# ---------------------------------------------------------
ASSUSERNAME = getenv("ASSUSERNAME" , "Apple_Ponnu")
# ---------------------------------------------------------


#---------------------------------------------------------------
#---------------------------------------------------------------
MONGO_DB_URI = getenv("MONGO_DB_URI", "mongodb+srv://heartbeat:Beat7Heart@heartbeat.1h1nbxv.mongodb.net/?retryWrites=true&w=majority")
#---------------------------------------------------------------
#---------------------------------------------------------------

# ----------------------------------------------------------------
DURATION_LIMIT_MIN = int(getenv("DURATION_LIMIT", 17000))
# ----------------------------------------------------------------

# ----------------------------------------------------------------
LOGGER_ID = int(getenv("LOGGER_ID", -1001735663878))
# ----------------------------------------------------------------
# ----------------------------------------------------------------
OWNER_ID = int(getenv("OWNER_ID", 1281282633))
# -----------------------------------------------------------------
# -----------------------------------------------------------------

# ----------------------------------------------------------------
# ----------------------------------------------------------------
# ----------------------------------------------------------------
HEROKU_APP_NAME = getenv("HEROKU_APP_NAME")
# ----------------------------------------------------------------
HEROKU_API_KEY = getenv("HEROKU_API_KEY")
# ----------------------------------------------------------------
# ----------------------------------------------------------------
# ----------------------------------------------------------------
UPSTREAM_REPO = getenv(
    "UPSTREAM_REPO",
    "https://github.com/rajs2695/Apple-Music",
)
UPSTREAM_BRANCH = getenv("UPSTREAM_BRANCH", "main")
GIT_TOKEN = getenv(
    "GIT_TOKEN", None
)  # ----------------------------------------------------------------
# -------------------------------------------------------------------
# --------------------------------------------------------------------
# --------------------------------------------------------------------



# ------------------------------------------------------------------------
# -------------------------------------------------------------------------
SUPPORT_CHANNEL = getenv("SUPPORT_CHANNEL", "https://t.me/HeartBeat_Offi")
SUPPORT_CHAT = getenv("SUPPORT_CHAT", "https://t.me/HeartBeat_Fam")
# ------------------------------------------------------------------------------
# -------------------------------------------------------------------------------







# --------------------------------------------------------------------------------
# --------------------------------------------------------------------------------
# --------------------------------------------------------------------------------
# --------------------------------------------------------------------------------
AUTO_LEAVING_ASSISTANT = getenv("AUTO_LEAVING_ASSISTANT", "False")
AUTO_LEAVE_ASSISTANT_TIME = int(getenv("ASSISTANT_LEAVE_TIME", "9000"))
SONG_DOWNLOAD_DURATION = int(getenv("SONG_DOWNLOAD_DURATION", "9999999"))
SONG_DOWNLOAD_DURATION_LIMIT = int(getenv("SONG_DOWNLOAD_DURATION_LIMIT", "9999999"))
# --------------------------------------------------------------------------------
# --------------------------------------------------------------------------------
# --------------------------------------------------------------------------------

# ---------------------------------------------------------------------------------
SPOTIFY_CLIENT_ID = getenv("SPOTIFY_CLIENT_ID", "487064a94b42419987704fe395492701")
SPOTIFY_CLIENT_SECRET = getenv("SPOTIFY_CLIENT_SECRET", "ec6615520a9d47d7865ec1e08b39915b")
# ----------------------------------------------------------------------------------




# -----------------------------------------------------------------------------------
PLAYLIST_FETCH_LIMIT = int(getenv("PLAYLIST_FETCH_LIMIT", 2500))
# ------------------------------------------------------------------------------------

# ------------------------------------------------------------------------------------
TG_AUDIO_FILESIZE_LIMIT = int(getenv("TG_AUDIO_FILESIZE_LIMIT", "5242880000"))
TG_VIDEO_FILESIZE_LIMIT = int(getenv("TG_VIDEO_FILESIZE_LIMIT", "5242880000"))
# --------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------



# ------------------------------------
# ------------------------------------
# ------------------------------------
# ------------------------------------
STRING1 = getenv("STRING_SESSION", "BQGJMTYALFX3dQvrOJlHh_mm12o2A-zJL3mH43MY8QkaXiTXd0uAIhdgA5kAZuqkPiRVDyaLHd6XCQDbllXGSsTu9C-yIIc-bmDDLVQ3ZekqCnchluPP6BHV1vKM6Hn4Pgph9tJ7mmdWumI2e_wpzbwloaEP4OAjIZoti1jF8_BpJPBCAZrQfLCglBpRltjiFaDh-lY8p95TPWFwdhyyDYkHCGNIbf3Rcs7VF1jIyprsExSczs9licjn1VUeWNcLbgS6sYzc7ZwrWFEsprJUjn_Y3rVr3Bj4uyDy55o2qNtn3Mb5xSU2bwY0gn3Kovc8mWOOjYI6d0zSr93ddH-JNjwNxxZxwgAAAAHSHPeVAA")
STRING2 = getenv("STRING_SESSION2", None)
STRING3 = getenv("STRING_SESSION3", None)
STRING4 = getenv("STRING_SESSION4", None)
STRING5 = getenv("STRING_SESSION5", None)
STRING6 = getenv("STRING_SESSION6", None)
STRING7 = getenv("STRING_SESSION7", None)
BANNED_USERS = filters.user()
adminlist = {}
lyrical = {}
votemode = {}
autoclean = []
confirmer = {}

# ------------------------------------
# ------------------------------------
# ------------------------------------
# ------------------------------------

# ------------------------------------------------------------------------
# ------------------------------------------------------------------------
# ------------------------------------------------------------------------
# ------------------------------------------------------------------------
# ------------------------------------------------------------------------
# ------------------------------------------------------------------------
# ------------------------------------------------------------------------
START_IMG_URL = getenv(
    "START_IMG_URL", "https://graph.org/file/f21bcb4b8b9c421409b64.png"
)
PING_IMG_URL = getenv(
    "PING_IMG_URL", "https://graph.org/file/ffdb1be822436121cf5fd.png"
)
PLAYLIST_IMG_URL = "https://graph.org/file/f21bcb4b8b9c421409b64.png"
STATS_IMG_URL = "https://graph.org/file/f21bcb4b8b9c421409b64.png"
TELEGRAM_AUDIO_URL = "https://graph.org/file/f21bcb4b8b9c421409b64.png"
TELEGRAM_VIDEO_URL = "https://graph.org/file/f21bcb4b8b9c421409b64.png"
STREAM_IMG_URL = "https://graph.org/file/f21bcb4b8b9c421409b64.png"
SOUNCLOUD_IMG_URL = "https://graph.org/file/f21bcb4b8b9c421409b64.png"
YOUTUBE_IMG_URL = "https://graph.org/file/f21bcb4b8b9c421409b64.png"
SPOTIFY_ARTIST_IMG_URL = "https://graph.org/file/f21bcb4b8b9c421409b64.png"
SPOTIFY_ALBUM_IMG_URL = "https://graph.org/file/f21bcb4b8b9c421409b64.png"
SPOTIFY_PLAYLIST_IMG_URL = "https://graph.org/file/f21bcb4b8b9c421409b64.png"


# ------------------------------------------------------------------------
# ------------------------------------------------------------------------
# ------------------------------------------------------------------------
# ------------------------------------------------------------------------
def time_to_seconds(time):
    stringt = str(time)
    return sum(int(x) * 60**i for i, x in enumerate(reversed(stringt.split(":"))))


DURATION_LIMIT = int(time_to_seconds(f"{DURATION_LIMIT_MIN}:00"))

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# ------------------------------------------------------------------------------
if SUPPORT_CHANNEL:
    if not re.match("(?:http|https)://", SUPPORT_CHANNEL):
        raise SystemExit(
            "[ERROR] - Your SUPPORT_CHANNEL url is wrong. Please ensure that it starts with https://"
        )

if SUPPORT_CHAT:
    if not re.match("(?:http|https)://", SUPPORT_CHAT):
        raise SystemExit(
            "[ERROR] - Your SUPPORT_CHAT url is wrong. Please ensure that it starts with https://"
        )
# ---------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------
