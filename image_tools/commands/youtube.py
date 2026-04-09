import yt_dlp
import os

from image_tools.paths import PROJECT_ROOT
from image_tools import settings as app_settings
from image_tools.settings import require_setting_str

_S = app_settings.load_settings()

# ＝＝＝ 設定エリア（このファイルを直接編集）＝＝＝
TARGET_URLS = [
    # ここにURLを入力
    "https://www.youtube.com/shorts/VmvV1Xrbl7Q",
]

SAVE_DIR = _S.get("YOUTUBE_SAVE_DIR") or ""
ARCHIVE_FILE = str(PROJECT_ROOT / "youtube_history.txt")
DOWNLOAD_MODE = "video"  # "video" または "audio"
AUDIO_FORMAT = "mp3" 

def get_ydl_opts():
    opts = {
        # 【改善】ファイル名の先頭に「投稿日(YYYYMMDD)」を付けて時系列ソートを完璧にする
        'outtmpl': f'{SAVE_DIR}/%(upload_date)s_%(uploader)s_%(title)s_[%(id)s].%(ext)s',
        'download_archive': ARCHIVE_FILE,
        'ignoreerrors': True,
        'quiet': False,
        
        # 【改善】概要欄のテキストなどをJSONファイルとしても保存（gallery-dlと同じ形式）
        'writeinfojson': True,
    }

    if DOWNLOAD_MODE == "video":
        opts['format'] = 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best'
        opts['merge_output_format'] = 'mp4'
        
        # 【改善】動画向けの強力なアーカイブ設定
        opts['writesubtitles'] = True      # 手動字幕をダウンロード
        opts['writeautomaticsub'] = True   # 自動生成字幕もダウンロード
        opts['subtitleslangs'] = ['ja', 'en'] # 日本語と英語を指定
        opts['writethumbnail'] = True      # サムネイルをダウンロード
        
        opts['postprocessors'] = [
            # 1. 字幕を動画ファイルに埋め込む
            {'key': 'FFmpegSubtitlesConvertor', 'format': 'srt'},
            {'key': 'FFmpegEmbedSubtitle'},
            # 2. メタデータ（タイトル、概要欄、チャプター）を動画に書き込む
            {'key': 'FFmpegMetadata', 'add_chapters': True, 'add_metadata': True},
            # 3. サムネイルを動画のカバーアートとして埋め込む
            {'key': 'EmbedThumbnail'},
        ]
        
    elif DOWNLOAD_MODE == "audio":
        opts['format'] = 'bestaudio/best'
        
        # 【改善】音声向けのアーカイブ設定（メタデータとサムネイルの埋め込み）
        opts['writethumbnail'] = True
        opts['postprocessors'] = [
            {'key': 'FFmpegExtractAudio', 'preferredcodec': AUDIO_FORMAT, 'preferredquality': '192'},
            {'key': 'FFmpegMetadata', 'add_metadata': True},
            {'key': 'EmbedThumbnail'},
        ]
        
    return opts

def run_youtube_downloader():
    require_setting_str("YOUTUBE_SAVE_DIR")
    os.makedirs(SAVE_DIR, exist_ok=True)
    print(f"🚀 YouTube高耐久アーカイブ処理を開始します（モード: {DOWNLOAD_MODE}）...\n")
    
    opts = get_ydl_opts()
    
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.download(TARGET_URLS)
        print("\n🎉 すべての処理が完了しました！")
    except Exception as e:
        print(f"\n⚠️ エラーが発生しました: {e}")
        print("メタデータの埋め込みには FFmpeg が必須です。インストールされているか確認してください。")

if __name__ == "__main__":
    if not TARGET_URLS or TARGET_URLS[0] == "https://www.youtube.com/watch?v=XXXXXXX":
        print("⚠️ 設定エリアの TARGET_URLS に実際のYouTube URLを入力してください。")
    else:
        run_youtube_downloader()

TARGET_ACCOUNTS = {
    "instagram": [
        "aoiyuki_official",
        "enakorin",
        "nashiko_cos",
        "ogurayuka_official",
        "0o_momomari_o0",
        "rolaofficial",
    ],
    "pixiv":[
        "61218475",         # かのめゆら
        "107415234",        # unight neko
        "111230574",        # Tamachi@AIart
        "117323970",        # めがねの裏側
        "141282146"         # ICBMLABO
    ],
    "twitter": [
        "AIartezu",
        "ainovlove8",
        "AIAVAAI",          # AVA
        "Ayanong_AIart",
        "Az_ai_layoutlab",
        "AzAIprompter",
        "bubu2kUFO",
        "cubesteak2",       # さいころ
        "daredare_ai2",
        "gfree728",
        "Giurasu_AIart",
        "GRAY_AIart",
        "GRAY_AIartsub",
        "growupkomari",
        "Giurasu_AIart",
        "habeli_ai",        # 侍りbot
        "hanenoha_ai",
        "horon_AIart",      # ほるん
        "h4k4s3_aae",
        "Iris_ai_Iris",
        "jsmatsu_44",
        "KariSeisak71570",  # かっこ
        "kanome_c",
        "kanome_yui",
        "KariSeisak71570",
        "KHAIWAI567997",
        "LifelongOrca98",
        "marume_AIart",
        "MCagnJP4Oniji",
        "megamega_aiart",
        "MotuNikomiAI",
        "nanowombat02",         # nanowombat
        "nanowombatSUB",        # nanowombatサブ
        "natsu_ichino",
        "nekoneet2",
        "ousetuaiirasuto",
        "PeR0pU",
        "poke066",
        "Poke0662nd",
        "poke0663rd",
        "reve_a_i",
        "Ringoame8163",         # りんごあめ
        "rockey2799m",          # Rockey
        "rit_ai_",
        "Tatara_AI",
        "timatanotamati",
        "tsukishiroalice",
        "tsumechanai",
        "Tuxedo_Ham31",
        "uramega_aiart",        # うらめがねの裏側
        "uramega_3rd",          # うらめがね3rd
        "user_kmer7223",        # RADIANT
        "XX_XX7658",
        "yo8680307672284",
        "zhongjoji00000",
        "zlippers_AIer",
        "___bcd___",
        "___dcb___",
    ]
    
}