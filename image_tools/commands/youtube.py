import yt_dlp
import os
import argparse
from pathlib import Path

from image_tools.paths import PROJECT_ROOT
from image_tools import settings as app_settings
from image_tools.settings import require_setting_str

def get_ydl_opts(save_dir: str, archive_file: str, mode: str = "video", audio_format: str = "mp3"):
    """yt-dlp のオプションを取得する"""
    save_path = Path(save_dir)
    opts = {
        # ファイル名の先頭に「投稿日(YYYYMMDD)」を付けて時系列ソートを容易にする
        'outtmpl': str(save_path / '%(upload_date)s_%(uploader)s_%(title)s_[%(id)s].%(ext)s'),
        'download_archive': archive_file,
        'ignoreerrors': True,
        'quiet': True,
        
        # 【改善】概要欄のテキストなどをJSONファイルとしても保存（gallery-dlと同じ形式）
        'writeinfojson': True,
    }

    if mode == "video":
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
        
    elif mode == "audio":
        opts['format'] = 'bestaudio/best'
        
        # 【改善】音声向けのアーカイブ設定（メタデータとサムネイルの埋め込み）
        opts['writethumbnail'] = True
        opts['postprocessors'] = [
            {'key': 'FFmpegExtractAudio', 'preferredcodec': audio_format, 'preferredquality': '192'},
            {'key': 'FFmpegMetadata', 'add_metadata': True},
            {'key': 'EmbedThumbnail'},
        ]
        
    return opts

def run_youtube_downloader(urls: list, mode: str = "video", audio_format: str = "mp3"):
    """ダウンロードメイン処理"""
    require_setting_str("YOUTUBE_SAVE_DIR")
    settings = app_settings.load_settings()
    save_dir = settings.get("YOUTUBE_SAVE_DIR")
    archive_file = str(PROJECT_ROOT / "youtube_history.txt")

    os.makedirs(save_dir, exist_ok=True)
    print(f"🚀 YouTube高耐久アーカイブ処理を開始します（モード: {mode}）...\n")
    
    opts = get_ydl_opts(save_dir, archive_file, mode, audio_format)
    
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.download(urls)
        print("\n🎉 すべての処理が完了しました！")
    except Exception as e:
        print(f"\n⚠️ エラーが発生しました: {e}")
        print("メタデータの埋め込みには FFmpeg が必須です。インストールされているか確認してください。")

# デフォルトのURL（直接編集用）
TARGET_URLS = [
    "https://www.youtube.com/shorts/_p9scunEydU",
]

TARGET_ACCOUNTS = {
    "instagram": [
        "aoiyuki_official",
        "enakorin",
        "nashiko_cos",
        "ogurayuka_official",
        "0o_momomari_o0",
        "rolaofficial",
    ],
}

def get_urls_from_accounts(platform: str) -> list:
    """アカウント辞書から URL リストを生成する"""
    accounts = TARGET_ACCOUNTS.get(platform, [])
    if platform == "instagram":
        return [f"https://www.instagram.com/{acc}/" for acc in accounts]
    return []

def main():
    parser = argparse.ArgumentParser(description="YouTube/SNS Video Downloader (yt-dlp wrapper)")
    parser.add_argument("urls", nargs="*", help="ダウンロード対象のURL")
    parser.add_argument("--mode", choices=["video", "audio"], default="video", help="ダウンロードモード")
    parser.add_argument("--audio-format", default="mp3", help="音声モード時のフォーマット")
    parser.add_argument("--platform", choices=["instagram"], help="指定したプラットフォームのアカウントリストを一括処理")
    
    args = parser.parse_args()
    
    # 対象URLの決定
    target_urls = args.urls if args.urls else []
    
    # プラットフォーム指定がある場合はアカウントリストからURLを追加
    if args.platform:
        target_urls.extend(get_urls_from_accounts(args.platform))
    
    if not target_urls:
        target_urls = list(TARGET_URLS)

    if not target_urls or (len(target_urls) == 1 and "XXXXXXX" in target_urls[0]):
        print("⚠️ 対象URLが指定されていないか、デフォルト値のままです。")
        print("引数でURLを渡すか、スクリプト内の target_urls を編集してください。")
        return

    run_youtube_downloader(target_urls, mode=args.mode, audio_format=args.audio_format)

if __name__ == "__main__":
    main()