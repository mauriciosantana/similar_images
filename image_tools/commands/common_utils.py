def read_text_file(filepath):
    """テキストファイルをUTF-8-SIGまたはCP932で読み込む"""
    try:
        with open(filepath, "r", encoding="utf-8-sig") as f:
            return f.readlines()
    except UnicodeDecodeError:
        with open(filepath, "r", encoding="cp932") as f:
            return f.readlines()
    except FileNotFoundError:
        return []