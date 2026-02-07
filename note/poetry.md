
git 建立完後

輸入
poetry init
一路enter到底

用poetry安裝python套件(會根據toml檔裡的python版本安裝套件)
poetry add pandas sqlalchemy pymysql requests


建立Dockerfile
 Dockerfile 是為了上雲端準備的「包裝紙」：

雲端環境的翻譯官： 你的電腦是 Windows，但 GCP 機器是 Linux。Dockerfile 會告訴 GCP：「請開一個 Linux 環境，裝好 Python，並讀取我的 pyproject.toml 把套件裝起來」。

解決「地端會動、雲端不動」： 有時候某些爬蟲套件需要 Linux 特有的系統工具（例如 libpq-dev 或 Chromium）。如果你在地端完全沒準備 Dockerfile，等到推上 GCP 才發現少裝東西，會很難修