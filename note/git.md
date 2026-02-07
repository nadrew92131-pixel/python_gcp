
git clone(terminal)
git clone git@github.com:nadrew92131-pixel/etl-gcp-project


檢查地端有沒有ssh key
type %userprofile%\.ssh\id_ed25519.pub

產生新的ssh key
ssh-keygen -t ed25519 -C "你的Email"


輸入這行指令可以秀出整行ssh key
type %userprofile%\.ssh\id_ed25519.pub

打開 GitHub SSH Settings。
點擊 New SSH key。
Title：寫 My Windows PC。
Key：貼上你剛才複製的那串亂碼，然後點點擊 Add SSH key。


這些完成後,github就可以監管我這個資料夾
