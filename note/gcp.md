
在gcp上設立vm

安裝git,即可使用git clone把已經上傳至github上的專案clone至雲端
安裝指令apt-get install git
雲端clone專案指令打法
git clone git@github.com:nadrew92131-pixel/etl-gcp-project.git

在雲端上跑docker, 建立一個裝滿套件的空間(container), 會去執行dockerfile裡的poetry install
docker build -t my-etl-app .

用建立好的container,執行專案
docker run --rm -v $(pwd):/app my-etl-app

在地端修改程式,只要在push一次,然後就在雲端pull即可
git pull origin main



在雲端利用docker 執行python程式指令,這個方法就得把所有module都抓下來, 所以不用這個方法
docker run --rm \
  -v $(pwd):/app \
  -w /app \
  python:3.14.2 \
  python main.py
