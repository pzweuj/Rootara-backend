# 初始化
## 数据库初始化
python /app/scripts/rootara_initial.py --name $USERNAME --email $EMAIL --db /data/rootara.db --force False

# FASTAPI启动
python /app/main.py
