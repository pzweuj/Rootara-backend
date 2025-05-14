# 初始化
## 数据库初始化
user=admin
email=admin@rootara.app
python /app/scripts/rootara_initial.py --name "${USERNAME:-$user}" --email "${EMAIL:-$email}" --db /data/rootara.db --force False

# FASTAPI启动
python /app/main.py
