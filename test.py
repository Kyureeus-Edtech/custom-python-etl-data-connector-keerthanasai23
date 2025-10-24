from pymongo import MongoClient
import certifi

MONGO_URI = "mongodb+srv://agentic123ai_db_user:16RV005934tFndPK@cluster0.vm4s7ik.mongodb.net/?appName=Cluster0"

client = MongoClient(
    MONGO_URI,
    tls=True,
    tlsCAFile=certifi.where(),
    serverSelectionTimeoutMS=10000
)

try:
    print(client.server_info())  # fetch cluster info
    print("✅ Connection successful!")
except Exception as e:
    print("❌ Connection failed:", e)
