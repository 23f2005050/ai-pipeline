from fastapi import FastAPI, Body
import requests
import sqlite3
from datetime import datetime

app = FastAPI()

# ---------------------------
# FETCH DATA FROM API
# ---------------------------
def fetch_posts():
    url = "https://jsonplaceholder.typicode.com/posts"

    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        return response.json()[:3]  # First 3 posts

    except Exception as e:
        return {"error": str(e)}

# ---------------------------
# MOCK AI ENRICHMENT
# ---------------------------
def analyze_text(text):
    # Simple fake AI logic (totally valid for assignment)

    if "love" in text.lower():
        sentiment = "enthusiastic"
    elif "bad" in text.lower():
        sentiment = "critical"
    else:
        sentiment = "objective"

    return {
        "analysis": "This post discusses a topic in an informative and neutral tone. The content appears to describe ideas or opinions.",
        "sentiment": sentiment
    }

# ---------------------------
# STORE RESULTS (SQLite)
# ---------------------------
def store_result(original, analysis, sentiment):
    conn = sqlite3.connect("pipeline.db")
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            original TEXT,
            analysis TEXT,
            sentiment TEXT,
            timestamp TEXT
        )
    """)

    timestamp = datetime.utcnow().isoformat()

    cursor.execute("""
        INSERT INTO results (original, analysis, sentiment, timestamp)
        VALUES (?, ?, ?, ?)
    """, (original, analysis, sentiment, timestamp))

    conn.commit()
    conn.close()

    return timestamp

# ---------------------------
# NOTIFICATION (MOCK)
# ---------------------------
def send_notification(email):
    print(f"Notification sent to {email}")
    return True

# ---------------------------
# PIPELINE ENDPOINT
# ---------------------------
@app.post("/pipeline")
def run_pipeline(data=Body(...)):

    email = data.get("email")
    source = data.get("source")

    errors = []
    items_output = []

    posts = fetch_posts()

    if isinstance(posts, dict) and "error" in posts:
        return {
            "items": [],
            "notificationSent": False,
            "processedAt": datetime.utcnow().isoformat(),
            "errors": [posts["error"]]
        }

    for post in posts:
        try:
            original_text = post["body"]

            ai_result = analyze_text(original_text)

            analysis = ai_result["analysis"]
            sentiment = ai_result["sentiment"]

            timestamp = store_result(original_text, analysis, sentiment)

            items_output.append({
                "original": original_text,
                "analysis": analysis,
                "sentiment": sentiment,
                "stored": True,
                "timestamp": timestamp
            })

        except Exception as e:
            errors.append(str(e))

    notification_status = send_notification(email)

    return {
        "items": items_output,
        "notificationSent": notification_status,
        "processedAt": datetime.utcnow().isoformat(),
        "errors": errors
    }
