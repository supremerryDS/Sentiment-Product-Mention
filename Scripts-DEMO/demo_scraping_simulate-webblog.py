import requests
from bs4 import BeautifulSoup
import json
from collections import defaultdict

def simulate_scrape_comments_from_blog(html):
    """Simulate scraping comments from a web blog HTML structure."""
    soup = BeautifulSoup(html, 'html.parser')
    comments = []

    for div in soup.find_all("div", class_="comment"):
        review_id = div.get("data-id") or "unknown"
        text = div.get_text(strip=True)
        comments.append({
            "review_id": review_id,
            "comment_text": text
        })

    return comments

def format_comments_as_json_structure(comments):
    """Convert comment list into original JSON format."""
    grouped = defaultdict(list)
    for c in comments:
        grouped[c["review_id"]].append(c["comment_text"])
    return {"TextItem": grouped}

def save_to_json(data, file_path="scraped_reviews.json"):
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"✅ Saved to {file_path}")

# Simulated or real HTML input
url = 'http://172.20.10.3:8501'  # <-- change to your actual blog address if needed
response = requests.get(url)
html = response.text

# Process
comments_data = simulate_scrape_comments_from_blog(html)
structured_json = format_comments_as_json_structure(comments_data)
save_to_json(structured_json)

