import streamlit as st
import pandas as pd
import json
import re

# Function to load and flatten JSON data
def load_reviews(json_path):
    with open(json_path, 'r', encoding='utf-8') as file:
        raw_data = json.load(file)
    text_items = raw_data[0]['TextItem']  # Adjust if structure changes
    data = []
    for review_id, comments in text_items.items():
        for comment in comments:
            cleaned = re.sub(r'</?[^>]+>', '', comment)
            cleaned = re.sub(r'<s>', '', cleaned)
            data.append({
                "comment_id": review_id,
                "original_text": comment,
                "cleaned_text": cleaned.strip()
            })
    return pd.DataFrame(data)

# Load and prepare data
st.set_page_config(page_title="Product Review Blog", layout="wide")
st.title("📝 Product Review Web Blog")

# Sidebar file input or default file
json_path = r"C:\Users\panisarak\Desktop\BigDataHW\aws\data\raw-data\social_media_posts\2025-05\product_reviews_final.json"
df_reviews = load_reviews(json_path)

# Filters
with st.sidebar:
    st.header("🔍 Filter Reviews")
    search_term = st.text_input("Search keywords (optional)").lower()
    show_cleaned = st.checkbox("Show cleaned text only", value=False)
    limit = st.slider("Number of reviews to show", 1, 100, 20)

# Apply filters
if search_term:
    df_reviews = df_reviews[df_reviews['cleaned_text'].str.lower().str.contains(search_term)]

df_reviews = df_reviews.head(limit)

# Display reviews
st.subheader("💬 Comments")
for idx, row in df_reviews.iterrows():
    with st.container():
        st.markdown(f"**Comment ID**: `{row['comment_id']}`")
        if show_cleaned:
            st.write(row['cleaned_text'])
        else:
            st.write(row['original_text'])
        st.markdown("---")
