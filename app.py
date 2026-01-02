import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
import os
from dotenv import load_dotenv

cert_content = os.environ.get("CA_CERT_CONTENT")
if cert_content:
    with open("ca.pem", "w") as f:
        f.write(cert_content)

st.set_page_config(page_title="Book Gem Finder", layout="wide")
st.title("Book Market Intelligence Dashboard")
st.markdown("Finding high-rated books with falling price trends.")

load_dotenv()

database_url = os.environ.get("DATABASE_URL")
engine = create_engine(database_url, connect_args={"ssl_ca": "ca.pem"})

@st.cache_data(ttl=600) 
def load_data():
    query = """
        SELECT b.title, b.rating, p.price, p.scraped_at
        FROM books b
        JOIN price_history p ON b.id = p.book_id
    """
    return pd.read_sql(query, engine)

df = load_data()

st.sidebar.divider()
st.sidebar.subheader("System Status")

if not df.empty:
    latest_update = df['scraped_at'].max()
    readable_time = latest_update.strftime("%b %d, %Y %I:%M %p")
    st.sidebar.success(f"Last Sync: {readable_time}")
    st.sidebar.caption("Data refreshes daily at 3:00 AM UTC")
else:
    st.sidebar.warning("Syncing data...")

st.sidebar.header("Top Price Drops")
def find_gems(data):
    gem_list = []
    for title in data['title'].unique():
        subset = data[data['title'] == title].sort_values('scraped_at')
        if len(subset) >= 2:
            price_diff = subset['price'].iloc[-1] - subset['price'].iloc[0]
            if price_diff < 0:
                gem_list.append({'Title': title, 'Drop': price_diff})
    return pd.DataFrame(gem_list).sort_values('Drop')

gems_df = find_gems(df)
if not gems_df.empty:
    st.sidebar.dataframe(gems_df, hide_index=True)
else:
    st.sidebar.write("No price drops detected yet.")

# --- MAIN SECTION: SEARCH & VISUALIZE ---
target_book = st.selectbox("Select a book to view history:", df['title'].unique())

if target_book:
    book_data = df[df['title'] == target_book].sort_values('scraped_at')
    
    col1, col2 = st.columns(2)
    col1.metric("Current Price", f"£{book_data['price'].iloc[-1]}")
    col1.write(f"Rating: {book_data['rating'].iloc[-1]}")
    
    fig = px.line(book_data, x='scraped_at', y='price', title=f"Price Trend: {target_book}", markers=True)
    st.plotly_chart(fig, use_container_width=True)

with st.expander("View Full Database"):
    st.dataframe(df, use_container_width=True)
    