import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import mysql.connector
from datetime import datetime
import pandas as pd
from sklearn.linear_model import LinearRegression
import os
from dotenv import load_dotenv

load_dotenv()

db = mysql.connector.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    database=os.getenv("DB_NAME"),
    port=int(os.getenv("DB_PORT")),
    ssl_ca="ca.pem"
)
cursor = db.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS books (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255),
    rating VARCHAR(50),
    price DECIMAL(10, 2),
    availability VARCHAR(50),
    image_url TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS price_history (
    id INT AUTO_INCREMENT PRIMARY KEY,
    book_id INT,
    book_title VARCHAR(255),
    price DECIMAL(10, 2),
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

current_url = "https://books.toscrape.com/catalogue/page-1.html"
    
headers = {
   "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

all_books = []
page_count = 1

while current_url:
    print(f"Checking Page {page_count}: {current_url}")
        
    try:
        response = requests.get(current_url, timeout=10)
        response.raise_for_status()
    except Exception as e:
        print(f"Stopped at page {page_count} due to error: {e}")
        break

    soup = BeautifulSoup(response.text, "html.parser")
    books = soup.find_all("article", class_="product_pod")

    data = []
    for book in books:
        try:
            title = book.find("h3").find("a")["title"]
            price_text = book.find("p",class_='price_color').text
            price = float(price_text.replace('£', '').replace('Â', ''))
            rating = book.find('p',class_="star-rating")["class"][1]
    
            cursor.execute("INSERT IGNORE INTO books (title, rating) VALUES (%s, %s)", (title, rating))
            cursor.execute("SELECT id FROM books WHERE title = %s", (title,))
            book_id = cursor.fetchone()[0]
            cursor.execute("""
                INSERT INTO price_history (book_id, price, scraped_at) 
                VALUES (%s, %s, %s)
        """, (book_id, price, datetime.now()))
            
            all_books.append(title)

        except AttributeError:
            title = None
            print('Attribute Error')
            
    db.commit()  
    next_button = soup.select_one('li.next a')
        
    if next_button:
        relative_link = next_button['href']
        current_url = urljoin(current_url, relative_link)
        page_count += 1
    else:
        print(f"Reached the last page. Scraping finished and captured {len(all_books)} books")
        current_url = None

db.close()

print("\n--- Starting Business Intelligence Analysis ---")

db_analysis = mysql.connector.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    database=os.getenv("DB_NAME"),
    port=int(os.getenv("DB_PORT")),
    ssl_ca="ca.pem"
)

query = """
    SELECT b.id, b.title, b.rating, p.price, p.scraped_at
    FROM books b
    JOIN price_history p ON b.id = p.book_id
"""
df = pd.read_sql(query, db_analysis)
db_analysis.close()

df['scraped_at'] = pd.to_datetime(df['scraped_at'])
df['day_offset'] = (df['scraped_at'] - df['scraped_at'].min()).dt.total_seconds() / 86400

trends = []

for book_id in df['id'].unique():
    book_subset = df[df['id'] == book_id]
    
    if len(book_subset) >= 2:  
        X = book_subset[['day_offset']].values 
        y = book_subset['price'].values
        model = LinearRegression().fit(X, y)
        slope = model.coef_[0]
        
        trends.append({
            'Title': book_subset['title'].iloc[0],
            'Rating': book_subset['rating'].iloc[0],
            'Current Price': y[-1],
            'Trend Score': round(slope, 2)
        })

if trends:
    trend_df = pd.DataFrame(trends)
    gems = trend_df[trend_df['Trend Score'] < 0].sort_values(by='Trend Score')
    
    print(f"Analysis complete. Found {len(gems)} books with dropping prices.")
    print(gems[['Title', 'Current Price', 'Trend Score']].head(10))
else:
    print("Not enough historical data yet to calculate trends.")
