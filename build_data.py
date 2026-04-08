import pandas as pd
import requests
import json
import time
import re
from datetime import datetime

# --- CONFIGURATION ---
INPUT_CSV = '/Users/ed009/Downloads/goodreads_library_export.csv'
OUTPUT_JSON = 'books.json'

def clean_isbn(raw):
    """Strip Goodreads ISBN format: =\"9781234567890\" → 9781234567890"""
    if not isinstance(raw, str): return ""
    return re.sub(r'[^0-9X]', '', raw)

def get_cover(isbn):
    """Try Open Library first, then Google Books."""
    if not isbn or len(isbn) < 10:
        return None

    # Open Library (check content-length to skip placeholder 1px images)
    ol_url = f"https://covers.openlibrary.org/b/isbn/{isbn}-L.jpg"
    try:
        r = requests.head(ol_url, timeout=5)
        if r.status_code == 200 and int(r.headers.get('content-length', 0)) > 2000:
            return ol_url
    except Exception:
        pass

    # Google Books fallback
    gb_url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}"
    try:
        r = requests.get(gb_url, timeout=5)
        data = r.json()
        if data.get('items'):
            img = data['items'][0]['volumeInfo'].get('imageLinks', {})
            cover = img.get('thumbnail') or img.get('smallThumbnail')
            if cover:
                # Force HTTPS and request larger image
                return cover.replace('http://', 'https://').replace('zoom=1', 'zoom=0')
    except Exception:
        pass

    return None

def parse_date(date_str):
    """Parse Goodreads date formats: YYYY/MM/DD or MM/DD/YYYY → (display, sortable)."""
    if not date_str or str(date_str).strip() in ('', 'nan'):
        return ("", "0000-00-00")
    date_str = str(date_str).strip()
    for fmt in ('%Y/%m/%d', '%m/%d/%Y', '%Y-%m-%d', '%B %d, %Y'):
        try:
            dt = datetime.strptime(date_str, fmt)
            return (dt.strftime("%B %d, %Y"), dt.strftime("%Y-%m-%d"))
        except ValueError:
            continue
    return (date_str, "0000-00-00")

def parse_shelves(raw):
    """Parse Goodreads bookshelves into a list, excluding default shelves."""
    default = {'read', 'currently-reading', 'to-read'}
    if not raw or str(raw).strip() in ('', 'nan'):
        return []
    return [s.strip() for s in str(raw).split(',') if s.strip() and s.strip() not in default]

# --- MAIN EXECUTION ---

try:
    try:
        df = pd.read_csv(INPUT_CSV, on_bad_lines='skip', encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv(INPUT_CSV, on_bad_lines='skip', encoding='utf-8-sig')
except Exception as e:
    print(f"Error reading CSV: {e}")
    exit()

df = df.fillna("")

# Only process books on the "read" shelf
read_df = df[df['Exclusive Shelf'].str.strip() == 'read'].copy()
print(f"Found {len(read_df)} books on 'read' shelf. Processing...")

book_list = []

for _, row in read_df.iterrows():
    title = str(row.get('Title', '')).strip()
    if not title:
        continue

    author = str(row.get('Author', '')).strip()
    isbn13 = clean_isbn(str(row.get('ISBN13', '')))
    isbn   = clean_isbn(str(row.get('ISBN', '')))
    isbn_to_use = isbn13 or isbn

    my_rating   = str(row.get('My Rating', '')).strip()
    avg_rating  = str(row.get('Average Rating', '')).strip()
    pages       = str(row.get('Number of Pages', '')).strip()
    year_pub    = str(row.get('Original Publication Year', '') or row.get('Year Published', '')).strip()
    if year_pub.endswith('.0'):
        year_pub = year_pub[:-2]

    review      = str(row.get('My Review', '')).strip()
    shelves     = parse_shelves(row.get('Bookshelves', ''))
    date_display, date_sort = parse_date(row.get('Date Read', ''))

    print(f"  [{title[:40]}] isbn={isbn_to_use or 'N/A'} ...", end=' ')
    cover = get_cover(isbn_to_use)

    if cover:
        print(f"Cover OK")
    else:
        print(f"No cover")

    book_list.append({
        "title":        title,
        "author":       author,
        "date_display": date_display,
        "date_sort":    date_sort,
        "review":       review,
        "rating":       my_rating,
        "cover":        cover or "",
        "pages":        pages,
        "year_published": year_pub,
        "genres":       shelves,
        "isbn":         isbn_to_use
    })

    time.sleep(0.15)

# Sort by date_sort descending (most recently read first)
book_list.sort(key=lambda b: b['date_sort'], reverse=True)

with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
    json.dump(book_list, f, indent=4, ensure_ascii=False)

total  = len(book_list)
covers = sum(1 for b in book_list if b['cover'])
print(f"\nDone! Saved {total} books ({covers} with covers, {total - covers} missing covers).")
