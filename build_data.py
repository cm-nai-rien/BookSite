import pandas as pd
import requests
import json
import time
import re
from datetime import datetime

# --- CONFIGURATION ---
INPUT_CSV  = '/Users/ed009/Downloads/goodreads_library_export.csv'
OUTPUT_JSON = 'books.json'
# Optional: add your Google Books API key for higher rate limits
# Get one free at: console.cloud.google.com (Books API, 1000 req/day free without key)
GOOGLE_API_KEY = ''   # leave empty to use without key

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def clean_isbn(raw):
    """Strip Goodreads =\"...\" format."""
    if not isinstance(raw, str): return ''
    return re.sub(r'[^0-9X]', '', raw)

def parse_date(date_str):
    if not date_str or str(date_str).strip() in ('', 'nan'):
        return ('', '0000-00-00')
    date_str = str(date_str).strip()
    for fmt in ('%Y/%m/%d', '%m/%d/%Y', '%Y-%m-%d', '%B %d, %Y'):
        try:
            dt = datetime.strptime(date_str, fmt)
            return (dt.strftime('%B %d, %Y'), dt.strftime('%Y-%m-%d'))
        except ValueError:
            continue
    return (date_str, '0000-00-00')

def parse_shelves(raw):
    default = {'read', 'currently-reading', 'to-read'}
    if not raw or str(raw).strip() in ('', 'nan'):
        return []
    return [s.strip() for s in str(raw).split(',') if s.strip() and s.strip() not in default]

def clean_category(cat):
    """'Literary Criticism / General' → 'Literary Criticism'"""
    return cat.split('/')[0].strip()

# ---------------------------------------------------------------------------
# COVER + CATEGORY FETCHING
# ---------------------------------------------------------------------------

def google_books_query(params):
    """Hit Google Books API and return the first volume item, or None."""
    base = 'https://www.googleapis.com/books/v1/volumes'
    if GOOGLE_API_KEY:
        params['key'] = GOOGLE_API_KEY
    try:
        r = requests.get(base, params=params, timeout=6)
        data = r.json()
        if data.get('totalItems', 0) > 0 and data.get('items'):
            return data['items'][0]
    except Exception:
        pass
    return None

def extract_from_volume(item):
    """Pull cover URL and categories from a Google Books volume item."""
    info = item.get('volumeInfo', {})
    img  = info.get('imageLinks', {})
    cover = (img.get('thumbnail') or img.get('smallThumbnail') or '')
    if cover:
        # Force HTTPS, remove edge curl, bump zoom for larger image
        cover = cover.replace('http://', 'https://')
        cover = re.sub(r'&edge=\w+', '', cover)
        cover = cover.replace('zoom=1', 'zoom=0')
    categories = [clean_category(c) for c in info.get('categories', [])]
    # Deduplicate while preserving order
    seen = set()
    categories = [c for c in categories if not (c in seen or seen.add(c))]
    return cover or None, categories

def get_book_data(isbn, title, author):
    """
    Try in order:
      1. Open Library by ISBN (fast, high-res)
      2. Google Books by ISBN
      3. Google Books by title + author (for books without ISBN)
    Returns (cover_url, categories).
    """
    # --- 1. Open Library ---
    if isbn:
        ol_url = f'https://covers.openlibrary.org/b/isbn/{isbn}-L.jpg'
        try:
            r = requests.head(ol_url, timeout=5)
            if r.status_code == 200 and int(r.headers.get('content-length', 0)) > 2000:
                return ol_url, []   # OL has no category data; will fill via Google Books below
        except Exception:
            pass

    # --- 2. Google Books by ISBN ---
    if isbn:
        item = google_books_query({'q': f'isbn:{isbn}'})
        if item:
            cover, cats = extract_from_volume(item)
            if cover:
                return cover, cats

    # --- 3. Google Books by title + author ---
    if title:
        q = f'intitle:{title}'
        if author:
            # Use only first author's last name to avoid noise
            last = author.split(',')[0].split()[-1] if author else ''
            if last:
                q += f'+inauthor:{last}'
        item = google_books_query({'q': q})
        if item:
            cover, cats = extract_from_volume(item)
            return cover, cats   # return even if no cover (we still get categories)

    return None, []

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

try:
    try:
        df = pd.read_csv(INPUT_CSV, on_bad_lines='skip', encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv(INPUT_CSV, on_bad_lines='skip', encoding='utf-8-sig')
except Exception as e:
    print(f'Error reading CSV: {e}'); exit()

df = df.fillna('')
read_df = df[df['Exclusive Shelf'].str.strip() == 'read'].copy()
print(f'Found {len(read_df)} books on "read" shelf. Processing...\n')

book_list = []

for _, row in read_df.iterrows():
    title  = str(row.get('Title', '')).strip()
    if not title: continue

    author    = str(row.get('Author', '')).strip()
    isbn13    = clean_isbn(str(row.get('ISBN13', '')))
    isbn      = clean_isbn(str(row.get('ISBN', '')))
    isbn_use  = isbn13 or isbn

    my_rating = str(row.get('My Rating', '')).strip()
    pages     = str(row.get('Number of Pages', '')).strip()
    year_pub  = str(row.get('Original Publication Year', '') or row.get('Year Published', '')).strip()
    if year_pub.endswith('.0'): year_pub = year_pub[:-2]

    review    = str(row.get('My Review', '')).strip()
    shelves   = parse_shelves(row.get('Bookshelves', ''))
    date_display, date_sort = parse_date(row.get('Date Read', ''))

    short = title[:40]
    print(f'  [{short:<40}] isbn={isbn_use or "N/A":<14}', end=' ')

    cover, categories = get_book_data(isbn_use, title, author)

    # Merge Goodreads shelves + Google Books categories (deduplicated)
    all_genres = list(dict.fromkeys(shelves + categories))

    status = f'cover={"✓" if cover else "✗"}  cats={categories[:2] if categories else "none"}'
    print(status)

    book_list.append({
        'title':          title,
        'author':         author,
        'date_display':   date_display,
        'date_sort':      date_sort,
        'review':         review,
        'rating':         my_rating,
        'cover':          cover or '',
        'pages':          pages,
        'year_published': year_pub,
        'genres':         all_genres,
        'isbn':           isbn_use,
    })

    time.sleep(0.2)

book_list.sort(key=lambda b: b['date_sort'], reverse=True)

with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
    json.dump(book_list, f, indent=4, ensure_ascii=False)

total  = len(book_list)
covers = sum(1 for b in book_list if b['cover'])
cats   = sum(1 for b in book_list if b['genres'])
print(f'\nDone! {total} books — {covers} with covers, {cats} with categories, {total - covers} still missing covers.')
