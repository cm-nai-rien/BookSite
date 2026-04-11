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

def google_books_query(params, max_results=1):
    """Hit Google Books API and return items list, or []."""
    base = 'https://www.googleapis.com/books/v1/volumes'
    if GOOGLE_API_KEY:
        params['key'] = GOOGLE_API_KEY
    params['maxResults'] = max_results
    try:
        r = requests.get(base, params=params, timeout=6)
        data = r.json()
        if data.get('totalItems', 0) > 0 and data.get('items'):
            return data['items']
    except Exception:
        pass
    return []

def extract_from_volume(item):
    """Pull cover URL, ISBN, and categories from a Google Books volume item."""
    info = item.get('volumeInfo', {})
    img  = info.get('imageLinks', {})
    cover = (img.get('extraLarge') or img.get('large') or img.get('medium') or
             img.get('thumbnail') or img.get('smallThumbnail') or '')
    if cover:
        cover = cover.replace('http://', 'https://')
        cover = re.sub(r'&edge=\w+', '', cover)
        cover = re.sub(r'zoom=\d+', 'zoom=0', cover)

    # Extract ISBN-13 (preferred) or ISBN-10
    isbn = ''
    for id_obj in info.get('industryIdentifiers', []):
        if id_obj['type'] == 'ISBN_13':
            isbn = id_obj['identifier']
            break
    if not isbn:
        for id_obj in info.get('industryIdentifiers', []):
            if id_obj['type'] == 'ISBN_10':
                isbn = id_obj['identifier']
                break

    categories = [clean_category(c) for c in info.get('categories', [])]
    seen = set()
    categories = [c for c in categories if not (c in seen or seen.add(c))]
    return cover or None, isbn or None, categories

def get_book_data(isbn, title, author):
    """
    Try in order:
      1. Open Library by ISBN (fast, high-res)
      2. Google Books by ISBN
      3. Google Books by title + author
      4. Google Books by original English title (for translated books)
    Returns (cover_url, found_isbn, categories).
    """
    # --- 1. Open Library by ISBN ---
    if isbn:
        ol_url = f'https://covers.openlibrary.org/b/isbn/{isbn}-L.jpg'
        try:
            r = requests.head(ol_url, timeout=5)
            if r.status_code == 200 and int(r.headers.get('content-length', 0)) > 2000:
                return ol_url, isbn, []
        except Exception:
            pass

    # --- 2. Google Books by ISBN ---
    if isbn:
        items = google_books_query({'q': f'isbn:{isbn}'})
        if items:
            cover, found_isbn, cats = extract_from_volume(items[0])
            if cover:
                return cover, found_isbn or isbn, cats

    # --- 3. Google Books by title + author ---
    if title:
        queries = []
        if author:
            last = author.split(',')[0].split()[-1] if author else ''
            if last:
                queries.append(f'intitle:{title}+inauthor:{last}')
        queries.append(title if not author else f'{title} {author}')
        queries.append(title)  # title only as last resort

        for q in queries:
            items = google_books_query({'q': q}, max_results=5)
            for item in items:
                cover, found_isbn, cats = extract_from_volume(item)
                if cover:
                    return cover, found_isbn or isbn or None, cats
            time.sleep(0.1)

    return None, isbn or None, []

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

# Load existing books to enable incremental updates
existing_books = {}
try:
    with open(OUTPUT_JSON, encoding='utf-8') as f:
        for book in json.load(f):
            existing_books[book['title']] = book
    print(f'Loaded {len(existing_books)} existing books from {OUTPUT_JSON}.')
except FileNotFoundError:
    print(f'No existing {OUTPUT_JSON} found — building from scratch.')

try:
    try:
        df = pd.read_csv(INPUT_CSV, on_bad_lines='skip', encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv(INPUT_CSV, on_bad_lines='skip', encoding='utf-8-sig')
except Exception as e:
    print(f'Error reading CSV: {e}'); exit()

df = df.fillna('')
read_df = df[df['Exclusive Shelf'].str.strip() == 'read'].copy()
print(f'Found {len(read_df)} books on "read" shelf.\n')

book_list = []
new_count = 0
skipped_count = 0

for _, row in read_df.iterrows():
    title = str(row.get('Title', '')).strip()
    if not title: continue

    # --- Incremental: reuse existing entry if already processed ---
    if title in existing_books:
        book_list.append(existing_books[title])
        skipped_count += 1
        continue

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
    print(f'  [NEW] [{short:<40}] isbn={isbn_use or "N/A":<14}', end=' ')

    cover, found_isbn, categories = get_book_data(isbn_use, title, author)

    # Use found ISBN if original was missing
    final_isbn = isbn_use or found_isbn or ''

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
        'isbn':           final_isbn,
    })
    new_count += 1
    time.sleep(0.2)

book_list.sort(key=lambda b: b['date_sort'], reverse=True)

with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
    json.dump(book_list, f, indent=4, ensure_ascii=False)

total  = len(book_list)
covers = sum(1 for b in book_list if b['cover'])
print(f'\nDone! {total} books total — {new_count} new, {skipped_count} unchanged.')
print(f'Covers: {covers}/{total} | Missing: {total - covers}')
