"""
BuyNewGadget.com — Pipeline v5
================================
Upload CSV/XLSX → Auto Process → Download CSV or Excel → WP Admin Import

Install:
    pip install streamlit pandas openpyxl
    streamlit run bng_pipeline_v5.py
"""

import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import re
from io import BytesIO

# ─────────────────────────────────────────────────────────────────────────────
# VALIDATOR
# ─────────────────────────────────────────────────────────────────────────────
REQUIRED_COLUMNS = ['Name', 'City', 'Country', 'First_category']

def validate_file(df):
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        return False, f"Missing required columns: {', '.join(missing)}"
    if df['Name'].isna().all():
        return False, "All rows in the 'Name' column are empty."
    return True, ""

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def clean_num(v):
    if pd.isna(v): return "0.0"
    m = re.search(r"[-+]?\d+\.\d+|\d+", str(v))
    return m.group() if m else "0.0"

def format_phone(v):
    if pd.isna(v) or str(v).strip().lower() in ["nan", ""]: return ""
    digits = re.sub(r'\D', '', str(v))
    return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}" if len(digits) == 10 else digits or ""

def slugify(text):
    text = str(text).lower().strip()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[\s_]+', '-', text)
    return re.sub(r'-+', '-', text).strip('-')

def val(v):
    s = str(v).strip()
    return "" if s.lower() in ["nan", "n/a", "none", ""] else s

def safe_get(row, *keys):
    for key in keys:
        v = val(str(row.get(key, '')))
        if v: return v
    return ""

def brand_from_domain(domain):
    d = val(str(domain))
    if not d: return ""
    d = re.sub(r'^www\.', '', d.lower())
    d = re.sub(r'\.[a-z]{2,}.*$', '', d)
    return re.sub(r'[^a-z0-9]', '', d)

def smart_social(row, platform):
    col_map = {
        "facebook":  ["Facebook_URL"],
        "twitter":   ["Twitter_URL"],
        "instagram": ["Instagram_URL"],
        "linkedin":  ["Linkedin_URL"],
        "youtube":   ["Youtube_URL"],
    }
    for col in col_map.get(platform, []):
        v = val(str(row.get(col, '')))
        if v: return v
    brand = brand_from_domain(row.get('Domain', ''))
    if not brand: return ""
    return {
        "facebook":  "https://www.facebook.com/" + brand,
        "twitter":   "https://x.com/" + brand,
        "instagram": "https://www.instagram.com/" + brand,
        "linkedin":  "https://www.linkedin.com/company/" + brand,
        "youtube":   "https://www.youtube.com/@" + brand,
    }.get(platform, "")

def smart_email(row):
    for col in ['Email', 'Email_From_WEBSITE']:
        v = val(str(row.get(col, '')))
        if v and '@' in v: return v
    domain = val(str(row.get('Domain', '')))
    return ("info@" + domain) if domain else ""

# ─────────────────────────────────────────────────────────────────────────────
# TEMPLATE — Rich, unique, SEO content + FAQ
# ─────────────────────────────────────────────────────────────────────────────
def build_template(row, name, cat, city, state, country):
    import random, hashlib

    cat_link  = f'<a href="https://buynewgadget.com/gadget-category/{slugify(cat)}/">{cat}</a>'
    loc_url   = f"https://buynewgadget.com/gadget-location/{slugify(country)}/{slugify(city)}/"
    loc_link  = f'<a href="{loc_url}">{city}</a>'
    cat_city  = f'<a href="{loc_url}">{cat} stores in {city}</a>'
    rating    = val(str(row.get('Average_rating',  ''))) or '4.0'
    reviews   = val(str(row.get('Reviews_count',   ''))) or '0'
    summary   = (val(str(row.get('Summary', ''))) or val(str(row.get('Description', ''))) or
                 val(str(row.get('Sub_Title', ''))) or val(str(row.get('Meta_Description', ''))))
    services  = val(str(row.get('Service_options', '')))
    payments  = val(str(row.get('Payments',        '')))
    access    = val(str(row.get('Accessibility',   '')))
    amenities = val(str(row.get('Amenities',       '')))
    offerings = val(str(row.get('Offerings',       '')))
    crowd     = val(str(row.get('Crowd',           '')))
    planning  = val(str(row.get('Planning',        '')))
    hours     = val(str(row.get('Hours',           '')))
    phone     = val(str(row.get('Phone_Standard_format', '') or row.get('Phone_1', '')))
    website   = val(str(row.get('Website',         '')))
    status    = val(str(row.get('Business_Status', '')))
    claimed   = val(str(row.get('Claimed_google_my_business', '')))
    pincode   = val(str(row.get('Zip',             '')))
    address   = val(str(row.get('Full_Address',    '')))

    # Use name hash for deterministic variety (same name = same variant every run)
    seed = int(hashlib.md5(name.encode()).hexdigest()[:8], 16)
    rng  = random.Random(seed)

    # ── Opening sentence variants ─────────────────────────────────────────────
    rating_f = float(rating) if rating else 4.0
    if rating_f >= 4.5:
        praise = rng.choice([
            f"one of the highest-rated {cat.lower()} destinations",
            f"a top-rated choice for {cat.lower()} shoppers",
            f"widely regarded as a leading {cat.lower()} provider",
        ])
    elif rating_f >= 4.0:
        praise = rng.choice([
            f"a well-regarded {cat.lower()} store",
            f"a trusted name in {cat.lower()}",
            f"a popular choice for {cat.lower()} needs",
        ])
    else:
        praise = rng.choice([
            f"a local {cat.lower()} store",
            f"a known {cat.lower()} provider",
            f"an established {cat.lower()} shop",
        ])

    open_variants = [
        f"Located in the heart of {loc_link}, {state}, <strong>{name}</strong> stands out as {praise} in the region.",
        f"<strong>{name}</strong> is {praise} serving customers in {loc_link}, {state} and the surrounding areas.",
        f"If you're looking for {cat_link} in {loc_link}, <strong>{name}</strong> is {praise} worth visiting.",
    ]
    p1 = rng.choice(open_variants) + " "
    if summary:
        p1 += (summary[:150] + "…" if len(summary) > 150 else summary)
    else:
        p1 += rng.choice([
            f"The store is committed to delivering quality products and excellent customer service to every visitor.",
            f"Customers can expect a knowledgeable team, a wide product selection, and a welcoming in-store experience.",
            f"Whether you're a first-time visitor or a returning customer, the team at {name} is ready to assist you.",
        ])

    # ── Services & features paragraph ─────────────────────────────────────────
    p2_parts = []
    if services:
        svc_list = [s.strip() for s in re.split(r'[|,]', services) if s.strip()][:5]
        if svc_list:
            p2_parts.append(f"Key service options include {', '.join(svc_list).lower()}.")
    if offerings:
        p2_parts.append(f"The store also offers {offerings.lower()[:120]}.")
    if payments:
        pay_list = [p.strip() for p in re.split(r'[|,]', payments) if p.strip()][:4]
        if pay_list:
            p2_parts.append(f"Accepted payment methods include {', '.join(pay_list).lower()}.")
    if access:
        p2_parts.append(f"Accessibility features available: {access.lower()[:100]}.")
    if amenities:
        p2_parts.append(f"On-site amenities include {amenities.lower()[:100]}.")
    if crowd:
        p2_parts.append(f"This location is known to be {crowd.lower()}.")
    if planning:
        p2_parts.append(f"Useful to know: {planning.lower()}.")

    if p2_parts:
        p2 = f"<strong>{name}</strong> offers a range of options to suit different customer needs. " + " ".join(p2_parts)
    else:
        p2 = (f"<strong>{name}</strong> caters to a wide range of {cat.lower()} needs in {city}. "
              f"The store is known for its helpful staff and well-stocked inventory, making it a "
              f"convenient stop for both individual buyers and business customers in {state}.")

    # ── Rating & location paragraph ───────────────────────────────────────────
    rev_int = int(reviews.replace(',', '')) if reviews.replace(',', '').isdigit() else 0
    if rev_int >= 1000:
        rev_note = f"an impressive {reviews} customer reviews"
    elif rev_int >= 100:
        rev_note = f"{reviews} verified customer reviews"
    else:
        rev_note = f"{reviews} reviews" if reviews != '0' else "customer reviews"

    if status and status.lower() == 'open':
        status_note = "Currently open for business. "
    else:
        status_note = ""

    p3 = (f"{status_note}With a rating of <strong>{rating}/5</strong> from {rev_note}, "
          f"{name} is a highly recommended {cat.lower()} in {city}. "
          f"Find more {cat_city} or explore the full directory of "
          f"{cat_link} listings on BuyNewGadget.com.")

    # ── What to expect paragraph (only if enough data) ────────────────────────
    extra_parts = []
    if address:
        extra_parts.append(f"The store is conveniently located at {address[:100]}.")
    if pincode:
        extra_parts.append(f"Serving the {pincode} area and nearby neighbourhoods.")
    if claimed and claimed.lower() == 'yes':
        extra_parts.append(f"This listing has been verified on Google My Business.")
    if phone:
        extra_parts.append(f"You can reach the store directly at {phone}.")

    p4 = ""
    if extra_parts:
        p4 = f"<p>{'  '.join(extra_parts)}</p>"

    # ── FAQ section ───────────────────────────────────────────────────────────
    faqs = []

    faqs.append((
        f"Where is {name} located?",
        address if address else f"{name} is located in {city}, {state}, {country}."
    ))

    if phone:
        faqs.append((
            f"What is the contact number for {name}?",
            f"You can contact {name} at {phone}."
        ))

    if services:
        svc_short = services[:120]
        faqs.append((
            f"What services does {name} offer?",
            f"{name} offers the following services: {svc_short.lower()}."
        ))

    if payments:
        faqs.append((
            f"What payment methods does {name} accept?",
            f"{name} accepts: {payments[:100].lower()}."
        ))

    if rating and reviews and reviews != '0':
        faqs.append((
            f"Is {name} a good {cat.lower()} in {city}?",
            f"Yes — {name} has a rating of {rating}/5 based on {reviews} customer reviews, making it one of the recommended {cat.lower()} options in {city}."
        ))

    if access:
        faqs.append((
            f"Is {name} wheelchair accessible?",
            f"Accessibility information for {name}: {access[:120].lower()}."
        ))

    faqs.append((
        f"How can I find more {cat.lower()} stores near {city}?",
        f'You can browse all <a href="https://buynewgadget.com/gadget-category/{slugify(cat)}/">{cat} stores</a> or explore <a href="{loc_url}">{cat} listings in {city}</a> on BuyNewGadget.com.'
    ))

    faq_items = ""
    for q, a in faqs[:5]:
        faq_items += f'<details style="border-bottom:1px solid #e5e7eb;"><summary style="padding:14px 4px;font-size:14px;font-weight:600;color:#1e3a5f;cursor:pointer;list-style:none;display:flex;justify-content:space-between;">{q} <span style="font-size:20px;font-weight:300;">+</span></summary><div style="padding:4px 4px 16px;font-size:14px;color:#374151;line-height:1.8;">{a}</div></details>'

    faq_html = (
        f'<div style="margin:28px 0;border:1px solid #e5e7eb;border-radius:10px;overflow:hidden;">'
        f'<div style="font-size:16px;font-weight:700;color:#111827;margin:0;padding:16px 20px;background:#f8fafc;border-bottom:1px solid #e5e7eb;">❓ Frequently Asked Questions</div>'
        f'<div style="padding:0 16px;">{faq_items}</div>'
        f'</div>'
    )

    # ── Assemble ──────────────────────────────────────────────────────────────
    return (
        f'<h2>About {name} — {cat} in {city}, {state}</h2>'
        f'<p>{p1}</p>'
        f'<p>{p2}</p>'
        f'<p>{p3}</p>'
        f'{p4}'
        f'{faq_html}'
    )

# ─────────────────────────────────────────────────────────────────────────────
# PROCESS ONE ROW
# ─────────────────────────────────────────────────────────────────────────────
ERROR_CODES = ["#VALUE!", "#N/A", "#REF!", "#DIV/0!", "#NAME?", "#NULL!"]

def process_row(row):
    if any(str(v).strip().upper() in ERROR_CODES for v in row.values):
        return None
    name     = val(str(row.get('Name', '')))           or 'Unknown'
    city     = val(str(row.get('City', '')))           or 'Unknown'
    state    = val(str(row.get('State', '')))          or ''
    zip_code = val(str(row.get('Zip', '')))            or ''
    cat      = val(str(row.get('First_category', ''))) or 'Business'
    country  = val(str(row.get('Country', '')))        or ''
    return {
        "Name":                 name if city.lower() in name.lower() else f"{name} | {city}",
        "Category":             cat,
        "Contact 1":            format_phone(safe_get(row, 'Phone_Standard_format', 'Phone_1')),
        "Contact 2":            format_phone(safe_get(row, 'Phone_1')),
        "Email":                smart_email(row),
        "Website 1":            safe_get(row, 'Website'),
        "Website 2":            safe_get(row, 'Domain'),
        "Full Address":         safe_get(row, 'Full_Address'),
        "City":                 city,
        "State":                state,
        "Pincode":              zip_code,
        "Country":              country,
        "Latitude":             clean_num(row.get('Latitude',  '0')),
        "Longitude":            clean_num(row.get('Longitude', '0')),
        "Google Maps Link":     safe_get(row, 'GMB_URL'),
        "Average Rating":       val(str(row.get('Average_rating', '0'))) or '0',
        "Review Count":         val(str(row.get('Reviews_count',  '0'))) or '0',
        "Facebook":             smart_social(row, "facebook"),
        "LinkedIn":             smart_social(row, "linkedin"),
        "Twitter":              smart_social(row, "twitter"),
        "Instagram":            smart_social(row, "instagram"),
        "YouTube":              smart_social(row, "youtube"),
        "Source URL":           safe_get(row, "Review_URL"),
        "SEO Title":            f"{name} — {cat} in {city}, {state} | BuyNewGadget",
        "SEO Description":      f"Visit {name}, a top-rated {cat} in {city}, {state}. Check contact details, opening hours, services, reviews and directions. Find more {cat} stores near {city} on BuyNewGadget.com.",
        "SEO Keywords":         f"{name}, {cat} in {city}, {cat} {state}, {cat} near me, {city} {cat}, buy {cat.lower()} {city}, {cat} store {country}, best {cat} {city}",
        "Content Description":  build_template(row, name, cat, city, state, country),
        "Monday":               val(str(row.get('Monday',    ''))) or 'Closed',
        "Tuesday":              val(str(row.get('Tuesday',   ''))) or 'Closed',
        "Wednesday":            val(str(row.get('Wednesday', ''))) or 'Closed',
        "Thursday":             val(str(row.get('Thursday',  ''))) or 'Closed',
        "Friday":               val(str(row.get('Friday',    ''))) or 'Closed',
        "Saturday":             val(str(row.get('Saturday',  ''))) or 'Closed',
        "Sunday":               val(str(row.get('Sunday',    ''))) or 'Closed',
        "Amenities & Features": " | ".join([
            "Service: "       + (val(str(row.get("Service_options", ""))) or "N/A"),
            "Payments: "      + (val(str(row.get("Payments", "")))        or "N/A"),
            "Amenities: "     + (val(str(row.get("Amenities", "")))       or "N/A"),
            "Accessibility: " + (val(str(row.get("Accessibility", "")))   or "N/A"),
        ]),
    }

# ─────────────────────────────────────────────────────────────────────────────
# EXPORT
# ─────────────────────────────────────────────────────────────────────────────
def to_excel(df):
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Listings')
    return buf.getvalue()

def to_csv(df):
    return df.to_csv(index=False).encode('utf-8')

# ─────────────────────────────────────────────────────────────────────────────
# APP
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="BNG Pipeline1",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
    html, body, .stApp,
    [data-testid="stAppViewContainer"],
    [data-testid="stHeader"] {
        background-color: #ffffff !important;
        color: #ffffff !important;
    }
    p, span, label, div, h1, h2, h3, li { color: #000000 !important; }
    .stMetric label, [data-testid="stMetricValue"] { color: #ffffff !important; }
    [data-testid="stFileUploader"] { background-color: #f8f9fb !important; }
    input, textarea { color: #ffffff !important; background: #ffffff !important; }
</style>
""", unsafe_allow_html=True)

st.title("⚡ BNG Pipeline")
st.caption("Upload CSV/XLSX → Auto Process → Download → WP Admin Import")
st.divider()

# ── UPLOAD — auto-process on upload ──────────────────────────────────────────
uploaded_file = st.file_uploader(
    "Upload your Lead Spinner CSV or XLSX file",
    type=["csv", "xlsx"]
)

if uploaded_file:
    try:
        df_full = pd.read_csv(uploaded_file) if uploaded_file.name.endswith('.csv') \
                  else pd.read_excel(uploaded_file, engine='openpyxl')
    except Exception as e:
        st.error(f"Could not read file: {e}")
        st.stop()

    ok, msg = validate_file(df_full)
    if not ok:
        st.error(f"❌ {msg}")
        st.stop()

    # Auto-process immediately
    results  = []
    total    = len(df_full)
    progress = st.progress(0)

    for idx, row in df_full.iterrows():
        progress.progress(
            (idx + 1) / total,
            text=f"⚙️ Processing {idx+1} of {total} rows..."
        )
        entry = process_row(row)
        if entry:
            results.append(entry)

    progress.empty()
    output_df = pd.DataFrame(results)

    st.success(f"✅ {len(output_df):,} of {total:,} rows ready — download below and import via WP Admin.")
    st.divider()

    st.download_button(
        "📥 Download as CSV",
        to_csv(output_df),
        "bng_output.csv",
        "text/csv",
        use_container_width=True,
        type="primary"
    )
