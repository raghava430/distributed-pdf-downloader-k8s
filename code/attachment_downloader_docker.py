import os, re, time, json, logging, random
from pathlib import Path 
from typing import List, Dict, Any, Tuple 
from urllib.parse import urlparse, parse_qs, unquote 
import requests 
import pandas as pd 
from tqdm import tqdm 
from selenium import webdriver  # Tool to control a web browser automatically
from selenium.webdriver.common.by import By  # Different ways to find elements on web pages
from selenium.webdriver.chrome.options import Options as ChromeOptions  # Settings for Chrome browser
from selenium.webdriver.chrome.service import Service   # Helps connect to Chrome driver
from selenium.webdriver.support.ui import WebDriverWait   # Wait for things to load on webpage
from selenium.webdriver.support import expected_conditions as EC   # Conditions to wait for like button becoming clickable
from selenium.common.exceptions import TimeoutException  # Error types that can happen
from concurrent.futures import ThreadPoolExecutor, as_completed   # Tools for running multiple tasks at same time

import oci
from oci.auth.signers import InstancePrincipalsSecurityTokenSigner

OCI_NAMESPACE = os.getenv("OCI_NAMESPACE", "idjxslnerp5l")
OCI_BUCKET    = os.getenv("OCI_BUCKET", "pdf-v")
OCI_PREFIX    = os.getenv("OCI_PREFIX", "downloads")

_signer   = InstancePrincipalsSecurityTokenSigner()
_osclient = oci.object_storage.ObjectStorageClient(config={}, signer=_signer)

def upload_to_oci(local_path: Path, object_name: str):
    with open(local_path, "rb") as f:
        _osclient.put_object(OCI_NAMESPACE, OCI_BUCKET, object_name, f)


# ========== RUNTIME CONFIG (env-driven for Docker/OCI) ==========
from datetime import datetime

TOTAL_URLS = int(os.getenv("TOTAL_URLS", "50"))
PARALLEL_DOWNLOADS = int(os.getenv("PARALLEL_DOWNLOADS", "1"))

EXCEL_PATH = os.getenv("EXCEL_PATH", "/app/input/urls.xlsx")
OUT_DIR = os.getenv("OUT_DIR", "/app/downloads")

RUN_TS = os.getenv("RUN_TS", datetime.now().strftime("%Y-%m-%d_%H%M%S"))
RUN_KEY = os.getenv("RUN_KEY", RUN_TS)   # used to resume a run
RUN_DIR = os.getenv("RUN_DIR", RUN_KEY)  # output folder name; default is RUN_KEY

PAGE_TIMEOUT = int(os.getenv("PAGE_TIMEOUT", "30"))
CLICK_PAUSE = float(os.getenv("CLICK_PAUSE", "0.2"))
START_TIMEOUT = int(os.getenv("START_TIMEOUT", "8"))
FINISH_TIMEOUT = int(os.getenv("FINISH_TIMEOUT", "60"))
IDLE_OK_SEC = int(os.getenv("IDLE_OK_SEC", "2"))

DB_ENABLE = os.getenv("DB_ENABLE", "false").lower() == "true"
POD_COUNT = max(1, int(os.getenv("POD_COUNT", "1")))  # Validate POD_COUNT first
POD_INDEX = max(0, min(int(os.getenv("POD_INDEX", "0")), POD_COUNT - 1))



# ========== SETUP LOGGING ==========
# this creates a system to show messages about what the program is doing
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [POD-%(pod_index)s] [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()],
)





# -------------------- Optional PostgreSQL logging helpers --------------------

try:
    import psycopg2
    from psycopg2.extras import Json
except Exception:
    psycopg2 = None
    Json = None 

def db_connect():
    if not DB_ENABLE or psycopg2 is None:
        return None
    cfg = {
        "host": os.getenv("PGHOST", "localhost"),
        "port": int(os.getenv("PGPORT", "5432")),
        "user": os.getenv("PGUSER", "postgres"),
        "password": os.getenv("PGPASSWORD", ""),
        "dbname": os.getenv("PGDATABASE", "postgres"),
    }
    try:
        return psycopg2.connect(**cfg)
    except Exception as e:
        logger.warning("DB disabled (connect failed): %s", e)
        return None

def db_init(conn):
    if not conn: return
    cur = conn.cursor()
    cur.execute("""    create table if not exists runs (
      id bigserial primary key,
      run_key text unique,
      started_at timestamptz default now(),
      ended_at   timestamptz,
      total_urls int,
      total_pdfs int default 0,
      total_errors int default 0,
      avg_sec_per_url numeric
    );""")
    cur.execute("""    create table if not exists url_results (
      id bigserial primary key,
      run_id bigint references runs(id) on delete cascade,
      permit_id text,
      url text,
      pdfs_found int,
      pdfs_downloaded int,
      elapsed_sec numeric,
      errors jsonb
    );"""    )
    conn.commit()

def db_get_or_create_run(conn, run_key: str, total_urls: int):
    if not conn: return None
    cur = conn.cursor()
    cur.execute("select id from runs where run_key=%s and ended_at is null order by id desc limit 1;", (run_key,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("insert into runs (run_key, total_urls) values (%s,%s) returning id;", (run_key, total_urls))
    rid = cur.fetchone()[0]
    conn.commit()
    return rid

def db_run_start(conn, total_urls):
    if not conn: return None
    cur = conn.cursor()
    cur.execute("insert into runs (total_urls) values (%s) returning id;", (total_urls,))
    rid = cur.fetchone()[0]
    conn.commit()
    return rid

def db_insert_url_result(conn, run_id, r):
    if not conn: return
    cur = conn.cursor()
    cur.execute(      """      insert into url_results(run_id, permit_id, url, pdfs_found, pdfs_downloaded, elapsed_sec, errors)
      values (%s,%s,%s,%s,%s,%s,%s);
      """    , (run_id, r.get("permit_id"), r.get("url"), r.get("pdfs_found"), r.get("pdfs_downloaded"), r.get("elapsed_sec"), Json(r.get("errors", []))))
    conn.commit()

def db_get_processed_urls(conn, run_id: int) -> set:
    if not conn: return set()
    cur = conn.cursor()
    cur.execute("select url from url_results where run_id=%s;", (run_id,))
    return set(u for (u,) in cur.fetchall())

def db_run_finish(conn, run_id, total_pdfs, total_errors, elapsed, url_count):
    if not conn: return
    cur = conn.cursor()
    avg = (elapsed / url_count) if url_count else 0
    cur.execute("""      update runs set ended_at = now(), total_pdfs=%s, total_errors=%s, avg_sec_per_url=%s
      where id=%s;
    """, (total_pdfs, total_errors, avg, run_id))
    conn.commit()

# -------------------- Local checkpoint helpers (for resume without DB) --------------------
def _checkpoint_file(base_out: Path) -> Path:
    chk = base_out / "state" / "completed.txt"
    chk.parent.mkdir(parents=True, exist_ok=True)
    return chk

def checkpoint_load(base_out: Path) -> set:
    try:
        chk = _checkpoint_file(base_out)
        if not chk.exists():
            return set()
        with chk.open("r", encoding="utf-8") as f:
            return set(line.strip() for line in f if line.strip())
    except Exception:
        return set()

def checkpoint_append(base_out: Path, url: str) -> None:
    try:
        chk = _checkpoint_file(base_out)
        with chk.open("a", encoding="utf-8") as f:
            f.write(url + "\n")
    except Exception as e:
        logger.warning("checkpoint append failed: %s", e)
logger = logging.LoggerAdapter(
    logging.getLogger("optimized_pdf_downloader"), 
    {'pod_index': POD_INDEX}
)

# Suppress WebDriver Manager logs
#logging.getLogger('WDM').setLevel(logging.WARNING)   # Suppress WebDriver Manager INFO messages
logging.getLogger('selenium').setLevel(logging.WARNING)   # Also suppress selenium debug messages

# Regular expression to find text that ends with .pdf
PDF_NAME_RE = re.compile(r"\.pdf$", re.I)

# ========== HELPER FUNCTIONS ==========
def safe_name(s: str) -> str:
    """Clean up a filename to make it safe for saving on computer"""
    # Replace any character that isn't a letter, number, dash, dot, or space with underscore
    cleaned = re.sub(r"[^\w\-. ]", "_", s)
    # Remove extra spaces, dots, underscores from beginning and end
    cleaned = cleaned.strip(" ._")
    # If nothing left, use "file" as default name
    return cleaned or "file"

def permit_id_from_altid(url: str) -> str:
    #Extract the permit ID from the URL --- For example, if URL has altId=3001234-EX, then it returns 3001234-EX """
    # Parse the URL to get the query parameters
    parsed_url = urlparse(url)
    # Convert query string into a dictionary, make keys lowercase
    query_params = {k.lower(): v for k, v in parse_qs(parsed_url.query).items()}

    # Look for the altid parameter
    altid_values = query_params.get("altid")
    if not altid_values or not altid_values[0].strip():
        raise ValueError("URL missing altId=... parameter")
    
    # Take first value, decode any URL encoding, and clean it up
    return safe_name(unquote(altid_values[0]).strip())

#===========File monitoring Functions================

def snapshot_dir(path: Path) -> tuple:
    """Take a snapshot of what files are in a directory
    Returns: (set of filenames, total size of all files)"""
    names = set()  # Collection of unique filenames
    total_size = 0  # Total size of all files in bytes
    
    try:
        # Look at each file directly in this directory
        for file_path in path.glob("*"):
            if file_path.is_file():  # Only count actual files, not folders
                names.add(str(file_path))  # Add full path as string
                total_size += file_path.stat().st_size  # Add file size in bytes
    except:
        pass  # If there's any error, just return empty results
    
    return names, total_size

def wait_for_download_start(dirpath: Path, before_snapshot, start_timeout: int) -> bool:
    """Wait until a new file appears in the directory
    this means that dowload is started"""
    start_time = time.time()  # Remember when we started waiting
    before_names, _ = before_snapshot  # Get the list of files from before
    
    # Keep checking until timeout
    while time.time() - start_time <= start_timeout:
        # Take a new snapshot of the directory
        current_names, _ = snapshot_dir(dirpath)
        # If there are new files (takes difference between current and before)
        if current_names - before_names:
            return True   # Download started!
        
        time.sleep(0.2)  # Wait 0.2 seconds before checking again
    
    return False   # Timeout reached, no download detected

def wait_for_download_finish(dirpath: Path, finish_timeout: int, idle_ok: int):
    """Wait until downloads are completely finished
    A download is finished when:
    1. No .crdownload files (Chrome's temporary download files)
    2. File sizes stop changing (stable for idle_ok seconds)"""
    start_time = time.time()  #when we started waiting
    last_change_time = time.time() # when files last changed
    prev_names, prev_size = snapshot_dir(dirpath)  #previous state
    
    while True:
        # check oif chrome still  downloading files (has .crdownload files)
        if any(str(p).endswith(".crdownload") for p in prev_names):
            time.sleep(0.3)  # wait a bi more time
        # take a new snapshot
        names, size = snapshot_dir(dirpath)
        
        # if files are changed like newfiles or differetn sizes
        if names != prev_names or size != prev_size:
            last_change_time = time.time()  # updated when last changed 
            prev_names, prev_size = names, size  #update our records
        
        # If nothing changed for idle_ok seconds, download is probably done
        if time.time() - last_change_time >= idle_ok:
            break
        
        # If we've been waiting too long, give up
        if time.time() - start_time > finish_timeout:
            logger.warning("Download timeout after %ss", finish_timeout)
            break
        
        time.sleep(0.3)   # wait before checking again

def move_new_pdfs(src_dir: Path, dest_dir: Path, before_names: set) -> List[Path]:
    """Move any new PDF files from source directory to destination directory
    Sometimes Chrome downloads to default Downloads folder instead of our target folder"""
    moved_files = []  # List to track which files we moved
    
    # See what files are there now
    current_names, _ = snapshot_dir(src_dir)
    # Find new files (files that weren't there before)
    new_files = current_names - before_names
    
    # Look at each new file
    for file_path_str in new_files:
        file_path = Path(file_path_str)
        
        # Skip incomplete downloads
        if file_path.suffix.lower() in (".crdownload", ".tmp"):
            continue
        
        # Only move PDF files
        if file_path.suffix.lower() != ".pdf":
            continue
        
        destination = dest_dir / file_path.name  # Figure out where to move it to same filename in destination folder
        
        # If file already exists, add numbers like pdf1,pdf2,pdf3..
        counter = 1
        while destination.exists():
            destination = dest_dir / f"{destination.stem}({counter}){destination.suffix}"
            counter += 1
        
        # Try to move the file
        try:
            file_path.replace(destination)  # Move file from source to destination
            moved_files.append(destination)  # Remember we moved this file
        except:
            pass   # If moving fails, just continue with other files
    
    return moved_files

def default_downloads_folder() -> Path:
    """Get the path to the users default Downloads folder."""
    p = Path.home() / "Downloads"
    p.mkdir(parents = True, exist_ok= True)
    return p

# ========== BROWSER SETUP ==========
def build_driver() -> webdriver.Chrome:
    """Create and configure a Chrome browser for fast PDF downloading
    Sets up headless mode and optimizes for speed"""
    # Create Chrome options object to configure the browser
    opts = ChromeOptions()
    
    # Browser configuration for speed and headless operation
    opts.add_argument("--headless=new")   # Run without showing browser window
    opts.add_argument("--disable-gpu")   # Don't use graphics card (not needed for headless)
    opts.add_argument("--no-sandbox")   # Remove security restrictions
    opts.add_argument("--disable-dev-shm-usage")   # Fix memory issues in some environments
    opts.add_argument("--disable-extensions")   # Don't load browser extensions
    opts.add_argument("--disable-plugins")   # Don't load plugins
    opts.add_argument("--disable-images")   # Don't load images
    opts.add_argument("--window-size=1400,900")  # Set virtual window size

    opts.add_argument("--remote-debugging-port=9222")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-backgrounding-occluded-windows")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--disable-features=TranslateUI")
    opts.add_argument("--disable-ipc-flooding-protection")


    opts.add_argument("--memory-pressure-off")

    import tempfile
    import uuid
    
    # Create unique user data directory for each Chrome instance
    unique_id = str(uuid.uuid4())[:8]
    user_data_dir = f"/tmp/chrome_user_data_{unique_id}"
    opts.add_argument(f"--user-data-dir={user_data_dir}")
    
    # Additional flags to prevent conflicts
    opts.add_argument("--no-first-run")
    opts.add_argument("--disable-default-apps")
    opts.add_argument("--disable-sync")
    opts.add_argument("--disable-background-networking")

    if PARALLEL_DOWNLOADS >1:
        debug_port = random.randint(9200,9400)
        opts.add_argument(f"--remote-debugging-port={debug_port}")
    
    download_prefs = {
        "plugins.always_open_pdf_externally": True,   # Download PDFs instead of opening in browser
        "download.prompt_for_download": False,   # Don't ask where to save files
        "profile.default_content_settings.popups": 0,  # Block popups
        "profile.default_content_setting_values": {
            "automatic_downloads": 1,   # Allow automatic downloads
        }
    }
    
    # Apply the download preferences
    opts.add_experimental_option("prefs", download_prefs)
    # Enable network logging so we can see what files are being downloaded
    opts.set_capability("goog:loggingPrefs", {"performance": "ALL"})

     # Set up the Chrome driver service
    service = Service("/usr/local/bin/chromedriver")   # Automatically download correct driver
    driver = webdriver.Chrome(service=service, options=opts)   # Create the actual browser instance
    driver.set_page_load_timeout(PAGE_TIMEOUT)   # Set how long to wait for pages to load
    
    # Enable network monitoring (for fallback PDF detection)
    try:
        driver.execute_cdp_cmd("Network.enable", {})
    except:
        pass  # If this fails, continue anyway
    
    return driver

def set_download_dir(driver: webdriver.Chrome, directory: Path):
    """Tell Chrome where to save downloaded files"""
    directory.mkdir(parents=True, exist_ok=True)  #Make sure the directory exists
    
    try:
        # Use Chrome DevTools Protocol to set download directory
        driver.execute_cdp_cmd("Browser.setDownloadBehavior", {
            "behavior": "allow",   # Allow downloads without asking
            "downloadPath": str(directory),  # Where to save files
            "eventsEnabled": True   # Enable download events
        })
    except:
        pass

# ========== WEBPAGE INTERACTION ==========
def click_attachments_tab(driver: webdriver.Chrome) -> bool:
    """Find and click the Attachments tab on the webpage
    Tries different ways to find the tab since websites vary"""
    # List of different ways to find the Attachments tab
    selectors = [
        (By.LINK_TEXT, "Attachments"),  # Exact text match
        (By.PARTIAL_LINK_TEXT, "Attach"),  # Partial text match
        (By.XPATH, "//a[contains(text(), 'Attachments')]"),   # XPath for text containing "Attachments"
        (By.CSS_SELECTOR, "a[href*='Attach']"),  # CSS selector for links with "Attach" in URL
    ]
    # Try each selector method
    for method, selector in selectors:
        try:
            # Wait up to 3 seconds for element to be clickable
            element = WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((method, selector))
            )
            element.click()   # Click the element
            #logger.info("waiting for attachments tab to load")
            time.sleep(3)   # Short pause for tab to load
            

            return True   # Success
        except:
            continue   # This method didn't work, try next one
    
    return False  # none of the mthods worked

def find_clickable_element( span_element):
    """Given a span or element that contains PDF name, find the actual clickable element
    Sometimes the text is in a <span> but you need to click a nearby <a> or <button>"""
    # Strategy 1: Check if the element itself is clickable
    try:
        if span_element.tag_name.lower() in ['a', 'button']:  # If it's already a link or button
            return span_element
        if span_element.get_attribute('onclick') or span_element.get_attribute('href'):  # Has click handler or URL
            return span_element
    except:
        pass
    # Strategy 2: Look at parent elements (go up the HTML tree)
    current_element = span_element
    for level in range(3):  # Check up to 3 levels up
        try:
            # Go to parent element
            current_element = current_element.find_element(By.XPATH, "./..")
            # Check if parent is clickable
            if current_element.tag_name.lower() in ['a', 'button']:
                return current_element
            if current_element.get_attribute('onclick') or current_element.get_attribute('href'):
                return current_element
        except:
            break # Can't go further up
    # Strategy 3: Look for clickable elements in the same row/container
    try:
        # Find the containing row or div
        container = span_element.find_element(By.XPATH, "./ancestor::tr | ./ancestor::div")
        # Find any clickable elements in that container
        clickables = container.find_elements(By.XPATH, ".//a | .//button | .//*[@onclick]")
        if clickables:
            return clickables[0]  # Return first clickable element found
    except:
        pass
    
    return None  # Couldn't find anything clickable

def find_pdf_elements(driver: webdriver.Chrome) -> List[Tuple[int, str, Any]]:
    """Find all PDF elements on the page that we can download
    Returns list of (frame_index, pdf_name, clickable_element)"""
    found_pdfs = []  # List to store what we find
    
    # Different XPath expressions to find PDFs
    pdf_selectors = [
        "//span[contains(translate(text(),'PDF','pdf'), '.pdf')]",  # Spans containing .pdf in text
        "//a[contains(translate(@href,'PDF','pdf'), '.pdf')]",  # Links with .pdf in URL
        "//a[contains(translate(text(),'PDF','pdf'), '.pdf')]",   # Links with .pdf in text
        "//*[contains(translate(@title,'PDF','pdf'), '.pdf')]",   # Any element with .pdf in title
        "//td[contains(text(), '.pdf')]",    # any element with .pdf in text
        "//li[contains(text(), '.pdf')]"
    ]
    # Try each selector to find PDF elements
    for selector in pdf_selectors:
        try:
            elements = driver.find_elements(By.XPATH, selector)   # Find all matching elements
            
            for element in elements:
                try:
                    # Get the text from element (either text content or title attribute)
                    text = element.text.strip() or element.get_attribute('title') or ""
                    
                    # Check if text contains .pdf
                    if text and PDF_NAME_RE.search(text):
                        # Find the actual clickable element for this PDF
                        clickable = find_clickable_element( element)
                        if clickable:
                            # Add to our list: (-1 means main page, not in iframe)
                            found_pdfs.append((-1, text, clickable))
                except:
                    continue   # Skip this element if there's an error
        except:
            continue   # Skip this selector if it fails
    

    # If we didn't find PDFs on main page, check iframes (frames within the page)
    if not found_pdfs:
        try:
            # Find all iframes on the page
            frames = driver.find_elements(By.TAG_NAME, "iframe")
            # Check first 2 iframes only (to save time)
            for frame_index, frame in enumerate(frames[:2]):
                try:
                    # Switch to this iframe
                    driver.switch_to.frame(frame)
                    # Try the same selectors inside this iframe
                    for selector in pdf_selectors:
                        elements = driver.find_elements(By.XPATH, selector)
                        for element in elements:
                            try:
                                text = element.text.strip() or element.get_attribute('title') or ""
                                if text and PDF_NAME_RE.search(text):
                                    clickable = find_clickable_element( element)
                                    if clickable:
                                        # Add with iframe index
                                        found_pdfs.append((frame_index, text, clickable))
                            except:
                                continue
                except:
                    pass  # If iframe access fails, skip it
                finally:
                    # Always switch back to main page
                    driver.switch_to.default_content()
        except:
            pass
    
    return found_pdfs

def click_pdf_element(driver: webdriver.Chrome, frame_idx: int, element) -> bool:
    """Try to click a PDF element to start download
    Handles both main page and iframe elements"""
    try:
        # Make sure we're on the main page first
        driver.switch_to.default_content()

        # If element is in an iframe, switch to that iframe
        if frame_idx >= 0:
            frames = driver.find_elements(By.TAG_NAME, "iframe")
            if frame_idx < len(frames):
                driver.switch_to.frame(frames[frame_idx])

        # Scroll the element into view so it's visible
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
        # Try different ways to click 
        click_methods = [
            lambda: element.click(),
            lambda: driver.execute_script("arguments[0].click();", element),
        ]
        # Try each click method
        for click_method in click_methods:
            try:
                click_method()   
                return True   # Click succeeded
            except:
                continue   # This method failed, try next
        
        return False   # All click methods failed
    
    except:
        return False   # Something went wrong
    finally:
        # Always switch back to main page
        driver.switch_to.default_content()

# ========== NETWORK MONITORING (FALLBACK METHOD) ==========

def flush_perf_log(driver: webdriver.Chrome):
    """Clear old performance log entries
    This helps us see only new network requests after we click"""
    try:
        driver.get_log("performance")  # Getting logs clears them
    except:
        pass

def get_pdf_urls_from_logs(driver: webdriver.Chrome) -> List[str]:
    """Look through Chrome's network logs to find PDF URLs
    Sometimes clicking doesn't start a download but makes a network request"""
    pdf_urls = []
    
    try:
        # Get all performance log entries like network requests
        logs = driver.get_log("performance")
        
        for log_entry in logs:
            try:
                # Parse the log entry ,it's JSON
                message = json.loads(log_entry.get("message", "{}"))
                
                # We only care about network responses
                if message.get("message", {}).get("method") == "Network.responseReceived":
                    params = message["message"]["params"]
                    response = params.get("response", {})
                    
                    # Get URL and content type
                    url = response.get("url", "")
                    mime_type = (response.get("mimeType") or "").lower()
                    
                    # If it looks like a PDF, add to our list
                    if "pdf" in mime_type or url.lower().endswith(".pdf"):
                        pdf_urls.append(url)
            except:
                continue  # Skip entries we can't parse
    except:
        pass
    
    return pdf_urls

def download_pdf_direct(url: str, dest_path: Path, driver: webdriver.Chrome, page_url: str) -> bool:
    """Download a PDF directly using HTTP request
    This is our backup method when normal clicking doesn't work"""

    pdf_name = dest_path.name  # Extract PDF name from the destination path for logging
    
    try:
        logger.info(f"Attempting direct download: {pdf_name}")
        # Create a requests session to download the file
        session = requests.Session()
        
        # Set headers to look like a real browser
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": page_url,  # Tell server which page we came from
        })
        
        # Copy cookies from Selenium browser to requests session
        # This is important bcoz this site is asking for login details if there are no cookies
        host = urlparse(page_url).hostname or ""
        for cookie in driver.get_cookies():
            domain = (cookie.get("domain") or "").lstrip(".").lower()
            # Only copy cookies that match this website
            if host == domain or host.endswith("." + domain):
                session.cookies.set(cookie["name"], cookie["value"])
        
        # Download the file
        with session.get(url, stream=True, timeout=30) as response:
            response.raise_for_status()    # Raise error if download failed
            
            # Verify it's actually a PDF
            content_type = response.headers.get("Content-Type", "").lower()
            if "pdf" not in content_type:
                # Check the first few bytes to see if it starts with PDF signature
                peek = response.raw.read(5, decode_content=True)
                response.raw.seek(0)   # Go back to beginning
                if not peek.startswith(b"%PDF"):
                    logger.warning(f"Direct Download failed- not a PDF:{pdf_name}")
                    return False  # not a PDF file
            
            # Save the file to disk
            with open(dest_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):  # Download in 8KB chunks
                    if chunk:
                        file.write(chunk)
            logger.info(f"direct download successful:{pdf_name}")
            return True   # Download successful!
    
    except Exception as e:
        logger.warning(f"Direct download failed:{pdf_name} - Error :{str(e)}")
        return False   # download failed

# ========== MAIN PROCESSING ==========
def process_record(driver: webdriver.Chrome, page_url: str, base_out: Path) -> Dict[str, Any]:
    """Process a single permit URL - this is the main function that:
    ----Loads the webpage
    ---Finds PDF elements
    ----Downloads all PDFs"""
    start_t = time.time()
    try:
        permit_id = permit_id_from_altid(page_url)
    except Exception as e:
        parsed = urlparse(page_url)
        altid_params = parse_qs(parsed.query).get('altId', ['unknown'])
        permit_id = safe_name(altid_params[0]) if altid_params and altid_params[0] != 'unknown' else f"unknown_{int(time.time())}"
        logger.warning(f"Could not extract permit_id from {page_url}: {e}, using fallback: {permit_id}")

    # permit_id computed above
    results = {"url": page_url, "permit_id": permit_id, "pdfs_found": 0, "pdfs_downloaded": 0, "elapsed_sec": 0.0, "errors": []}

    # Track results for this URL
    results = {"url": page_url, "pdfs_downloaded": 0, "errors": []}
    
    try:
        # Step 1: Extract permit ID from URL
        # permit_id computed above
        permit_directory = base_out / permit_id # Create folder path
        permit_directory.mkdir(parents=True, exist_ok=True)  # Create folder
        
        # Step 2: Configure browser to download to this folder
        set_download_dir(driver, permit_directory)
        
        # Step 3: Load the webpage
        try:
            driver.get(page_url)
        except TimeoutException:
            results["errors"].append("Page load timeout")
            return results
        
        # Step 4: Click the attachments tab
        if not click_attachments_tab(driver):
            results["errors"].append("Could not find attachments tab")
            return results
        
        # Step 5: Find all PDF elements on the page
        pdf_elements = find_pdf_elements(driver)
        results["pdfs_found"]= len(pdf_elements)
        logger.info(f"{permit_id}: {len(pdf_elements)} PDF elements found")
        
        if not pdf_elements:
            results["errors"].append("No PDF elements found")
            return results
        
        # Get default downloads folder as backup
        default_downloads = default_downloads_folder()
        
        # Step 6: Try to download each PDF
        for frame_index, pdf_name, clickable_element in pdf_elements:
            logger.info(f"Downloading: {pdf_name}")
            
            # Clear network logs so we only see new requests
            flush_perf_log(driver)
            
            # Take snapshots of both folders before clicking
            before_target = snapshot_dir(permit_directory)
            before_default = snapshot_dir(default_downloads)
            
            # Try to click the PDF element  
            clicked = click_pdf_element(driver, frame_index, clickable_element)
            if not clicked:
                results["errors"].append(f"Could not click: {pdf_name}")
                continue
            
            time.sleep(CLICK_PAUSE)   # Short pause after clicking
            
            # Method 1: Check if file appeared in  target directory
            if wait_for_download_start(permit_directory, before_target, START_TIMEOUT):
                wait_for_download_finish(permit_directory, FINISH_TIMEOUT, IDLE_OK_SEC)
                results["pdfs_downloaded"] += 1
                logger.info(f"Downloaded: {pdf_name}")
                continue
            
            # Method 2: Check file appeared in  default downloads
            if wait_for_download_start(default_downloads, before_default, START_TIMEOUT):
                wait_for_download_finish(default_downloads, FINISH_TIMEOUT, IDLE_OK_SEC)
                moved = move_new_pdfs(default_downloads, permit_directory, before_default[0])
                if moved:
                    results["pdfs_downloaded"] += len(moved)
                    logger.info(f"Moved from default: {pdf_name}")
                    continue
            
            # Method 3: Network fallback
            time.sleep(2)
            pdf_urls = get_pdf_urls_from_logs(driver)
            
            saved_via_network = False                           # Flag to track if any PDF was successfully downloaded via network method
            for url in pdf_urls:                               # Loop through each PDF URL found in the network logs
                safe_pdf_name = safe_name(pdf_name)            # Clean the PDF filename to remove special characters that might cause problems
                if not safe_pdf_name.lower().endswith('.pdf'): # Check if filename doesn't end with .pdf extension
                    safe_pdf_name += '.pdf'                    # Add .pdf extension to the filename
    
                dest_path = permit_directory / safe_pdf_name         # Create full file path where PDF will be saved
                i = 1                                          # Counter for handling duplicate filenames
                while dest_path.exists():                      # Check if file already exists at this location
                    dest_path = permit_directory / f"{Path(safe_pdf_name).stem}({i}).pdf"  # Create new filename like "document(1).pdf"
                    i += 1                                     # Increment counter for next potential duplicate
    
                if download_pdf_direct(url, dest_path, driver, page_url):  # Try to download PDF using direct HTTP request
                    results["pdfs_downloaded"] += 1            # Increase count of successfully downloaded PDFs
                    saved_via_network = True                   # Mark that we successfully downloaded this PDF
                    logger.info("Downloaded via network: %s", pdf_name)  # Log success message
                    break                                      # Stop trying other URLs once one succeeds

            if not saved_via_network:                          # If no PDF was successfully downloaded via network method
                results["errors"].append(f"Failed to download: {pdf_name}")  # Add error message to results

    except Exception as e:                             # Catch any unexpected errors during processing
        results["errors"].append(f"Processing error: {str(e)}")     # Add error details to results
    
    # Summary logging
    try:
        # permit_id computed above
        total_found = len(pdf_elements) if 'pdf_elements' in locals() else 0
        downloaded = results["pdfs_downloaded"]
        failed = total_found - downloaded
        error_count = len(results["errors"])

        #log detailed summary for specific url
        logger.info(f"SUMMARY for {permit_id}: Found: {total_found}, Downloaded: {downloaded}, Failed: {failed}, Errors: {error_count}")
    except:
        pass
    
    try:
        results["elapsed_sec"] = round(time.time() - start_t, 3)
    except Exception:
        pass
    # -------- FINAL SWEEP: upload any PDFs left in the permit folder --------
    try:
        for p in permit_directory.glob("*.pdf"):
            rel = f"{OCI_PREFIX}/{RUN_TS}/{permit_id}/{p.name}"
            upload_to_oci(p, rel)          # uses the helper you added earlier
            try:
                p.unlink()                 # optional: reclaim disk
            except:
                pass
    except Exception as e:
        results["errors"].append(f"Upload error: {e}")

    return results              # Send back dictionary with download counts and errors

def load_urls_from_excel(xlsx_path: str, limit: int = 0) -> List[str]:  # Function to read URLs from Excel file
    if not os.path.exists(xlsx_path):
        logger.error(f"Excel file not found: {xlsx_path}")
        return []
    try:
        df = pd.read_excel(xlsx_path, header=None, usecols=[0])
    except Exception as e:
        logger.error(f"Failed to read Excel file: {e}")
        return []
    col = df.iloc[:, 0].astype(str).str.strip()   # Convert first column to strings and remove whitespace
    
    if limit > 0:                                  # If a limit number was specified, if it's 0 then select all values
        col = col.iloc[:limit]                     # Take only the first 'limit' number of rows
    
    urls = [u for u in col.tolist() if u.lower().startswith(("http://", "https://"))]  # Filter to keep only valid URLs
    
    # Deduplicate - remove duplicate URLs
    seen, out = set(), []                          # Create empty set to track seen URLs and empty list for output
    for u in urls:                                 # Loop through each URL
        if u not in seen:                          # If we haven't seen this URL before
            seen.add(u)                            # Remember this URL as seen
            out.append(u)                          # Add it to our output list
    return out                                     # Return list of unique URLs

def process_url_worker(url: str, base_out: Path) -> Dict[str, Any]:  
    """Worker function for processing a single URL    used by parallel processing"""  
    driver = None                                  # Initialize browser driver variable as None
    
    try:                                          # Try to process the URL
        driver = build_driver()              # Create a new Chrome browser instance
        return process_record(driver, url, base_out)  # Process the URL and return results
    except Exception as e:                        # If any error occurs during processing
        return {"url": url, "permit_id": None, "pdfs_found": 0, "pdfs_downloaded": 0, "elapsed_sec": 0.0, "errors": [f"Worker error: {str(e)}"]}  # Return error details
    finally:                                      # This runs whether try succeeds or fails
        if driver:                                # If we created a browser instance
            try:                                  # Try to close the browser
                driver.quit()                     # Close the browser and free up memory
            except:                               # If closing browser fails
                pass                              # Ignore the error and continue

# ========== MAIN FUNCTION ==========
def main():
    """ main function with path handling for both local and Docker"""
    
    
    # Paths come from env; create per-run output folder
    excel_path = EXCEL_PATH
    base_out = Path(OUT_DIR) / RUN_TS
    base_out.mkdir(parents=True, exist_ok=True)

    # Initialize DB (optional) and get/continue run
    conn = db_connect()
    db_init(conn)
    run_id = db_get_or_create_run(conn, RUN_KEY, TOTAL_URLS) if conn else None
    
    base_out.mkdir(parents=True, exist_ok=True)   # Create output directory if it doesn't exist
    
    urls = load_urls_from_excel(excel_path, TOTAL_URLS)  # Load URLs from Excel file
    # --- shard the urls list across pods: e.g., 50 urls / 5 pods = ~10 each ---
    n = len(urls)
    pc = max(POD_COUNT, 1)
    idx = max(min(POD_INDEX, pc - 1), 0)
    # Even slicing; first 'extra' pods get one extra item when n % pc != 0
    base  = n // pc
    extra = n % pc
    start = idx * base + min(idx, extra)
    length = base + (1 if idx < extra else 0)

    urls = urls[start:start + length]
    logger.info("Pod %d of %d: taking slice [%d:%d] (size %d)", idx, pc, start, start + length, len(urls))
    # Resume support: drop already-processed URLs
    already = db_get_processed_urls(conn, run_id) if run_id else checkpoint_load(base_out)
    if already:
        before_len = len(urls)
        urls = [u for u in urls if u and u not in already]
        logger.info('Resuming this slice: %d already done, %d remaining', before_len - len(urls), len(urls))
    logger.info("Processing %d URLs in this pod...", len(urls))  # Log how many URLs we found
    
    if not urls:                                  # If no valid URLs were found
        logger.info("No valid URLs found")        # Log that no URLs were found
        return                                    # Exit the function early
    
    start_time = time.time()                      # Record when we started processing
    total_pdfs = 0                               # Counter for total PDFs downloaded
    total_errors = 0                             # Counter for total errors encountered
    
    # Process URLs with limited parallelism - run multiple browsers at same time
    with ThreadPoolExecutor(max_workers=PARALLEL_DOWNLOADS) as executor:  # Create thread pool for parallel processing
        # Submit all jobs - send each URL to be processed in parallel
        future_to_url = {                         # Dictionary to track which job processes which URL
            executor.submit(process_url_worker, url, base_out): url   # Submit job to process each URL
            for url in urls                       # Do this for every URL in our list
        }
        
        # Process completed jobs - handle results as they finish
        for future in tqdm(as_completed(future_to_url), total=len(urls), desc="Processing URLs"):  # Progress bar for completed jobs
            result = future.result()              # Get the result from the completed job
            total_pdfs += result["pdfs_downloaded"]  # Add PDFs downloaded from this job to total
            total_errors += len(result["errors"])    # Add errors from this job to total

            # persist progress for resume
            if run_id:
                db_insert_url_result(conn, run_id, result)
            else:
                checkpoint_append(base_out, result.get('url',''))
            
            if result["errors"]:                  # If there were any errors for this URL
                logger.warning("Errors for %s: %s", result["url"], result["errors"])  # Log the errors
    
    elapsed = time.time() - start_time            # Calculate how long the entire process took
    if 'conn' in locals() and run_id:
        db_run_finish(conn, run_id, total_pdfs, total_errors, elapsed, len(urls))
        try:
            conn.close()
        except Exception:
            pass
    logger.info("="*60)                          # Log separator line
    logger.info("COMPLETED: %d URLs processed in %.1f seconds", len(urls), elapsed)  # Log completion summary
    logger.info("DOWNLOADED: %d PDFs total", total_pdfs)     # Log total PDFs downloaded
    logger.info("ERRORS: %d total errors", total_errors)    # Log total errors encountered
    logger.info("AVERAGE: %.1f seconds per URL", elapsed / len(urls) if urls else 0)  # Log average time per URL
    logger.info("All done...........")   #finishhhhh

if __name__ == "__main__":
    main()
