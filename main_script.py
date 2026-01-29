import time
import pandas as pd
import os
import requests
import re
import json
import shutil
from datetime import datetime
from PIL import Image, ImageDraw
from difflib import SequenceMatcher

# Selenium & WebDriver
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# ==========================================
# ### 1. KONFIGURATION ###
# ==========================================

# DATEI FÜR LOKALE WARTESCHLANGE
QUEUE_FILE = "import_queue.json"

# BC / Azure Zugangsdaten
TENANT_ID     = "675e2df2-6e8f-4868-a9d7-2d3d1d093907"
ENVIRONMENT   = "Sandbox_Apotheke_Stammdaten" 
CLIENT_ID     = "d7068529-769f-4e41-b47b-f4cfd75e6b67"
CLIENT_SECRET = "fSR8Q~I6jL2z5NWJGSk582e5sHF96omdvzHl.apv"

# BC Einstellungen
COMPANY_ID    = ""  
ITEM_CATEGORY = ""  
UNIT_CODE     = "GR"

# ODATA SERVICES
ODATA_ATTR_SERVICE  = "Artikelattribute_SD"        
ODATA_VAL_SERVICE   = "Artikelattributwerte_SD"    

# CUSTOM API
API_PUBLISHER = "flowzz"
API_GROUP     = "automation"
API_VERSION   = "v2.0"
API_ENTITY    = "itemAttributeMappings"

# Nummern-Logik
PREFIX        = "100." 
START_NUMMER  = 3000   

# Flowzz Einstellungen
START_URL     = "https://flowzz.com/product?sort%5Bn%5D=createdAt&sort%5Bd%5D=-1&fav=0&pagination%5Bpage%5D=1&avail=1"
ANZAHL_CHECK  = 4
MAX_ITEMS_PRO_SPALTE = 3 
BILDER_ORDNER = "Produkt_Bilder"

# ==========================================
# ### 2. DATA CLEANING & MAPPING ###
# ==========================================

def clean_string_global(text):
    if not text: return ""
    text = str(text)
    text = re.sub(r'[^\w\s,.\-\(\)%/:]', '', text) 
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# Mapping Tabelle (Wird jetzt AUCH beim Scrapen angewendet!)
VALUE_MAPPINGS = {
    "Sorte": {
        "Hybrid Indica dominant": "Indica dominant", 
        "Hybrid Sativa dominant": "Sativa dominant",
        "Indica dominant Hybrid": "Indica dominant",
        "Sativa dominant Hybrid": "Sativa dominant",
        "Sativa dominant": "Sativa dominant",
        "Indica dominant": "Indica dominant",
        "Hybrid": "Hybrid", 
        "Sativa": "Sativa", 
        "Indica": "Indica"
    },
    "Bestrahlung": {
        "Bestrahlt": "Bestrahlt", 
        "Unbestrahlt": "Unbestrahlt", 
        "Nicht bestrahlt": "Unbestrahlt"
    }
}

MANUFACTURER_CODE_MAPPING = {
    "1A Pharma": "1APHARMA", "ACA Müller Pharma": "ACA-MUELLE", "ADREXpharma": "ADREX",
    "Adven": "ADVEN", "alephSana": "ALEPHSANA", "Amp": "AMP", "Apocan": "APOCAN",
    "Apurano": "APURANO", "Aurora": "AURORA", "Avaay Medical": "AVAAY", "Avaay": "AVAAY",  
    "Avextra Pharma": "AVEXTRA PH", "Balancial GmbH": "BALANCIAL", "Bathera": "BATHERA",
    "Bavamedical": "BAVAMED", "Bavaria Weed": "BAVA-WEED", "BC Green": "BC-GREEN",
    "Beacon Medical Germany": "BEACON", "Beacon Medical": "BEACON", "Becanex GmbH": "BECANEX",
    "Bedrocan": "BEDROCAN", "California’s Medical": "CALI-MED", "Can Bava Med": "CAN BAVA M",
    "Canify": "CANIFY", "Cannaflos": "CANNAFLOS", "Cannamedical": "CANNAMEDIC",
    "Cannovum": "CANNOVUM", "Cannymed": "CANNYMED", "canymed": "CANYMED", 
    "Canopy Growth": "CANOPY", "Canopy Medical": "CANOPY", "CanPharma": "CANPHARMA",
    "Cansativa": "CANSATIVA", "Cantourage": "CANTOURAGE", "Cannabistada": "CA-STADA",
    "Stada": "CA-STADA", "CNBS Medical": "CNBS", "Collini Apotheke": "COLLINI",
    "Day3 Pharma": "DAY3", "Demecan": "DEMECAN", "Dopiopharm": "DOPIOPHARM",
    "Drapalin": "DRAPALIN", "Enua": "ENUA", "ENUA Pharma": "ENUA", "Ethypharm": "ETHYPHARM",
    "Farmako": "FARMAKO", "Felder Green": "FELDERGREE", "Fette Pharma GmbH": "FETTE-PHAR",
    "Four 20 Pharma": "FOUR20PHAR", "Four20 Pharma": "FOUR20PHAR", "420 Pharma": "FOUR20PHAR",
    "German Medical": "GERMAN-MED", "Grow Pharma": "GROW-PHARM", "Grünhorn": "GRUENHORN",
    "HAPA pharm": "HAPA-PHARM", "HerbaMedica": "HERBAMED", "Herbery": "HERBERY",
    "Herbsana": "HERBSANA", "Hexacan": "HEXACAN", "Heyday": "HEYDAY", "Ilios Santé": "ILLIOS-SAN",
    "IMC": "IMC", "IUVO Therapeutics GmbH": "IUVO", "IUVO": "IUVO", "Khiron": "KHIRON",
    "Kineo Medical GmbH": "KINEO", "LINNEO Health S.L.": "LINNEO", "Little Green Pharma": "LITTLEGREE",
    "Madrecan": "MADRECAN", "Materia": "MATERIA", "MeCann": "MECANN", "MediCann": "MEDICANN",
    "Medipharm Labs": "MEDIPHARML", "Mediprocan": "MEDIPROCAN", "MGF Via Medical": "MGF",
    "Montu Group": "MONTU", "Naxiva": "NAXIVA", "NEDCANN": "NEDCANN", "Nimbus": "NIMBUS",
    "Noc Pharma GmbH": "NOC", "Novacana": "NOVACANA", "Oxygen": "OXYGEN", "Peace Naturals": "PEACENATUR",
    "Pharmaleaves": "PHARMALEA", "Pharmcann": "PHARMCANN", "phyto-hemp": "PHYTO-HEMP",
    "PureCan": "PURECAN", "Remexian": "REMEXIAN", "Remexian Pharma": "REMEXIAN",
    "Solidmind Group GmbH": "SOLIDMIND", "SOMAÍ Pharmaceuticals": "SOMAI", "Swiss Alpino Pharma": "SWISSALPIN",
    "Therismos": "THERISMOS", "Tikun Olam Europe": "TIKUNOLAM", "Tilray": "TILRAY",
    "Top Shelf Medical": "TOPSHELF", "Vayamed": "VAYAMED", "Vertanical": "VERTANICAL",
    "Weeco": "WEECO", "WMG Pharma": "WMG"
}

CREATE_NEW_VALUES = True 

# ==========================================
# TEIL 3: BUSINESS CENTRAL CONNECTOR
# ==========================================

class BusinessCentralConnector:
    def __init__(self):
        self.base_url = f"https://api.businesscentral.dynamics.com/v2.0/{TENANT_ID}/{ENVIRONMENT}/api/v2.0"
        self.odata_root = f"https://api.businesscentral.dynamics.com/v2.0/{TENANT_ID}/{ENVIRONMENT}/ODataV4"
        self.custom_api_root = f"https://api.businesscentral.dynamics.com/v2.0/{TENANT_ID}/{ENVIRONMENT}/api/{API_PUBLISHER}/{API_GROUP}/{API_VERSION}"
        
        self.token = None
        self.company_id = COMPANY_ID
        self.company_name = "" 
        self.existing_items_cache = [] 
        self.attributes_cache = {} 

    def authenticate(self):
        print("🔑 Verbinde mit Business Central...")
        url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": "https://api.businesscentral.dynamics.com/.default"
        }
        r = requests.post(url, data=data)
        if r.status_code == 200:
            self.token = r.json().get("access_token")
            if not self.company_id:
                self._get_company_id()
            else:
                self._find_company_name()
            self._load_existing_items()
            self._load_odata_attributes() 
        else:
            raise Exception(f"Login fehlgeschlagen! ({r.text})")

    def _get_company_id(self):
        headers = {"Authorization": f"Bearer {self.token}"}
        r = requests.get(f"{self.base_url}/companies", headers=headers)
        if r.status_code == 200:
            val = r.json().get('value', [])
            if val: 
                target = next((c for c in val if "Masterstammdaten" in c['displayName']), val[0])
                self.company_id = target['id']
                self.company_name = target['name'] 
                print(f"🏢 Verbunden mit Firma: '{target['displayName']}'")
            else: 
                raise Exception("Keine Firma gefunden!")

    def _find_company_name(self):
        headers = {"Authorization": f"Bearer {self.token}"}
        r = requests.get(f"{self.base_url}/companies({self.company_id})", headers=headers)
        if r.status_code == 200:
            self.company_name = r.json().get('name')

    def _load_existing_items(self):
        print("⏳ Lade Artikelstamm...")
        headers = {"Authorization": f"Bearer {self.token}"}
        url = f"{self.base_url}/companies({self.company_id})/items?$select=id,number,displayName"
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            self.existing_items_cache = r.json().get('value', [])
            print(f"✅ {len(self.existing_items_cache)} Artikel im Cache.")

    def _load_odata_attributes(self):
        print("⏳ Lade Attribute und Werte über OData...")
        headers = {"Authorization": f"Bearer {self.token}"}
        comp_part = f"Company('{self.company_name}')"
        
        url_attr = f"{self.odata_root}/{comp_part}/{ODATA_ATTR_SERVICE}"
        r = requests.get(url_attr, headers=headers)
        if r.status_code == 200:
            for a in r.json().get('value', []):
                k_id = next((k for k in ['ID','id'] if k in a), 'ID')
                k_name = next((k for k in ['Name','name'] if k in a), 'Name')
                self.attributes_cache[a[k_name]] = {'id': a[k_id], 'values': {}}
        
        url_vals = f"{self.odata_root}/{comp_part}/{ODATA_VAL_SERVICE}"
        r_vals = requests.get(url_vals, headers=headers)
        if r_vals.status_code == 200:
            vals = r_vals.json().get('value', [])
            if vals:
                vf = vals[0]
                k_aid = next((k for k in ['Attribute_ID','AttributeID'] if k in vf), 'Attribute_ID')
                k_vid = next((k for k in ['ID','id'] if k in vf), 'ID')
                k_val = next((k for k in ['Value','value','Name'] if k in vf), 'Value')
                
                for v in vals:
                    try:
                        p_aid = v.get(k_aid)
                        p_val = str(v.get(k_val))
                        p_vid = v.get(k_vid)
                        for attr_data in self.attributes_cache.values():
                            if attr_data['id'] == p_aid:
                                attr_data['values'][p_val] = p_vid
                    except: pass
        print(f"✅ Attribute geladen.")

    def _ensure_value_exists(self, attr_name, attr_id, raw_val):
        clean_val = clean_string_global(raw_val)
        if not clean_val: return None

        # Putz-Logik (Dupliziert für Import-Sicherheit)
        def normalize_for_match(s):
            return re.sub(r'[\W_]+', '', s.lower())

        if attr_name in VALUE_MAPPINGS:
            val_norm = normalize_for_match(clean_val)
            for map_key, map_target in VALUE_MAPPINGS[attr_name].items():
                if normalize_for_match(map_key) == val_norm:
                    clean_val = map_target
                    break 
        
        cached_values = self.attributes_cache[attr_name]['values']
        search_key_strict = clean_val.lower().strip()

        if attr_name == "Hersteller":
            ignore_words = ["gmbh", "ag", "limited", "ltd", "pharma", "pharm", "medical", "cannabis", "deutschland", "germany", "europe", "healthcare", "therapeutics", "labs"]
            def normalize_brand(name):
                n = name.lower()
                for word in ignore_words: n = n.replace(word, "")
                return re.sub(r'[^a-z0-9]', '', n)

            input_core = normalize_brand(clean_val)
            for existing_name, existing_id in cached_values.items():
                bc_core = normalize_brand(existing_name)
                is_match = False
                if input_core == bc_core: is_match = True
                elif len(input_core) > 2 and len(bc_core) > 2:
                     if input_core in bc_core or bc_core in input_core: is_match = True
                if is_match: return existing_id
        else:
            for existing_name, existing_id in cached_values.items():
                if existing_name.lower().strip() == search_key_strict:
                    return existing_id

        ALLOWED_TO_CREATE = ["Produktname", "Kultivar", "URL", "Hersteller"] 
        if attr_name not in ALLOWED_TO_CREATE:
            print(f"      ⚠️ STRICT MODE: Wert '{clean_val}' existiert nicht für '{attr_name}' (Skip).")
            return None 

        if CREATE_NEW_VALUES:
            print(f"      🆕 Erstelle neuen Wert: '{clean_val}' für '{attr_name}'...")
            headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
            comp_part = f"Company('{self.company_name}')"
            url = f"{self.odata_root}/{comp_part}/{ODATA_VAL_SERVICE}"
            payload = { "Attribute_ID": attr_id, "Value": clean_val }
            r = requests.post(url, headers=headers, json=payload)
            if r.status_code in [200, 201]:
                data = r.json()
                new_id = data.get('ID') or data.get('id')
                if new_id:
                    self.attributes_cache[attr_name]['values'][clean_val] = new_id
                    return new_id
        return None

    def find_next_number(self):
        max_val = START_NUMMER
        for item in self.existing_items_cache:
            nr = item.get('number', '')
            if nr.startswith(PREFIX):
                try:
                    val = int(nr.split(PREFIX)[1])
                    if val > max_val: max_val = val
                except: pass
        return f"{PREFIX}{max_val + 1}"

    def _calculate_token_sort_ratio(self, str1, str2):
        if not str1 or not str2: return 0.0
        def clean_token(s):
            s = re.sub(r'[^\w\s/]', '', str(s).lower())
            return s
        tokens1 = sorted(clean_token(str1).split())
        tokens2 = sorted(clean_token(str2).split())
        t1_str = " ".join(tokens1)
        t2_str = " ".join(tokens2)
        return SequenceMatcher(None, t1_str, t2_str).ratio()

    def get_match_info(self, new_name):
        best_score = 0.0
        best_name = "Kein Vergleichswert"
        best_no = None
        if not self.existing_items_cache: return best_name, 0.0, None

        ignore_words = ["cannabis", "flos", "blüten", "extract", "gmbh", "kultivar", "strain"]
        def remove_fillers(text):
            t = text.lower()
            for w in ignore_words: t = t.replace(w, "")
            return t

        clean_new_name = remove_fillers(new_name)
        for item in self.existing_items_cache:
            existing_name = item['displayName']
            clean_existing_name = remove_fillers(existing_name)
            
            score_normal = SequenceMatcher(None, clean_new_name, clean_existing_name).ratio()
            score_token = self._calculate_token_sort_ratio(new_name, existing_name)
            final_score = max(score_normal, score_token)
            
            nums_new = re.findall(r'\d+/\d+', new_name)
            nums_old = re.findall(r'\d+/\d+', existing_name)
            if nums_new and nums_old:
                if nums_new[0] != nums_old[0]:
                    final_score = final_score - 0.5 
            
            if final_score > best_score:
                best_score = final_score
                best_name = existing_name
                best_no = item['number']
        return best_name, best_score, best_no

    def create_item_now(self, display_name, bild_pfad, scraped_data):
        headers = { "Authorization": f"Bearer {self.token}", "Content-Type": "application/json" }
        next_no = self.find_next_number()

        raw_hersteller = scraped_data.get('Hersteller', '').strip()
        m_code = None
        m_code = MANUFACTURER_CODE_MAPPING.get(raw_hersteller)
        if not m_code:
            for k, v in MANUFACTURER_CODE_MAPPING.items():
                if k.lower() == raw_hersteller.lower():
                    m_code = v
                    break
        if not m_code:
             for k, v in MANUFACTURER_CODE_MAPPING.items():
                if k.lower() in raw_hersteller.lower():
                    m_code = v
                    break

        payload = {
            "number": next_no,
            "displayName": display_name[:100], 
            "baseUnitOfMeasureCode": UNIT_CODE,
            "blocked": False
        }
        if m_code: payload["manufacturerCode"] = m_code
        if ITEM_CATEGORY: payload["itemCategoryCode"] = ITEM_CATEGORY

        base_api_root = self.base_url.rsplit("/v2.0", 1)[0] 
        custom_url = f"{base_api_root}/{API_PUBLISHER}/{API_GROUP}/{API_VERSION}/companies({self.company_id})/items"

        print(f"🚀 Sende Request an BC: {display_name}")
        r = requests.post(custom_url, headers=headers, json=payload)
        
        if r.status_code == 201:
            item = r.json()
            item_id = item.get('id') or item.get('systemId')
            self.existing_items_cache.append(item) 
            print(f"   ✅ Erstellt: {item['number']} - {item['displayName']}")
            
            if bild_pfad and os.path.exists(bild_pfad) and item_id:
                time.sleep(1) 
                self._upload_image(item_id, bild_pfad)
            
            self._process_and_link_attributes(item['number'], scraped_data)
            return True
        else:
            print(f"   ❌ Fehler beim Erstellen: {r.text}")
            return False

    def _upload_image(self, item_id, file_path):
        url = f"{self.base_url}/companies({self.company_id})/items({item_id})/picture/pictureContent"
        headers = { "Authorization": f"Bearer {self.token}", "Content-Type": "application/octet-stream", "If-Match": "*" }
        try:
            with open(file_path, "rb") as f: requests.put(url, headers=headers, data=f.read())
            print("      📸 Bild hochgeladen.")
        except: pass

    def _link_attribute_to_item(self, item_no, attr_id, val_id):
        url = f"{self.custom_api_root}/companies({self.company_id})/{API_ENTITY}"
        headers = { "Authorization": f"Bearer {self.token}", "Content-Type": "application/json" }
        payload = { "itemNo": item_no, "attributeId": attr_id, "valueId": val_id }
        r = requests.post(url, headers=headers, json=payload)
        if r.status_code in [200, 201] or "already exists" in r.text:
            return True
        return False

    def _process_and_link_attributes(self, item_no, data):
        static_mapping = {
            'THC': 'THC in Prozent', 'CBD': 'CBD in Prozent', 'Hersteller': 'Hersteller',
            'Herkunft': 'Herkunftsland', 'Sorte': 'Sorte', 'Bestrahlung': 'Bestrahlung',
            'Kultivar': 'Kultivar', 'Produktgruppe': 'Produktgruppen', 'URL': 'URL', 'Produktname': 'Produktname'
        }
        list_mapping = {
            'Aroma': 'Aroma', 'Terpen': 'Terpen', 
            'Med. Wirkung': 'Medizinische Wirkung', 'Kategorie Effekt': 'Kategorie Effekt' 
        }
        
        print(f"      🔗 Verknüpfe Attribute...")
        for scraper_key, bc_name in static_mapping.items():
            raw_val = data.get(scraper_key, "").strip()
            if not raw_val: continue
            
            if bc_name in self.attributes_cache:
                attr_id = self.attributes_cache[bc_name]['id']
                val_id = self._ensure_value_exists(bc_name, attr_id, raw_val)
                if val_id: self._link_attribute_to_item(item_no, attr_id, val_id)

        for scraper_base, bc_name in list_mapping.items():
            if bc_name not in self.attributes_cache: continue 
            attr_id = self.attributes_cache[bc_name]['id']
            for i in range(1, MAX_ITEMS_PRO_SPALTE + 1):
                key = f"{scraper_base} {i}" 
                raw_val = data.get(key, "").strip()
                if raw_val:
                    val_id = self._ensure_value_exists(bc_name, attr_id, raw_val)
                    if val_id: self._link_attribute_to_item(item_no, attr_id, val_id)

# ==========================================
# TEIL 4: HELPER & SCRAPING
# ==========================================

def get_driver():
    chrome_options = Options()
    # chrome_options.add_argument("--headless=new") 
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.maximize_window()
    return driver

def clean_text(text):
    if not text: return ""
    t = text.strip()
    if len(t) > 50: return "" 
    if t in ["Wirkung", "Geschmack", "Terpene", "Effekte", "Medizinische Wirkung bei", "Alle anzeigen"]: return ""
    return t

def clean_number_int(text):
    if not text: return ""
    clean = re.sub(r'[^\d,.]', '', text).replace(',', '.')
    try:
        val = float(clean)
        return str(int(round(val)))
    except: return ""

def remove_watermark_rectangle(file_path):
    try:
        with Image.open(file_path) as img:
            img = img.convert("RGB")
            width, height = img.size
            draw = ImageDraw.Draw(img)
            rect_width = 380
            rect_height = 160
            coords = [width - rect_width, height - rect_height, width, height]
            draw.rectangle(coords, fill=(255, 255, 255), outline=None)
            img.save(file_path, quality=95)
    except: pass

def sanitize_filename(name):
    return re.sub(r'[\\/*?:"<>|]', "", name).strip()

def download_image(url, product_name):
    if not url: return None
    if not os.path.exists(BILDER_ORDNER): os.makedirs(BILDER_ORDNER, exist_ok=True)
    clean_name = sanitize_filename(product_name)
    filename = f"{clean_name}.jpg"
    file_path = os.path.join(BILDER_ORDNER, filename)
    if os.path.exists(file_path): return file_path
    try:
        if url.startswith("/"): url = f"https://flowzz.com{url}"
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, stream=True)
        if response.status_code == 200:
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(1024): f.write(chunk)
            remove_watermark_rectangle(file_path) 
            return file_path
    except: pass
    return None

def hole_listen_safe(driver, keywords):
    ergebnis_liste = []
    if isinstance(keywords, str): keywords = [keywords]
    for kw in keywords:
        try:
            xpath_header = f"//*[self::h2 or self::h3 or self::h4 or self::h5 or self::p or self::div][contains(text(), '{kw}')]"
            headers = driver.find_elements(By.XPATH, xpath_header)
            for header in headers:
                try:
                    container = header.find_element(By.XPATH, "following-sibling::div[1]")
                    items = container.find_elements(By.XPATH, ".//*[contains(@class, 'MuiTypography-body1') or contains(@class, 'MuiChip-label')]")
                    for item in items:
                        t = clean_text(item.text)
                        if t and t not in ergebnis_liste and t not in keywords and len(t) < 40:
                            ergebnis_liste.append(t)
                    if ergebnis_liste: break 
                except: continue
        except: continue
    return list(dict.fromkeys(ergebnis_liste)) 

def hole_hersteller(driver):
    try:
        label = driver.find_element(By.XPATH, "//*[contains(text(), 'Im Sortiment von')]")
        return label.find_element(By.XPATH, "following::div[1]//p").text.strip()
    except: return ""

def hole_thc_cbd(driver, typ):
    try:
        label = driver.find_element(By.XPATH, f"//p[text()='{typ}']")
        val = label.find_element(By.XPATH, "following::p[1]").text.strip()
        return clean_number_int(val)
    except: return ""

def hole_herkunftsland(driver):
    try:
        img = driver.find_element(By.XPATH, "//img[contains(@src, 'flagcdn')]")
        return img.find_element(By.XPATH, "./..").text.strip()
    except: return ""

def hole_bestrahlung(driver):
    try:
        if driver.find_elements(By.XPATH, "//*[contains(@data-testid, 'NotIrradiated')]"): return "Unbestrahlt"
        if driver.find_elements(By.XPATH, "//*[contains(@data-testid, 'Irradiated')]"): return "Bestrahlt"
        return ""
    except: return ""

def hole_sorte_genetik(driver):
    try:
        chips = driver.find_elements(By.CLASS_NAME, "MuiChip-label")
        for chip in chips:
            t = chip.text.strip()
            if any(x in t for x in ["Hybrid", "Indica", "Sativa"]): return t
        return ""
    except: return ""

def hole_kultivar(driver):
    try:
        header = driver.find_element(By.XPATH, "//h3[contains(text(), 'Über diesen Strain')]")
        link = header.find_element(By.XPATH, "following::a[contains(@href, '/strain/')][1]")
        return link.text.strip()
    except: return ""

def hole_bild_url(driver):
    try:
        imgs = driver.find_elements(By.XPATH, "//div[contains(@class, 'MuiGrid-item')]//img")
        for img in imgs:
            src = img.get_attribute("src")
            if src and ("next/image" in src or "assets.flowzz" in src): return src
        return ""
    except: return ""

def scrape_full_details(driver, url):
    driver.get(url)
    time.sleep(3)
    daten = {'URL': url}
    try: daten['Produktname'] = driver.find_element(By.TAG_NAME, "h1").text.strip()
    except: daten['Produktname'] = "Unbekannt"
    try:
        breads = driver.find_elements(By.XPATH, "//li[contains(@class, 'MuiBreadcrumbs-li')]//p")
        daten['BC_DisplayName'] = breads[-1].text.strip() if breads else daten['Produktname']
    except: daten['BC_DisplayName'] = daten['Produktname']

    daten['Hersteller']  = hole_hersteller(driver)
    daten['Herkunft']    = hole_herkunftsland(driver)
    daten['Bestrahlung'] = hole_bestrahlung(driver)
    daten['THC']         = hole_thc_cbd(driver, "THC") 
    daten['CBD']         = hole_thc_cbd(driver, "CBD") 
    daten['Sorte']       = hole_sorte_genetik(driver)
    daten['Kultivar']    = hole_kultivar(driver)
    daten['Produktgruppe'] = "Blüten"
    
    img_url = hole_bild_url(driver)
    daten['Bild Datei'] = download_image(img_url, daten['Produktname'])
    daten['Bild Datei URL'] = img_url 
    
    effekte = hole_listen_safe(driver, ["Effekte", "Kategorie Effekt", "Wirkung"])
    for i in range(MAX_ITEMS_PRO_SPALTE): 
        val = effekte[i] if i < len(effekte) else ""
        daten[f'Kategorie Effekt {i+1}'] = val
    aromen = hole_listen_safe(driver, ["Aroma", "Geschmack"])
    for i in range(MAX_ITEMS_PRO_SPALTE):
        daten[f'Aroma {i+1}'] = aromen[i] if i < len(aromen) else ""
    terpene = hole_listen_safe(driver, "Terpene")
    for i in range(MAX_ITEMS_PRO_SPALTE):
        daten[f'Terpen {i+1}'] = terpene[i] if i < len(terpene) else ""
    med_wirk = hole_listen_safe(driver, ["Medizinische Wirkung", "Medizinische Wirkung bei"])
    for i in range(MAX_ITEMS_PRO_SPALTE):
        daten[f'Med. Wirkung {i+1}'] = med_wirk[i] if i < len(med_wirk) else ""

    return daten

def hole_links_von_uebersicht(driver):
    print("🔎 Suche im Grid...")
    found = []
    xpath = "//div[contains(@class, 'MuiGrid2-grid-xs-6')]//div[contains(@class, 'MuiCard-root')]"
    karten = driver.find_elements(By.XPATH, xpath)
    if not karten:
         xpath = "//div[contains(@class, 'MuiCard-root') and .//span[text()='Neu']]"
         karten = driver.find_elements(By.XPATH, xpath)
    print(f"🃏 {len(karten)} Karten gefunden.")
    for karte in karten:
        try:
            link = karte.find_element(By.TAG_NAME, "a").get_attribute("href")
            if link and link not in found:
                found.append(link)
            if len(found) >= ANZAHL_CHECK: break 
        except: continue
    return found

# ==========================================
# QUEUE FUNKTIONEN (PRE-CLEANING HIER!)
# ==========================================

def load_local_queue():
    if os.path.exists(QUEUE_FILE):
        try:
            with open(QUEUE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except: return []
    return []

def save_to_local_queue(queue):
    with open(QUEUE_FILE, "w", encoding="utf-8") as f:
        json.dump(queue, f, indent=4, ensure_ascii=False)

def apply_pre_cleaning(details):
    """
    Wendet Mapping-Regeln SOFORT an, damit das Dashboard saubere Daten zeigt.
    """
    def normalize_for_match(s):
        return re.sub(r'[\W_]+', '', s.lower())

    for key, mappings in VALUE_MAPPINGS.items():
        if key in details:
            raw_val = details[key]
            raw_norm = normalize_for_match(raw_val)
            # Suche im Mapping
            for map_key, map_target in mappings.items():
                if normalize_for_match(map_key) == raw_norm:
                    details[key] = map_target # Überschreibe mit sauberem Wert
                    break
    return details

# ==========================================
# HAUPTPROGRAMM (Scraper für die Nacht)
# ==========================================

def run_nightly_scraper():
    print("🚀 START: Flowzz Nightly Scraper -> LOCAL JSON")
    
    try:
        bc = BusinessCentralConnector()
        bc.authenticate()
    except Exception as e:
        print(f"❌ ABBRUCH: Konnte BC nicht erreichen: {e}")
        return

    current_queue = load_local_queue()
    print(f"📂 Lokale Queue geladen: {len(current_queue)} Einträge.")
    
    processed_or_ignored = [
        item['Produktname'] for item in current_queue 
        if item.get('Status') in ['PROCESSED', 'IGNORED']
    ]

    driver = get_driver()
    
    try:
        print(f"🌍 Öffne URL: {START_URL}")
        driver.get(START_URL)
        time.sleep(5)
        links = hole_links_von_uebersicht(driver)

        for link in links:
            print(f"\nScanning: {link}")
            details = scrape_full_details(driver, link)
            
            if not details['Produktname'] or details['Produktname'] == "Unbekannt": 
                continue

            if details['Produktname'] in processed_or_ignored:
                print(f"⏭️ Übersprungen (Bereits erledigt/ignoriert): {details['Produktname']}")
                continue

            # --- HIER PASSIERT DAS PRE-CLEANING FÜR DIE QUEUE ---
            details = apply_pre_cleaning(details)
            # ----------------------------------------------------

            bc_name_check = details['BC_DisplayName']
            match_name, score, match_no = bc.get_match_info(bc_name_check)
            
            status = "READY"
            info_text = "Neu"
            
            if score > 0.98:
                status = "DUPLICATE"
                info_text = f"Gefunden: {match_name} ({match_no})"
            elif score > 0.85:
                status = "REVIEW"
                info_text = f"Ähnlich: {match_name} ({int(score*100)}%)"

            exists_in_queue = False
            for q_item in current_queue:
                if q_item['Produktname'] == details['Produktname']:
                    exists_in_queue = True
                    if q_item.get('Status') == 'READY': # Nur updaten wenn noch nicht processed
                        q_item['Status'] = status
                        q_item['MatchInfo'] = info_text
                        q_item['ScrapedData'] = details 
                        print(f"♻️ Bereits in Queue (aktualisiert).")
                    break
            
            if not exists_in_queue:
                new_entry = {
                    "Produktname": details['Produktname'],
                    "Status": status,
                    "MatchInfo": info_text,
                    "ScrapedData": details,
                    "Timestamp": datetime.now().isoformat()
                }
                current_queue.append(new_entry)
                print(f"💾 Zur Queue hinzugefügt: {details['Produktname']}")

            save_to_local_queue(current_queue)

    except Exception as e:
        print(f"❌ Fehler im Loop: {e}")
    finally:
        driver.quit()
        print("😴 Scraper beendet.")

if __name__ == "__main__":
    run_nightly_scraper()