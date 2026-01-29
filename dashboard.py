import streamlit as st
import pandas as pd
import json
import os
import time
from main_script import BusinessCentralConnector 

# DATEI CONFIG
QUEUE_FILE = "import_queue.json"

st.set_page_config(layout="wide", page_title="BC Import Dashboard (Local)")
st.title("🌿 Flowzz Import Manager")

def load_data():
    if os.path.exists(QUEUE_FILE):
        try:
            with open(QUEUE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return pd.DataFrame(data)
        except: return pd.DataFrame()
    return pd.DataFrame()

def save_data(df):
    # Convert dataframe back to list of dicts
    data = df.to_dict(orient='records')
    with open(QUEUE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

# 1. Daten laden
df = load_data()

# FILTER: Zeige KEINE, die bereits importiert (PROCESSED) oder ignoriert (IGNORED) sind
if not df.empty:
    # Wir filtern die Ansicht, behalten aber den Index des Originals bei, um korrekt zu speichern
    df_view = df[~df['Status'].isin(['PROCESSED', 'IGNORED'])].copy()
else:
    df_view = pd.DataFrame()

if not df_view.empty:
    st.info(f"**{len(df_view)} Produkte warten auf Prüfung.**")
    
    with st.form("my_form"):
        selected_indices = []

        # Grid anzeigen
        for index, row in df_view.iterrows():
            col1, col2, col3, col4 = st.columns([0.5, 1, 3, 2])
            
            # Safe Access auf ScrapedData
            data = row.get('ScrapedData', {})
            if not isinstance(data, dict): data = {} # Fallback falls Daten korrupt

            with col1:
                # Checkbox
                is_checked = st.checkbox("", key=f"chk_{index}", value=(row['Status'] == 'READY'))
                if is_checked:
                    selected_indices.append(index)
            
            with col2:
                # Bild
                img_url = data.get('Bild Datei URL') # Prio 1: URL (für Cloud/Lokal)
                img_path = data.get('Bild Datei')    # Prio 2: Pfad
                
                # Versuch Bild anzuzeigen
                try:
                    if img_path and os.path.exists(img_path):
                        st.image(img_path, width=100)
                    elif img_url:
                        st.image(img_url, width=100)
                    else:
                        st.text("Kein Bild")
                except: st.text("Bild Fehler")
            
            with col3:
                st.subheader(row['Produktname'])
                hersteller = data.get('Hersteller', 'N/A')
                sorte = data.get('Sorte', 'N/A')
                st.caption(f"🏭 {hersteller} | 🧬 {sorte}")
                
                if row['Status'] == 'DUPLICATE':
                    st.warning(f"⚠️ {row['MatchInfo']}")
                elif row['Status'] == 'REVIEW':
                    st.info(f"👀 {row['MatchInfo']}")
            
            with col4:
                with st.expander("Details"):
                    st.json(data)
            
            st.divider()

        # --- ACTION BUTTONS ---
        col_submit_1, col_submit_2 = st.columns(2)
        
        with col_submit_1:
            btn_import = st.form_submit_button("🚀 Ausgewählte Importieren", type="primary")
        
        with col_submit_2:
            btn_ignore = st.form_submit_button("🗑️ Ausgewählte Ignorieren (Liste bereinigen)")

        # --- LOGIK ---
        
        if btn_import:
            if not selected_indices:
                st.warning("Bitte wähle mindestens ein Produkt aus.")
            else:
                st.write("Verbinde mit Business Central...")
                try:
                    bc = BusinessCentralConnector()
                    bc.authenticate()
                    
                    progress_bar = st.progress(0)
                    total_items = len(selected_indices)
                    
                    for i, idx in enumerate(selected_indices):
                        row = df.loc[idx]
                        scraped_data = row['ScrapedData']
                        st.write(f"⚙️ Erstelle: {row['Produktname']}...")
                        
                        # Pfad für Upload holen (lokal bevorzugt)
                        bild_pfad = scraped_data.get('Bild Datei')
                        
                        success = bc.create_item_now(scraped_data.get('BC_DisplayName'), bild_pfad, scraped_data)
                        
                        if success:
                            df.at[idx, 'Status'] = 'PROCESSED'
                        
                        progress_bar.progress((i + 1) / total_items)
                    
                    save_data(df)
                    st.success("Import abgeschlossen! Seite lädt neu...")
                    time.sleep(1)
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"Verbindungsfehler: {e}")

        if btn_ignore:
            if not selected_indices:
                st.warning("Bitte wähle Produkte zum Ignorieren aus.")
            else:
                # Setze Status auf IGNORED
                for idx in selected_indices:
                    df.at[idx, 'Status'] = 'IGNORED'
                
                save_data(df)
                st.success(f"{len(selected_indices)} Produkte ignoriert (werden nicht mehr angezeigt).")
                time.sleep(1)
                st.rerun()

else:
    st.success("✅ Alles erledigt! Keine offenen Aufgaben.")
    st.caption("Bereits importierte oder ignorierte Produkte sind ausgeblendet.")
    if st.button("Aktualisieren"):
        st.rerun()