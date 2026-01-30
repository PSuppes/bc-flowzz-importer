import streamlit as st
import pandas as pd
import json
import os
import time

# IMPORTIEREN DER LOGIK AUS CONNECTOR.PY
from connector import BusinessCentralConnector 

QUEUE_FILE = "import_queue.json"

st.set_page_config(layout="wide", page_title="BC Import Dashboard", page_icon="🌿")

# --- CSS HACK FÜR BESSERE OPTIK ---
st.markdown("""
<style>
    .stButton>button { width: 100%; border-radius: 5px; }
    .status-box { padding: 10px; border-radius: 5px; margin-bottom: 10px; font-weight: bold; }
    .success { background-color: #d4edda; color: #155724; }
    .warning { background-color: #fff3cd; color: #856404; }
    .info { background-color: #d1ecf1; color: #0c5460; }
</style>
""", unsafe_allow_html=True)

# --- DATEN LADEN & SPEICHERN ---
def load_data():
    if os.path.exists(QUEUE_FILE):
        try:
            with open(QUEUE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                df = pd.DataFrame(data)
                # Sicherstellen, dass Status Spalte existiert
                if 'Status' not in df.columns: df['Status'] = 'READY'
                return df
        except: return pd.DataFrame()
    return pd.DataFrame()

def save_data(df):
    data = df.to_dict(orient='records')
    with open(QUEUE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

# --- HAUPTPROGRAMM ---
st.title("🌿 Flowzz Import Manager")

df = load_data()

# SIDEBAR: FILTER & ACTIONS
with st.sidebar:
    st.header("🎛️ Steuerung")
    
    # 1. Statistik
    if not df.empty:
        count_ready = len(df[df['Status'] == 'READY'])
        count_review = len(df[df['Status'] == 'REVIEW'])
        count_ignored = len(df[df['Status'] == 'IGNORED'])
        
        col_kpi1, col_kpi2 = st.columns(2)
        col_kpi1.metric("Neu", count_ready)
        col_kpi2.metric("Prüfen", count_review)
        st.caption(f"Ignoriert: {count_ignored} | Importiert: {len(df[df['Status']=='PROCESSED'])}")
    
    st.divider()

    # 2. Filter
    show_ignored = st.checkbox("🗑️ Papierkorb (Ignorierte) zeigen", value=False)
    
    filter_options = ['READY', 'REVIEW', 'DUPLICATE']
    if show_ignored:
        filter_options = ['IGNORED'] # Wenn Papierkorb an, zeige NUR Ignorierte
    
    selected_status = st.multiselect(
        "Status Filter:", 
        options=['READY', 'REVIEW', 'DUPLICATE', 'PROCESSED'],
        default=['READY', 'REVIEW'] if not show_ignored else ['IGNORED']
    )
    
    st.divider()

    # 3. Massen-Auswahl Buttons
    st.subheader("Auswahl")
    c1, c2 = st.columns(2)
    if c1.button("✅ Alle an"):
        for idx in df.index:
            st.session_state[f"chk_{idx}"] = True
        st.rerun()
        
    if c2.button("❌ Alle aus"):
        for idx in df.index:
            st.session_state[f"chk_{idx}"] = False
        st.rerun()

# FILTER ANWENDEN
if not df.empty:
    # Filterlogik
    if show_ignored:
         df_view = df[df['Status'] == 'IGNORED'].copy()
    else:
         # Zeige nur was im Multiselect gewählt ist UND nicht ignoriert/processed ist (außer explizit gewählt)
         df_view = df[df['Status'].isin(selected_status)].copy()
else:
    df_view = pd.DataFrame()


# MAIN CONTENT
if not df_view.empty:
    st.info(f"Zeige {len(df_view)} Einträge basierend auf Filter.")
    
    with st.form("main_form"):
        selected_indices = []

        # Grid anzeigen
        for index, row in df_view.iterrows():
            col1, col2, col3, col4 = st.columns([0.5, 1, 3, 2])
            
            data = row.get('ScrapedData', {})
            if not isinstance(data, dict): data = {} 

            with col1:
                # Checkbox mit Session State Logic für "Alle auswählen"
                key = f"chk_{index}"
                # Default Value Logik: Wenn READY dann standardmäßig an, sonst aus
                default_val = (row['Status'] == 'READY')
                
                # Wenn wir den Key im Session State haben (durch Buttons), nutzen wir den
                if key not in st.session_state:
                    st.session_state[key] = default_val
                
                is_checked = st.checkbox("", key=key)
                if is_checked:
                    selected_indices.append(index)
            
            with col2:
                img_url = data.get('Bild Datei URL')
                img_path = data.get('Bild Datei')    
                try:
                    if img_url: st.image(img_url, width=80)
                    elif img_path and os.path.exists(img_path): st.image(img_path, width=80)
                    else: st.text("🖼️")
                except: st.text("Error")
            
            with col3:
                # Titel mit Status-Icon
                icon = "🆕" if row['Status'] == 'READY' else "⚠️" if row['Status'] == 'REVIEW' else "🛑"
                if row['Status'] == 'IGNORED': icon = "🗑️"
                
                st.subheader(f"{icon} {row['Produktname']}")
                
                hersteller = data.get('Hersteller', 'N/A')
                sorte = data.get('Sorte', 'N/A')
                st.caption(f"🏭 {hersteller} | 🧬 {sorte}")
                
                # Warnmeldungen
                if row['Status'] == 'DUPLICATE':
                    st.markdown(f"<div class='status-box warning'>⚠️ {row['MatchInfo']}</div>", unsafe_allow_html=True)
                elif row['Status'] == 'REVIEW':
                    st.markdown(f"<div class='status-box info'>👀 {row['MatchInfo']}</div>", unsafe_allow_html=True)
            
            with col4:
                with st.expander("🔍 Details & JSON"):
                    # Kleine Tabelle für wichtigste Daten
                    st.markdown(f"""
                    | Attribut | Wert |
                    | --- | --- |
                    | **THC/CBD** | {data.get('THC')} / {data.get('CBD')} |
                    | **Bestrahlung** | {data.get('Bestrahlung')} |
                    | **Kultivar** | {data.get('Kultivar')} |
                    """)
                    st.json(data)
            
            st.divider()

        # --- ACTION BUTTONS (Sticky Bottom Style) ---
        col_submit_1, col_submit_2 = st.columns([1, 1])
        
        with col_submit_1:
            if show_ignored:
                btn_restore = st.form_submit_button("♻️ Wiederherstellen (zu 'Neu')")
            else:
                btn_import = st.form_submit_button("🚀 Ausgewählte Importieren", type="primary")
        
        with col_submit_2:
            if not show_ignored:
                btn_ignore = st.form_submit_button("🗑️ Ausgewählte Ignorieren")
            else:
                 st.write("") # Platzhalter

        # --- LOGIK ---
        
        if not show_ignored and btn_import:
            if not selected_indices:
                st.warning("Bitte wähle mindestens ein Produkt aus.")
            else:
                st.write("Verbinde mit Business Central...")
                try:
                    bc = BusinessCentralConnector()
                    bc.authenticate()
                    
                    bar = st.progress(0)
                    for i, idx in enumerate(selected_indices):
                        row = df.loc[idx]
                        st.write(f"⚙️ Verarbeite: {row['Produktname']}...")
                        
                        # Import Logic
                        success = bc.create_item_now(
                            row['ScrapedData'].get('BC_DisplayName'), 
                            row['ScrapedData'].get('Bild Datei'), 
                            row['ScrapedData']
                        )
                        
                        if success:
                            df.at[idx, 'Status'] = 'PROCESSED'
                        
                        bar.progress((i + 1) / len(selected_indices))
                    
                    save_data(df)
                    st.success("Fertig! Seite lädt neu...")
                    time.sleep(1.5)
                    st.rerun()
                except Exception as e:
                    st.error(f"Fehler: {e}")

        if not show_ignored and btn_ignore:
            if not selected_indices:
                st.warning("Nichts ausgewählt.")
            else:
                for idx in selected_indices:
                    df.at[idx, 'Status'] = 'IGNORED'
                save_data(df)
                st.success("Produkte in den Papierkorb verschoben.")
                time.sleep(1)
                st.rerun()
                
        if show_ignored and btn_restore:
            if not selected_indices:
                st.warning("Nichts ausgewählt.")
            else:
                for idx in selected_indices:
                    df.at[idx, 'Status'] = 'READY' # Zurück auf Los
                save_data(df)
                st.success("Produkte wiederhergestellt!")
                time.sleep(1)
                st.rerun()

else:
    st.success("✅ Alles sauber! Keine Einträge für den aktuellen Filter.")
    if st.button("🔄 Liste aktualisieren"):
        st.rerun()
