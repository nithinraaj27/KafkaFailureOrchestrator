import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import os

# 1. Database Connection Engine
def get_engine():
    user = os.getenv("DB_USER", "orchestrator")
    pw = os.getenv("DB_PASSWORD", "orchestrator")
    host = os.getenv("DB_HOST", "postgres")
    port = os.getenv("DB_PORT", "5432")
    db = os.getenv("DB_NAME", "orchestrator_db")
    return create_engine(f"postgresql://{user}:{pw}@{host}:{port}/{db}")

st.set_page_config(page_title="MCP Failure Orchestrator", layout="wide")
engine = get_engine()

st.title("🛡️ Kafka Self-Healing Dashboard")

# 2. Sidebar Metrics
with st.sidebar:
    st.header("📊 System Health")
    try:
        with engine.connect() as conn:
            total = pd.read_sql("SELECT COUNT(*) FROM failed_events", conn).iloc[0,0]
            resolved = pd.read_sql("SELECT COUNT(*) FROM failed_events WHERE status = 'RESOLVED'", conn).iloc[0,0]
            quarantined = pd.read_sql("SELECT COUNT(*) FROM failed_events WHERE status = 'QUARANTINED'", conn).iloc[0,0]
            
            st.metric("Total Failures", total)
            st.metric("Self-Healed ✅", resolved)
            st.metric("Quarantined 🗑️", quarantined)
    except Exception as e:
        st.error("Waiting for Database...")

# 3. Main Live Stream
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("📡 Live Event Stream")
    try:
        with engine.connect() as conn:
            df = pd.read_sql("SELECT event_id, exception_type, status, last_updated_at FROM failed_events ORDER BY last_updated_at DESC LIMIT 15", conn)
            st.dataframe(df, use_container_width=True)
    except:
        st.warning("No events found yet.")

with col2:
    st.subheader("🧠 Brain Decisions")
    try:
        with engine.connect() as conn:
            df_audit = pd.read_sql("SELECT decision, COUNT(*) as count FROM decision_audit GROUP BY decision", conn)
            if not df_audit.empty:
                st.bar_chart(df_audit.set_index('decision'))
            else:
                st.info("Waiting for first AI decision...")
    except:
        pass

# 4. Audit Trail
st.divider()
st.subheader("📝 Decision Audit Trail")
try:
    with engine.connect() as conn:
        audit_query = """
            SELECT a.event_id, a.decision, a.reason, a.created_at 
            FROM decision_audit a 
            ORDER BY a.created_at DESC LIMIT 5
        """
        df_audit_full = pd.read_sql(audit_query, conn)
        st.table(df_audit_full)
except:
    st.write("No audit logs yet.")

if st.button('🔄 Force Refresh'):
    st.rerun()