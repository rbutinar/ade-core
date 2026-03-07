"""
Platform Overview - Metadata statistics from SQLite catalog
"""
import streamlit as st
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from ade_app.streamlit_app.config import (
    CATALOG_DB_PATH, ENVIRONMENT_DISPLAY_NAME, SUPPORTED_PLATFORMS
)
from ade_app.core.catalog import CatalogDB

st.set_page_config(
    page_title=f"Platform Overview - ADE {ENVIRONMENT_DISPLAY_NAME}",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Platform Overview")
st.markdown(f"**Metadata statistics for {ENVIRONMENT_DISPLAY_NAME} environment**")
st.divider()

if not CATALOG_DB_PATH.exists():
    st.error(f"No catalog.db found at `{CATALOG_DB_PATH}`")
    st.info("Run `python -m ade_app.scripts.build_demo_catalog` to create it.")
    st.stop()

catalog = CatalogDB(CATALOG_DB_PATH)
stats = catalog.get_stats()

# =============================================================================
# PLATFORM CARDS
# =============================================================================

st.subheader("📈 Imported Metadata Summary")

if stats:
    cols = st.columns(len(stats))
    for i, (platform, type_counts) in enumerate(stats.items()):
        plat_info = SUPPORTED_PLATFORMS.get(platform, {"icon": "📦", "name": platform.title()})
        with cols[i]:
            col_title, col_link = st.columns([4, 1])
            with col_title:
                st.markdown(f"### {plat_info['icon']} {plat_info['name']}")
            with col_link:
                if st.button("🔍", key=f"explore_{platform}",
                             help=f"Explore {plat_info['name']} in Data Catalog"):
                    st.session_state['catalog_platform_filter'] = platform
                    st.switch_page("pages/2_📁_Data_Catalog.py")

            total = 0
            for obj_type, count in type_counts.items():
                st.metric(obj_type.replace("_", " ").title(), f"{count:,}")
                total += count
            st.markdown(f"**Total: {total:,}**")
else:
    st.info("No metadata found. Run extraction first.")

st.divider()

# =============================================================================
# OBJECT TYPE BREAKDOWN
# =============================================================================

st.subheader("📋 Object Type Breakdown")

for platform, type_counts in stats.items():
    plat_info = SUPPORTED_PLATFORMS.get(platform, {"icon": "📦", "name": platform.title()})
    with st.expander(f"{plat_info['icon']} {plat_info['name']} — {sum(type_counts.values())} objects"):
        import pandas as pd
        df = pd.DataFrame([
            {"Object Type": k.replace("_", " ").title(), "Count": v}
            for k, v in type_counts.items()
        ])
        st.dataframe(df, use_container_width=True, hide_index=True)

catalog.close()

st.divider()

# =============================================================================
# QUICK ACTIONS
# =============================================================================

st.subheader("⚡ Quick Actions")

act1, act2 = st.columns(2)

with act1:
    if st.button("📁 Data Catalog", use_container_width=True, type="primary"):
        st.switch_page("pages/2_📁_Data_Catalog.py")

with act2:
    if st.button("🔍 Object Details", use_container_width=True):
        st.switch_page("pages/3_🔍_Object_Details.py")

st.markdown("---")
st.caption(f"📊 Platform Overview | SQLite: {CATALOG_DB_PATH.name}")
