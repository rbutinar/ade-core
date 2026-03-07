"""
Data Catalog - Search and explore all metadata objects

Unified search interface across Databricks and Power BI platforms.
Click any object to view full details.
"""
import streamlit as st
import pandas as pd
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from ade_app.streamlit_app.config import (
    CATALOG_DB_PATH, ENVIRONMENT_DISPLAY_NAME, SUPPORTED_PLATFORMS
)
from ade_app.core.catalog import CatalogDB

st.set_page_config(
    page_title=f"Data Catalog - ADE {ENVIRONMENT_DISPLAY_NAME}",
    page_icon="📁",
    layout="wide"
)

st.title("📁 Data Catalog")
st.markdown(f"**Search and explore metadata objects — {ENVIRONMENT_DISPLAY_NAME}**")

if not CATALOG_DB_PATH.exists():
    st.error(f"No catalog.db found at `{CATALOG_DB_PATH}`")
    st.stop()

catalog = CatalogDB(CATALOG_DB_PATH)

# =============================================================================
# SEARCH CONTROLS
# =============================================================================

col_search, col_platform, col_type = st.columns([3, 1, 1])

with col_search:
    search_query = st.text_input(
        "🔍 Search",
        value=st.session_state.get('catalog_search', ''),
        placeholder="Search by name, path, or description...",
        key="search_input"
    )

# Get available platforms and object types for filters
platforms = catalog.get_platforms()
stats = catalog.get_stats()
all_types = set()
for type_counts in stats.values():
    all_types.update(type_counts.keys())

# Apply platform filter from session state (e.g., from Platform Overview)
default_platform_idx = 0
if 'catalog_platform_filter' in st.session_state:
    pf = st.session_state.pop('catalog_platform_filter')
    if pf in platforms:
        default_platform_idx = platforms.index(pf) + 1  # +1 for "All"

with col_platform:
    platform_options = ["All"] + platforms
    selected_platform = st.selectbox(
        "Platform",
        platform_options,
        index=default_platform_idx,
        key="platform_filter"
    )

with col_type:
    type_options = ["All"] + sorted(all_types)
    selected_type = st.selectbox("Object Type", type_options, key="type_filter")

st.divider()

# =============================================================================
# SEARCH RESULTS
# =============================================================================

platform_filter = selected_platform if selected_platform != "All" else None
type_filter = selected_type if selected_type != "All" else None

results = catalog.search(
    query=search_query or "*",
    platform=platform_filter,
    object_type=type_filter,
    limit=100
)

if results:
    st.markdown(f"**{len(results)} results**")

    # Build display dataframe
    rows = []
    for item in results:
        plat = item.get("platform", "")
        plat_info = SUPPORTED_PLATFORMS.get(plat, {"icon": "📦", "name": plat})
        rows.append({
            "Platform": f"{plat_info['icon']} {plat_info['name']}",
            "Type": item.get("object_type", "").replace("_", " ").title(),
            "Name": item.get("name", ""),
            "Path": item.get("path", ""),
            "Description": (item.get("description", "") or "")[:120],
            "_id": item.get("id"),
            "_platform": plat,
            "_object_type": item.get("object_type", ""),
        })

    df = pd.DataFrame(rows)

    # Show results as interactive table
    st.dataframe(
        df[["Platform", "Type", "Name", "Path", "Description"]],
        use_container_width=True,
        hide_index=True,
        column_config={
            "Description": st.column_config.TextColumn(width="large"),
        }
    )

    st.divider()

    # Object selection for detail navigation
    st.subheader("🔍 View Object Details")

    object_names = [f"{r['Name']} ({r['_platform']}/{r['_object_type']})" for r in rows]
    selected_idx = st.selectbox(
        "Select an object to view details",
        range(len(object_names)),
        format_func=lambda i: object_names[i],
        key="detail_select"
    )

    if st.button("View Details →", type="primary"):
        selected = rows[selected_idx]
        st.session_state['detail_object_name'] = selected['Name']
        st.session_state['detail_platform'] = selected['_platform']
        st.session_state['detail_object_type'] = selected['_object_type']
        st.switch_page("pages/3_🔍_Object_Details.py")

else:
    if search_query:
        st.info(f"No results found for '{search_query}'")
    else:
        st.info("No objects in catalog. Run extraction first.")

catalog.close()

st.markdown("---")
st.caption(f"📁 Data Catalog | {ENVIRONMENT_DISPLAY_NAME}")
