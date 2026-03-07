"""
ADE Core - Streamlit Web Interface

Lightweight metadata catalog browser powered by SQLite (CatalogDB).
"""
import streamlit as st
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ade_app.streamlit_app.config import (
    APP_TITLE, APP_ICON, PAGE_CONFIG, CATALOG_DB_PATH,
    ENVIRONMENT_DISPLAY_NAME, SUPPORTED_PLATFORMS
)
from ade_app.core.catalog import CatalogDB

st.set_page_config(**PAGE_CONFIG)

# =============================================================================
# HEADER
# =============================================================================

st.title(f"{APP_ICON} {APP_TITLE}")

st.markdown("""
## Open-Source Metadata Catalog

**ADE Core** provides structured context that transforms AI agents
from code assistants into autonomous data engineering partners.
""")

st.markdown("---")

# =============================================================================
# CATALOG STATS
# =============================================================================

if CATALOG_DB_PATH.exists():
    catalog = CatalogDB(CATALOG_DB_PATH)
    stats = catalog.get_stats()
    platforms = catalog.get_platforms()

    st.subheader("📈 Catalog Overview")

    if stats:
        cols = st.columns(len(stats))
        for i, (platform, type_counts) in enumerate(stats.items()):
            plat_info = SUPPORTED_PLATFORMS.get(platform, {"icon": "📦", "name": platform.title()})
            with cols[i]:
                st.markdown(f"### {plat_info['icon']} {plat_info['name']}")
                for obj_type, count in type_counts.items():
                    st.metric(obj_type.replace("_", " ").title(), count)
    else:
        st.info("Catalog is empty. Run `python -m ade_app.scripts.build_demo_catalog` to populate it.")

    catalog.close()
else:
    st.warning(f"No catalog.db found at `{CATALOG_DB_PATH}`")
    st.info("Run `python -m ade_app.scripts.build_demo_catalog` to create it.")

st.markdown("---")

# =============================================================================
# CAPABILITIES
# =============================================================================

st.subheader("Core Capabilities")

cap1, cap2, cap3, cap4 = st.columns(4)

with cap1:
    st.markdown("""
    #### 🔍 Discovery

    - Multi-platform extraction
    - Unified data catalog
    - Full-text search
    """)

with cap2:
    st.markdown("""
    #### 🔗 Lineage

    - Notebook I/O parsing
    - Table dependencies
    - Power BI relationships
    """)

with cap3:
    st.markdown("""
    #### 🔌 MCP Server

    - AI-native API
    - Claude / GPT ready
    - Catalog & lineage tools
    """)

with cap4:
    st.markdown("""
    #### 📁 File-Based

    - No API keys required
    - Parse local files
    - SQLite portable DB
    """)

st.markdown("---")

# =============================================================================
# ARCHITECTURE
# =============================================================================

st.subheader("🏗️ Architecture")

mermaid_code = """
flowchart LR
    subgraph src["🔌 Source Platforms"]
        direction TB
        DB["⚡ Databricks"] ~~~ PBI["📊 Power BI"]
    end

    subgraph core["🧠 ADE Core"]
        direction TB
        EXT["🔄 Extract"]
        STORE[("💾 SQLite\\nCatalog")]
        MCP["🔌 MCP Server"]
        WEB["🌐 Web UI"]
        EXT --> STORE
        STORE --> MCP
        STORE --> WEB
    end

    subgraph users["👥 Consumers"]
        direction TB
        DEV["💻 Developers"]
        AGENT["🤖 AI Agents"]
    end

    src --> EXT
    DEV -.->|browse| WEB
    AGENT -.->|query| MCP
"""

mermaid_html = f"""
<!DOCTYPE html>
<html>
<head>
    <script type="module">
        import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
        mermaid.initialize({{
            startOnLoad: true,
            theme: 'base',
            flowchart: {{ curve: 'basis', nodeSpacing: 30, rankSpacing: 50 }},
            themeVariables: {{
                primaryColor: '#e3f2fd',
                primaryBorderColor: '#1976d2',
                primaryTextColor: '#1a1a1a',
                lineColor: '#666666',
                fontSize: '13px'
            }}
        }});
    </script>
    <style>
        body {{ margin: 0; padding: 10px 0; background: transparent; }}
        .mermaid {{
            display: flex;
            justify-content: center;
            align-items: center;
            min-height: 300px;
        }}
        .mermaid svg {{ max-width: none; height: auto; }}
    </style>
</head>
<body>
    <div class="mermaid">
    {mermaid_code}
    </div>
</body>
</html>
"""

st.components.v1.html(mermaid_html, height=400, scrolling=False)

st.markdown("---")

# =============================================================================
# QUICK NAVIGATION
# =============================================================================

st.subheader("🚀 Quick Navigation")

nav1, nav2, nav3 = st.columns(3)

with nav1:
    if st.button("📊 Platform Overview", key="nav_overview", use_container_width=True):
        st.switch_page("pages/1_📊_Platform_Overview.py")

with nav2:
    if st.button("📁 Data Catalog", key="nav_catalog", use_container_width=True, type="primary"):
        st.switch_page("pages/2_📁_Data_Catalog.py")

with nav3:
    if st.button("🔍 Object Details", key="nav_details", use_container_width=True):
        st.switch_page("pages/3_🔍_Object_Details.py")

# =============================================================================
# SUPPORTED PLATFORMS
# =============================================================================

st.markdown("---")

st.subheader("🔌 Supported Platforms")

plat1, plat2 = st.columns(2)

with plat1:
    st.markdown("""
    **⚡ Databricks**
    - Notebooks (`.py` files)
    - Table I/O lineage
    - Variable resolution
    """)

with plat2:
    st.markdown("""
    **📊 Power BI**
    - TMDL definitions
    - Tables & columns
    - DAX measures & relationships
    """)

# =============================================================================
# SIDEBAR
# =============================================================================

st.sidebar.markdown(f"""
### {APP_ICON} {APP_TITLE}

**Environment:** {ENVIRONMENT_DISPLAY_NAME}

**Backend:** SQLite

---

**Quick Links**
""")

if st.sidebar.button("📁 Data Catalog", key="sb_catalog", use_container_width=True):
    st.switch_page("pages/2_📁_Data_Catalog.py")

if st.sidebar.button("🔍 Object Details", key="sb_details", use_container_width=True):
    st.switch_page("pages/3_🔍_Object_Details.py")

st.sidebar.markdown("""
---
**🔌 MCP Server**

AI-native API for LLM agents.

```
python -m ade_app.mcp_server.server
```
""")

st.markdown("---")
st.caption(f"ADE Core {ENVIRONMENT_DISPLAY_NAME} | Open Source")
