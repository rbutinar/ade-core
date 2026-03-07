"""
Object Details - Full metadata view with lineage visualization

Shows complete details for any catalog object:
- Databricks notebooks: source code + table I/O lineage
- Power BI: tables, columns, measures (DAX), relationships
"""
import streamlit as st
import pandas as pd
import json
import re
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from ade_app.streamlit_app.config import (
    CATALOG_DB_PATH, ENVIRONMENT_DISPLAY_NAME, SUPPORTED_PLATFORMS
)
from ade_app.core.catalog import CatalogDB

st.set_page_config(
    page_title=f"Object Details - ADE {ENVIRONMENT_DISPLAY_NAME}",
    page_icon="🔍",
    layout="wide"
)

st.title("🔍 Object Details")

if not CATALOG_DB_PATH.exists():
    st.error(f"No catalog.db found at `{CATALOG_DB_PATH}`")
    st.stop()

catalog = CatalogDB(CATALOG_DB_PATH)

# =============================================================================
# OBJECT SELECTION
# =============================================================================

# Check if navigated from Data Catalog
pre_name = st.session_state.pop('detail_object_name', '')
pre_platform = st.session_state.pop('detail_platform', '')
pre_type = st.session_state.pop('detail_object_type', '')

col1, col2, col3 = st.columns([3, 1, 1])

platforms = catalog.get_platforms()
stats = catalog.get_stats()
all_types = set()
for type_counts in stats.values():
    all_types.update(type_counts.keys())

with col1:
    obj_name = st.text_input("Object name", value=pre_name,
                             placeholder="Enter object name...")

with col2:
    plat_options = ["Any"] + platforms
    plat_idx = 0
    if pre_platform in platforms:
        plat_idx = platforms.index(pre_platform) + 1
    obj_platform = st.selectbox("Platform", plat_options, index=plat_idx)

with col3:
    type_options = ["Any"] + sorted(all_types)
    type_idx = 0
    if pre_type in sorted(all_types):
        type_idx = sorted(all_types).index(pre_type) + 1
    obj_type = st.selectbox("Type", type_options, index=type_idx)

st.divider()

if not obj_name:
    st.info("Enter an object name above, or navigate here from the Data Catalog.")
    catalog.close()
    st.stop()

# =============================================================================
# FETCH OBJECT
# =============================================================================

plat_filter = obj_platform if obj_platform != "Any" else None
type_filter = obj_type if obj_type != "Any" else None

obj = catalog.get_object(obj_name, plat_filter, type_filter)

if not obj:
    st.warning(f"Object '{obj_name}' not found.")
    # Suggest similar objects
    similar = catalog.search(obj_name, plat_filter, type_filter, limit=5)
    if similar:
        st.markdown("**Did you mean:**")
        for s in similar:
            st.markdown(f"- **{s['name']}** ({s['platform']}/{s['object_type']})")
    catalog.close()
    st.stop()

# =============================================================================
# OBJECT HEADER
# =============================================================================

platform = obj.get("platform", "")
object_type = obj.get("object_type", "")
plat_info = SUPPORTED_PLATFORMS.get(platform, {"icon": "📦", "name": platform})

st.markdown(f"### {plat_info['icon']} {obj['name']}")
st.markdown(f"**Platform:** {plat_info['name']} | **Type:** {object_type.replace('_', ' ').title()} | **Path:** `{obj.get('path', '')}`")

if obj.get("description"):
    st.markdown(f"> {obj['description']}")

st.divider()

# =============================================================================
# METADATA
# =============================================================================

col_meta, col_extra = st.columns(2)

with col_meta:
    st.subheader("📋 Metadata")
    meta_fields = {
        "ID": obj.get("id"),
        "Platform": platform,
        "Object Type": object_type,
        "Name": obj.get("name"),
        "Path": obj.get("path"),
        "Created": obj.get("created_at", "N/A"),
        "Updated": obj.get("updated_at", "N/A"),
        "Extracted": obj.get("extracted_at", "N/A"),
    }
    for k, v in meta_fields.items():
        if v:
            st.markdown(f"**{k}:** {v}")

with col_extra:
    # Show platform-specific metadata (from the flattened JSON blob)
    st.subheader("🔧 Properties")
    skip_keys = {"id", "platform", "object_type", "name", "path", "description",
                 "source_code", "created_at", "updated_at", "extracted_at", "parent_id"}
    extra = {k: v for k, v in obj.items() if k not in skip_keys and v}
    if extra:
        for k, v in extra.items():
            if isinstance(v, str) and len(v) > 200:
                with st.expander(f"**{k}**"):
                    st.code(v)
            else:
                st.markdown(f"**{k}:** {v}")
    else:
        st.markdown("*No additional properties*")

# =============================================================================
# CHILDREN (columns, measures for tables)
# =============================================================================

obj_id = obj.get("id")
if obj_id:
    children = catalog.get_children(obj_id)
    if children:
        st.divider()
        st.subheader(f"📦 Children ({len(children)})")

        # Group children by type
        by_type = {}
        for child in children:
            ct = child.get("object_type", "unknown")
            by_type.setdefault(ct, []).append(child)

        for child_type, items in by_type.items():
            with st.expander(f"{child_type.replace('_', ' ').title()} ({len(items)})", expanded=True):
                child_rows = []
                for c in items:
                    row = {"Name": c.get("name", ""), "Description": c.get("description", "") or ""}
                    # Add type-specific fields
                    if child_type == "column":
                        row["Data Type"] = c.get("data_type", c.get("dataType", ""))
                        row["Expression"] = c.get("expression", "") or ""
                    elif child_type == "measure":
                        row["Expression"] = c.get("expression", "") or ""
                    elif child_type == "relationship":
                        row["From"] = c.get("from_table", "") or ""
                        row["To"] = c.get("to_table", "") or ""
                    child_rows.append(row)

                df = pd.DataFrame(child_rows)
                st.dataframe(df, use_container_width=True, hide_index=True)

# =============================================================================
# SOURCE CODE (Databricks notebooks)
# =============================================================================

source_code = obj.get("source_code", "")
if source_code:
    st.divider()
    st.subheader("💻 Source Code")
    with st.expander("View source code", expanded=False):
        st.code(source_code, language="python")

# =============================================================================
# LINEAGE (Databricks notebooks)
# =============================================================================

if platform == "databricks" and object_type == "notebook" and source_code:
    st.divider()
    st.subheader("🔗 Table Lineage")

    # Extract lineage using the same logic as MCP server
    upstream = set()
    downstream = set()

    # Resolve variable assignments (schema.table pattern)
    var_map = {}
    for var_name, table_name in re.findall(
        r'(\w+)\s*=\s*["\']([a-zA-Z_]\w*\.[a-zA-Z_]\w*)["\']', source_code
    ):
        var_map[var_name] = table_name

    def is_table_name(name):
        exclude = ['pyspark', 'spark', 'delta', 'org.', 'com.', 'io.', 'java.']
        return not any(pkg in name.lower() for pkg in exclude)

    # Upstream: spark.table(), FROM, JOIN
    for match in re.findall(r'spark\.table\(["\']([^"\']+)["\']\)', source_code):
        upstream.add(match)
    for var in re.findall(r'spark\.table\((\w+)\)', source_code):
        if var in var_map:
            upstream.add(var_map[var])
    for match in re.findall(r'(?:FROM|JOIN)\s+(`?[a-zA-Z_]\w*\.[a-zA-Z_]\w*`?)', source_code, re.IGNORECASE):
        clean = match.strip('`')
        if is_table_name(clean):
            upstream.add(clean)

    # Downstream: saveAsTable(), insertInto(), INTO
    for pattern in [r'\.saveAsTable\(["\']([^"\']+)["\']\)', r'\.insertInto\(["\']([^"\']+)["\']\)']:
        for match in re.findall(pattern, source_code):
            downstream.add(match)
    for pattern in [r'\.saveAsTable\((\w+)\)', r'\.insertInto\((\w+)\)']:
        for var in re.findall(pattern, source_code):
            if var in var_map:
                downstream.add(var_map[var])
    for match in re.findall(r'INTO\s+(`?[a-zA-Z_]\w*\.[a-zA-Z_]\w*`?)', source_code, re.IGNORECASE):
        clean = match.strip('`')
        if is_table_name(clean):
            downstream.add(clean)

    if upstream or downstream:
        col_up, col_down = st.columns(2)

        with col_up:
            st.markdown("#### ⬅️ Reads From (Upstream)")
            if upstream:
                for t in sorted(upstream):
                    st.markdown(f"- `{t}`")
            else:
                st.markdown("*No upstream tables detected*")

        with col_down:
            st.markdown("#### ➡️ Writes To (Downstream)")
            if downstream:
                for t in sorted(downstream):
                    st.markdown(f"- `{t}`")
            else:
                st.markdown("*No downstream tables detected*")

        # Mermaid lineage diagram
        st.markdown("#### 📊 Lineage Graph")
        notebook_label = obj['name'].replace(' ', '_')

        mermaid_nodes = []
        mermaid_edges = []
        for i, t in enumerate(sorted(upstream)):
            node_id = f"up_{i}"
            mermaid_nodes.append(f'    {node_id}["{t}"]')
            mermaid_edges.append(f'    {node_id} --> NB')
        for i, t in enumerate(sorted(downstream)):
            node_id = f"down_{i}"
            mermaid_nodes.append(f'    {node_id}["{t}"]')
            mermaid_edges.append(f'    NB --> {node_id}')

        mermaid_src = "flowchart LR\n"
        mermaid_src += f'    NB(["📓 {notebook_label}"])\n'
        mermaid_src += "\n".join(mermaid_nodes) + "\n"
        mermaid_src += "\n".join(mermaid_edges)

        mermaid_lineage_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <script type="module">
                import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
                mermaid.initialize({{ startOnLoad: true, theme: 'base',
                    themeVariables: {{
                        primaryColor: '#e8f5e9', primaryBorderColor: '#43a047',
                        primaryTextColor: '#1a1a1a', lineColor: '#666', fontSize: '12px'
                    }}
                }});
            </script>
            <style>
                body {{ margin: 0; padding: 5px; background: transparent; }}
                .mermaid {{ display: flex; justify-content: center; min-height: 200px; }}
            </style>
        </head>
        <body><div class="mermaid">{mermaid_src}</div></body>
        </html>
        """
        st.components.v1.html(mermaid_lineage_html, height=300, scrolling=False)
    else:
        st.info("No table references found in this notebook's source code.")

# =============================================================================
# POWER BI RELATIONSHIPS (if viewing a Power BI table)
# =============================================================================

if platform == "powerbi" and object_type == "table" and obj_id:
    # Look for relationship children
    all_relationships = catalog.search("", platform="powerbi", object_type="relationship", limit=100)
    related = []
    obj_name_lower = obj['name'].lower()
    for rel in all_relationships:
        from_t = (rel.get("from_table", "") or "").lower()
        to_t = (rel.get("to_table", "") or "").lower()
        if obj_name_lower in (from_t, to_t):
            related.append(rel)

    if related:
        st.divider()
        st.subheader("🔗 Relationships")

        rel_rows = []
        for r in related:
            rel_rows.append({
                "From Table": r.get("from_table", ""),
                "From Column": r.get("from_column", ""),
                "To Table": r.get("to_table", ""),
                "To Column": r.get("to_column", ""),
                "Cardinality": r.get("cardinality", ""),
                "Cross Filter": r.get("cross_filtering", ""),
            })

        df = pd.DataFrame(rel_rows)
        st.dataframe(df, use_container_width=True, hide_index=True)

        # Mermaid ER diagram
        st.markdown("#### 📊 Relationship Graph")
        mermaid_er = "erDiagram\n"
        for r in related:
            from_t = (r.get("from_table", "") or "").replace(" ", "_")
            to_t = (r.get("to_table", "") or "").replace(" ", "_")
            card = r.get("cardinality", "")
            label = f"{r.get('from_column', '')} - {r.get('to_column', '')}"
            if "Many" in card and "One" in card:
                mermaid_er += f'    {from_t} }}o--|| {to_t} : "{label}"\n'
            else:
                mermaid_er += f'    {from_t} ||--|| {to_t} : "{label}"\n'

        er_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <script type="module">
                import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
                mermaid.initialize({{ startOnLoad: true, theme: 'base',
                    themeVariables: {{
                        primaryColor: '#fff3e0', primaryBorderColor: '#f57c00',
                        primaryTextColor: '#1a1a1a', lineColor: '#666', fontSize: '12px'
                    }}
                }});
            </script>
            <style>
                body {{ margin: 0; padding: 5px; background: transparent; }}
                .mermaid {{ display: flex; justify-content: center; min-height: 250px; }}
            </style>
        </head>
        <body><div class="mermaid">{mermaid_er}</div></body>
        </html>
        """
        st.components.v1.html(er_html, height=350, scrolling=False)

catalog.close()

st.markdown("---")
st.caption(f"🔍 Object Details | {ENVIRONMENT_DISPLAY_NAME}")
