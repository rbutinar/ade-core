# ADE Core - Claude Code Instructions

## Project Overview

ADE Core is the open-source version of the ADE framework (v0.3.0).
A personal project by Roberto Butinar — providing the structured context that transforms AI agents from code assistants into autonomous data engineering partners.

## Repository Structure

```
ade-core/
├── ade_app/                      # Framework code
│   ├── core/                     # CatalogDB (SQLite backend)
│   ├── platforms/                # Platform parsers
│   │   ├── databricks/           # Notebook parser, I/O lineage, API extractor
│   │   ├── powerbi/              # TMDL parser (tables, measures, relationships)
│   │   └── postgresql/           # Coming soon
│   ├── mcp_server/               # MCP Server for AI agents
│   ├── streamlit_app/            # Web catalog UI (beta)
│   │   ├── Home.py               # Landing page
│   │   └── pages/                # Platform Overview, Data Catalog, Object Details
│   └── scripts/                  # CLI utilities (build_demo_catalog)
│
├── ade_data/
│   └── demo/                     # Demo environment (synthetic Acme Corp)
│       └── inputs/
│           ├── databricks/       # .py notebook files
│           └── powerbi/          # TMDL definition files
│
├── tests/                        # 117 tests
└── .mcp.json                     # Auto-start MCP server config
```

## Key Principles

1. **This is a personal project** - All commits use `roberto.butinar@gmail.com`
2. **No client data** - Only synthetic demo data
3. **Open source** - Apache 2.0 license
4. **Educational** - Powers the YouTube video series
5. **File-based** - Both platforms parse local files, no API required for core usage

## Development Guidelines

- Keep code clean and well-documented
- All documentation in English
- Demo data must be realistic but entirely synthetic
- Never reference real client names or architectures
- All parsers use stdlib only (no external dependencies beyond MCP)

## Quick Commands

```bash
# Build demo catalog
python -m ade_app.scripts.build_demo_catalog

# Run MCP server
python -m ade_app.mcp_server.server

# Run tests
pytest

# Extract Power BI from TMDL
python -m ade_app.platforms.powerbi.extractor --path <definition_dir> --db <catalog.db>

# Launch web catalog UI
streamlit run ade_app/streamlit_app/Home.py
```

## Related

- YouTube: The Autonomous Data Engineer
- Full version: Private (enterprise features)
