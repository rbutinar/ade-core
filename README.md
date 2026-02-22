# ADE Core

**ADE** extracts metadata from your data platforms and exposes it to AI agents via MCP.

## What it does

- **Extract** → Pull metadata from Databricks notebooks, jobs, and source code
- **Search** → Find any data asset across your platforms from Claude
- **Trace** → Analyze dependencies between notebooks and tables
- **Understand** → Let AI reason about your architecture without reading 100 files

## Why

In data engineering, context is scattered: SQL in views, DAX in Power BI, PySpark in notebooks, YAML in pipelines. There's no single repo to read.

ADE collects this context and makes it queryable — so AI agents can actually help.

> "The context is the product. The agent is the engine. The human is the pilot."

## Quick Start (3 steps)

```bash
# 1. Clone and install
git clone https://github.com/rbutinar/ade-core.git
cd ade-core
pip install -r requirements.txt

# 2. Test with demo data
python -m ade_app.mcp_server.server
# Server starts with pre-loaded synthetic demo data

# 3. Configure in Claude Code (see below)
```

## Use with Claude

ADE works with any Claude client that supports MCP (Model Context Protocol).

### Claude Code

Add to `~/.claude/mcp.json`:

```json
{
  "mcpServers": {
    "ade": {
      "command": "python",
      "args": ["-m", "ade_app.mcp_server.server"],
      "cwd": "/path/to/ade-core"
    }
  }
}
```

### Claude Desktop

Add to your config file:
- **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`
- **Mac**: `~/Library/Application Support/Claude/claude_desktop_config.json`

```json
{
  "mcpServers": {
    "ade": {
      "command": "python",
      "args": ["-m", "ade_app.mcp_server.server"],
      "cwd": "C:\\path\\to\\ade-core"
    }
  }
}
```

Restart Claude Desktop after saving.

### Then you can ask:

```
"What notebooks do we have in the demo environment?"
"Show me the source code of the sales aggregation notebook"
"What tables does the 01_ingest_raw_sales notebook write to?"
```

## Available MCP Tools

| Tool | Description |
|------|-------------|
| `get_ade_overview()` | What is ADE and how to use it |
| `list_environments()` | See available environments |
| `set_environment(id)` | Switch to different environment |
| `get_environment_info()` | Current environment details |
| `search_catalog(query)` | Find objects by name |
| `get_object_details(name, platform)` | Full metadata with source code |
| `get_platform_stats()` | Object counts by platform |
| `get_notebook_lineage(name)` | Analyze notebook dependencies |

## Extract Your Own Data

Extract metadata from your Databricks workspace:

```bash
python -m ade_app.platforms.databricks.extractor \
    --host https://your-workspace.azuredatabricks.net \
    --token your_databricks_token \
    --output ade_data/my_env/extractions/databricks
```

Then set the environment in Claude Code:
```
"Switch to my_env environment"
"What notebooks do I have?"
```

## Project Structure

```
ade-core/
├── ade_app/
│   ├── mcp_server/          # MCP server for AI agents
│   └── platforms/
│       └── databricks/      # Databricks extractor
├── ade_data/
│   └── demo/                # Demo environment with synthetic data
│       └── extractions/
│           └── databricks/
│               ├── notebooks.json
│               └── jobs.json
├── requirements.txt
└── README.md
```

## Supported Platforms

### ADE Core (this repo)

| Platform | Status | Features |
|----------|--------|----------|
| Databricks | ✅ Ready | Notebooks, jobs, source code extraction |
| Power BI | 🔜 Coming | Datasets, measures, DAX |
| PostgreSQL | 🔜 Coming | Tables, views, SQL definitions |

### ADE Extended (private)

Additional platforms available in the extended version:

| Platform | Features |
|----------|----------|
| Microsoft Fabric | Warehouses, lakehouses, pipelines, notebooks, semantic models |
| Talend | Jobs, components, data flows |
| Tableau | Workbooks, datasources, worksheets |
| SQL Server / SSIS | Packages, data flows, connections |
| Cloudera | Hive tables, Spark jobs |
| Synapse | Pools, procedures, views |

The extended version also includes:
- SQL Server metadata store with full lineage graph
- Cross-platform impact analysis
- AI-powered documentation generation
- Streamlit dashboard

*Interested? Contact: roberto.butinar@gmail.com*

## The Autonomous Data Engineer

This project powers **The Autonomous Data Engineer** video series on YouTube, showing real agentic workflows for data platforms.

- [YouTube Channel](https://youtube.com/@autonomous-data-engineer) *(coming soon)*
- [Episode Scripts](docs/podcast/)

## License

Apache 2.0 — See [LICENSE](LICENSE)

## Author

**Roberto Butinar** — Data Engineer & AI Automation Specialist

- [LinkedIn](https://linkedin.com/in/rbutinar)
- [GitHub](https://github.com/rbutinar)
