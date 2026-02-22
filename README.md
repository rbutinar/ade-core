# ADE Core - Agentic Data Engineering Framework

**ADE** (Analytics Data Environment) is a lightweight framework that enables **autonomous data engineering** workflows using AI agents like Claude Code.

## What is Agentic Data Engineering?

It's not about AI writing code for you. It's about giving AI agents the **context** they need to reason about your entire data architecture — then letting them work with high autonomy while you supervise.

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

## Use with Claude Code

Add ADE to your Claude Code MCP settings (`~/.claude/mcp.json`):

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

Then in Claude Code you can:

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

| Platform | Status | Features |
|----------|--------|----------|
| Databricks | ✅ Ready | Notebooks, jobs, source code extraction |
| Power BI | 🔜 Coming | Datasets, measures, DAX |
| PostgreSQL | 🔜 Coming | Tables, views, SQL definitions |

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
