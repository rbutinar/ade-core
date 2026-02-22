# ADE Core - Agentic Data Engineering Framework

**ADE** (Analytics Data Environment) makes the implicit context of data platforms explicit and queryable — enabling AI agents to reason about architectures that span multiple tools, teams, and technologies.

## Why Data Engineering Needs a Different Approach

In **software engineering**, AI agents can work end-to-end: the code is in a repo, dependencies are declared, environments are homogeneous. Tools like Claude Code and Cursor thrive here.

**Data engineering** is structurally different:

| Aspect | Software Engineering | Data Engineering |
|--------|---------------------|------------------|
| **Where is the logic?** | Code in a repo | Distributed: SQL in views, DAX in measures, PySpark in notebooks, YAML in pipelines |
| **Dependencies** | Explicit (package.json) | Implicit, cross-platform, often undocumented |
| **Environment** | Relatively homogeneous | Heterogeneous: Databricks, Fabric, Power BI, legacy ETL tools... |
| **Who's involved?** | Developers | Engineers, analysts, stewards, business users |

The context is more fragmented, more opaque. An AI agent can't just "read the repo" — because there is no single repo.

**ADE bridges this gap** by extracting metadata from all your platforms and exposing it as a unified, queryable knowledge graph via MCP.

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
