"""
Notebook I/O Parser — extract input/output data sources from Databricks notebook source code.

Parses Python/SQL code to discover:
- Input sources: Delta tables, DBFS/mount paths, file reads
- Output targets: Delta tables, file writes

Based on the ADE Discovered Objects methodology.
All parsing uses stdlib only (re, dataclasses).
"""

import re
import logging
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


# ======================================================================
# Dataclasses
# ======================================================================

@dataclass
class DiscoveredObject:
    """A data source or target discovered from notebook code."""
    name: str                              # Table or path name
    platform: str                          # databricks, dbfs, file
    object_type: str                       # table, file, folder
    direction: str                         # input, output
    confidence: str                        # high, medium, low
    raw_reference: str                     # Original string found in code
    line_number: Optional[int] = None
    context: Optional[str] = None
    pattern_matched: Optional[str] = None
    properties: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class NotebookParseResult:
    """Result of parsing a single notebook."""
    notebook_name: str
    notebook_path: str
    language: str
    source_code: str
    inputs: list[DiscoveredObject] = field(default_factory=list)
    outputs: list[DiscoveredObject] = field(default_factory=list)


# ======================================================================
# Parser
# ======================================================================

class NotebookIOParser:
    """
    Parse Databricks notebook source code to extract I/O references.

    Detects spark.table(), .saveAsTable(), SQL FROM/JOIN/INSERT,
    file reads/writes, and resolves variable references.
    """

    # -- Input patterns (what the notebook reads) --
    INPUT_PATTERNS = {
        'spark_read_table': {
            'pattern': r'spark\.read\.table\s*\(\s*["\']([^"\']+)["\']\s*\)',
            'platform': 'databricks',
            'object_type': 'table',
            'confidence': 'high',
        },
        'spark_read_table_var': {
            'pattern': r'spark\.read\.table\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\)',
            'platform': 'databricks',
            'object_type': 'table',
            'confidence': 'medium',
            'resolve_variable': True,
        },
        'spark_table': {
            'pattern': r'spark\.table\s*\(\s*["\']([^"\']+)["\']\s*\)',
            'platform': 'databricks',
            'object_type': 'table',
            'confidence': 'high',
        },
        'spark_table_var': {
            'pattern': r'spark\.table\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\)',
            'platform': 'databricks',
            'object_type': 'table',
            'confidence': 'medium',
            'resolve_variable': True,
        },
        'sql_from_table': {
            'pattern': r'(?:FROM|JOIN)\s+([a-zA-Z_]\w*\.[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)?)',
            'platform': 'databricks',
            'object_type': 'table',
            'confidence': 'high',
            'exclude_prefixes': [
                'pyspark.', 'pandas.', 'numpy.', 'delta.', 'py4j.',
                'org.', 'com.', 'java.', 'scala.', 'io.',
            ],
        },
        'spark_read_load': {
            'pattern': r'spark\.read\.[^)]+\.load\s*\(\s*["\']([^"\']+)["\']\s*\)',
            'platform': 'dbfs',
            'object_type': 'file',
            'confidence': 'high',
        },
        'mnt_path': {
            'pattern': r'["\'](/mnt/[^"\']+)["\']',
            'platform': 'dbfs',
            'object_type': 'file',
            'confidence': 'medium',
        },
        'pd_read_csv': {
            'pattern': r'pd\.read_csv\s*\(\s*["\']([^"\']+)["\']\s*',
            'platform': 'file',
            'object_type': 'file',
            'confidence': 'high',
        },
        'pd_read_excel': {
            'pattern': r'pd\.read_excel\s*\(\s*["\']([^"\']+)["\']\s*',
            'platform': 'file',
            'object_type': 'file',
            'confidence': 'high',
        },
    }

    # -- Output patterns (what the notebook writes) --
    OUTPUT_PATTERNS = {
        'save_as_table': {
            'pattern': r'\.saveAsTable\s*\(\s*["\']([^"\']+)["\']\s*\)',
            'platform': 'databricks',
            'object_type': 'table',
            'confidence': 'high',
        },
        'save_as_table_var': {
            'pattern': r'\.saveAsTable\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\)',
            'platform': 'databricks',
            'object_type': 'table',
            'confidence': 'medium',
            'resolve_variable': True,
        },
        'insert_into': {
            'pattern': r'\.insertInto\s*\(\s*["\']([^"\']+)["\']\s*\)',
            'platform': 'databricks',
            'object_type': 'table',
            'confidence': 'high',
        },
        'insert_into_var': {
            'pattern': r'\.insertInto\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\)',
            'platform': 'databricks',
            'object_type': 'table',
            'confidence': 'medium',
            'resolve_variable': True,
        },
        'sql_insert': {
            'pattern': r'INSERT\s+(?:INTO|OVERWRITE)\s+(?:TABLE\s+)?([a-zA-Z_]\w*\.[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)?)',
            'platform': 'databricks',
            'object_type': 'table',
            'confidence': 'high',
        },
        'sql_create_table': {
            'pattern': r'CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z_]\w*\.[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)?)',
            'platform': 'databricks',
            'object_type': 'table',
            'confidence': 'high',
        },
        'write_save': {
            'pattern': r'\.write\.[^)]+\.save\s*\(\s*["\']([^"\']+)["\']\s*\)',
            'platform': 'dbfs',
            'object_type': 'file',
            'confidence': 'high',
        },
        'pd_to_csv': {
            'pattern': r'\.to_csv\s*\(\s*["\']([^"\']+)["\']\s*',
            'platform': 'file',
            'object_type': 'file',
            'confidence': 'high',
        },
        'pd_to_excel': {
            'pattern': r'\.to_excel\s*\(\s*["\']([^"\']+)["\']\s*',
            'platform': 'file',
            'object_type': 'file',
            'confidence': 'high',
        },
    }

    # Variable assignment patterns for resolution
    VARIABLE_PATTERNS = [
        r'(\w+)\s*=\s*["\']([^"\']+)["\']',        # VAR = "value"
        r'(\w+)\s*=\s*f["\']([^"\']+)["\']',        # VAR = f"value"
    ]

    def __init__(self):
        self.variables: dict[str, str] = {}

    def parse_file(self, path: str | Path) -> NotebookParseResult:
        """Parse a notebook .py file from disk."""
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Notebook not found: {path}")

        source_code = path.read_text(encoding="utf-8")
        return self.parse_source(
            source_code=source_code,
            notebook_name=path.stem,
            notebook_path=str(path),
        )

    def parse_source(self, source_code: str, notebook_name: str = "unknown",
                     notebook_path: str = "", language: str = "python") -> NotebookParseResult:
        """Parse notebook source code string."""
        self._extract_variables(source_code)

        inputs = self._find_references(source_code, self.INPUT_PATTERNS, "input")
        outputs = self._find_references(source_code, self.OUTPUT_PATTERNS, "output")

        inputs = self._deduplicate(inputs)
        outputs = self._deduplicate(outputs)

        return NotebookParseResult(
            notebook_name=notebook_name,
            notebook_path=notebook_path,
            language=language,
            source_code=source_code,
            inputs=inputs,
            outputs=outputs,
        )

    def _extract_variables(self, source_code: str):
        """Extract variable assignments for reference resolution."""
        self.variables = {}
        for pattern in self.VARIABLE_PATTERNS:
            for match in re.finditer(pattern, source_code, re.MULTILINE):
                self.variables[match.group(1)] = match.group(2)

    def _find_references(self, source_code: str, patterns: dict,
                         direction: str) -> list[DiscoveredObject]:
        """Find all references matching the given patterns."""
        found = []
        lines = source_code.split('\n')

        for pattern_name, info in patterns.items():
            regex = re.compile(info['pattern'], re.IGNORECASE | re.MULTILINE)

            for match in regex.finditer(source_code):
                raw_ref = match.group(1) if match.groups() else match.group(0)

                # Skip if in comment
                line_start = source_code.rfind('\n', 0, match.start()) + 1
                line_content = source_code[line_start:match.end()]
                if self._is_in_comment(line_content, match.start() - line_start):
                    continue

                # Skip Python module imports
                exclude = info.get('exclude_prefixes', [])
                if any(raw_ref.startswith(p) for p in exclude):
                    continue

                # Resolve variables
                resolve_var = info.get('resolve_variable', False)
                resolved = raw_ref
                confidence = info['confidence']

                if resolve_var:
                    if raw_ref in self.variables:
                        resolved = self.variables[raw_ref]
                        confidence = 'high'
                    else:
                        confidence = 'low'
                        resolved = f"${{{raw_ref}}}"

                # Adjust confidence for dynamic references
                confidence = self._assess_confidence(resolved, confidence)

                line_num = source_code[:match.start()].count('\n') + 1
                context = self._get_context(lines, line_num - 1)

                obj = DiscoveredObject(
                    name=resolved,
                    platform=info['platform'],
                    object_type=info['object_type'],
                    direction=direction,
                    confidence=confidence,
                    raw_reference=raw_ref if not resolve_var else f"{raw_ref} -> {resolved}",
                    line_number=line_num,
                    context=context,
                    pattern_matched=pattern_name,
                )
                found.append(obj)

        return found

    @staticmethod
    def _is_in_comment(line: str, position: int) -> bool:
        """Check if position in line is within a comment."""
        hash_pos = line.find('#')
        if hash_pos != -1 and hash_pos < position:
            return True
        if '"""' in line or "'''" in line:
            return True
        return False

    @staticmethod
    def _get_context(lines: list[str], line_idx: int, size: int = 2) -> str:
        """Get surrounding lines for context."""
        start = max(0, line_idx - size)
        end = min(len(lines), line_idx + size + 1)
        return '\n'.join(lines[start:end])

    @staticmethod
    def _assess_confidence(ref: str, base: str) -> str:
        """Adjust confidence based on reference content."""
        if '{' in ref or '$' in ref:
            return 'low'
        if '*' in ref or '?' in ref:
            return 'medium'
        return base

    @staticmethod
    def _deduplicate(objects: list[DiscoveredObject]) -> list[DiscoveredObject]:
        """Remove duplicates, keeping highest confidence."""
        confidence_order = {'high': 3, 'medium': 2, 'low': 1}
        seen: dict[str, DiscoveredObject] = {}

        for obj in objects:
            key = f"{obj.name}:{obj.direction}"
            if key not in seen:
                seen[key] = obj
            else:
                existing = confidence_order.get(seen[key].confidence, 0)
                new = confidence_order.get(obj.confidence, 0)
                if new > existing:
                    seen[key] = obj

        return list(seen.values())
