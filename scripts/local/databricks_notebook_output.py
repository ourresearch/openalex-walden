#!/usr/bin/env python3
"""
Extract and display notebook cell outputs from a Databricks job run.

Usage:
    python databricks_notebook_output.py <run_id> [--profile <profile>]

    # Or pipe from databricks CLI:
    databricks jobs export-run <run_id> --profile <profile> | python databricks_notebook_output.py
"""

import sys
import json
import re
import base64
import urllib.parse
import argparse
import subprocess


def extract_notebook_results(export_data: dict) -> list[dict]:
    """Extract cell results from notebook export data."""
    content = export_data.get('views', [{}])[0].get('content', '')

    match = re.search(r'__DATABRICKS_NOTEBOOK_MODEL = \'([^\']+)\'', content)
    if not match:
        return []

    model_encoded = match.group(1)
    decoded_b64 = base64.b64decode(model_encoded).decode('utf-8')
    decoded = urllib.parse.unquote(decoded_b64)
    model = json.loads(decoded)

    results = []
    for i, cmd in enumerate(model.get('commands', [])):
        cell_result = {
            'cell_num': i + 1,
            'code': cmd.get('command', ''),
            'type': cmd.get('commandType', 'unknown'),
        }

        cmd_results = cmd.get('results', {})
        if cmd_results and cmd_results.get('type') == 'listResults':
            result_list = cmd_results.get('data', [])
            if result_list and len(result_list) > 0:
                first_result = result_list[0]
                if isinstance(first_result, dict):
                    schema = first_result.get('schema', [])
                    data = first_result.get('data', [])

                    cell_result['columns'] = [c.get('name') for c in schema] if schema else []
                    cell_result['rows'] = data
                    cell_result['row_count'] = len(data)

        results.append(cell_result)

    return results


def format_table(columns: list, rows: list, max_rows: int = 10, max_col_width: int = 40) -> str:
    """Format data as a simple ASCII table."""
    if not columns or not rows:
        return "(no data)"

    # Truncate column values for display
    def truncate(val, width):
        s = str(val) if val is not None else 'NULL'
        return s[:width-3] + '...' if len(s) > width else s

    # Calculate column widths
    widths = []
    for i, col in enumerate(columns):
        col_vals = [truncate(row[i] if i < len(row) else '', max_col_width) for row in rows[:max_rows]]
        widths.append(max(len(col), max(len(v) for v in col_vals) if col_vals else 0))

    # Build table
    lines = []

    # Header
    header = ' | '.join(col.ljust(widths[i])[:widths[i]] for i, col in enumerate(columns))
    lines.append(header)
    lines.append('-' * len(header))

    # Rows
    for row in rows[:max_rows]:
        row_str = ' | '.join(
            truncate(row[i] if i < len(row) else '', max_col_width).ljust(widths[i])[:widths[i]]
            for i in range(len(columns))
        )
        lines.append(row_str)

    if len(rows) > max_rows:
        lines.append(f'... ({len(rows) - max_rows} more rows)')

    return '\n'.join(lines)


def display_results(results: list[dict], show_all: bool = False):
    """Display notebook results in a readable format."""
    for cell in results:
        code = cell.get('code', '')

        # Skip markdown cells unless they have results
        if code.startswith('%md') and 'rows' not in cell:
            continue

        # Get first meaningful line of code
        code_lines = [l for l in code.split('\n') if l.strip() and not l.startswith('%')]
        first_line = code_lines[0][:70] if code_lines else code[:70]

        if 'rows' in cell or show_all:
            print(f"\n{'='*70}")
            print(f"Cell {cell['cell_num']}: {first_line}...")
            print('='*70)

            if 'columns' in cell and 'rows' in cell:
                print(format_table(cell['columns'], cell['rows']))
            elif 'rows' not in cell:
                print("(no output)")


def main():
    parser = argparse.ArgumentParser(description='Display Databricks notebook run output')
    parser.add_argument('run_id', nargs='?', help='Run ID (or pipe export-run JSON via stdin)')
    parser.add_argument('--profile', default='dbc-ce570f73-0362', help='Databricks CLI profile')
    parser.add_argument('--all', action='store_true', help='Show all cells including those without output')
    parser.add_argument('--json', action='store_true', help='Output raw JSON instead of formatted tables')
    args = parser.parse_args()

    # Get export data
    if args.run_id:
        # Fetch via CLI
        cmd = ['databricks', 'jobs', 'export-run', args.run_id, '--profile', args.profile]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Error: {result.stderr}", file=sys.stderr)
            sys.exit(1)
        export_data = json.loads(result.stdout)
    else:
        # Read from stdin
        export_data = json.load(sys.stdin)

    # Extract and display results
    results = extract_notebook_results(export_data)

    if args.json:
        print(json.dumps(results, indent=2))
    else:
        display_results(results, show_all=args.all)
        print(f"\n{'='*70}")
        print(f"Total cells: {len(results)}, Cells with output: {sum(1 for r in results if 'rows' in r)}")


if __name__ == '__main__':
    main()
