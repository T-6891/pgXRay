#!/usr/bin/env python3

"""
pgXRay
-----------------------------------------------------------------

Version: 3.1.0

Purpose
    Advanced PostgreSQL database audit:
      - Structure (tables, columns, indexes, constraints)
      - Sample data (up to SAMPLE_LIMIT rows from each table)
      - Full function and trigger definitions
      - ER diagram (DOT and PNG) with HTML tables in nodes
        and XLabel edge annotations

Usage
    python pgXRay.py --conn "postgresql://user:pass@host/db"
    See README or -h/--help option for detailed parameters.

Output
    schema_ddl.sql         - DDL of all objects
    sample_<table>.csv     - data samples
    er_diagram.dot/png     - ER diagram
    audit_report.md        - comprehensive report

Requirements
    Python 3.9+
    psycopg2-binary >= 2.9
    graphviz >= 0.20

Author
    Vladimir Smelnitskiy  <master@t-brain.ru>

Repository
    https://github.com/T-6891/pgXRay

License
    Creative Commons Attribution 4.0 International (CC BY 4.0)
    https://creativecommons.org/licenses/by/4.0/
"""

# Configuration
SAMPLE_LIMIT = 10                  # rows to sample
DOT_FILE = 'er_diagram.dot'        # DOT filename
PNG_FILE = 'er_diagram.png'        # PNG filename
DEFAULT_MD_REPORT = 'audit_report.md'

import os
import subprocess
import argparse
from datetime import datetime

import psycopg2
import psycopg2.extras


def get_conn(conn_str):
    return psycopg2.connect(conn_str)


def fetch_all(cur, sql, params=None):
    cur.execute(sql, params or ())
    return cur.fetchall()


def generate_er_dot(tables, fks, dot_path):
    """Generates DOT file with HTML tables for ER diagram"""
    with open(dot_path, 'w', encoding='utf-8') as f:
        f.write('digraph ER {\n')
        f.write('  graph [rankdir=LR, splines=ortho, bgcolor="white"];\n')
        f.write('  node [shape=none, margin=0, fontname="Helvetica"];\n')
        f.write('  edge [arrowhead=none, color="#555555", fontsize=10, fontname="Helvetica"];\n\n')

        # Table nodes with columns
        for tbl in tables:
            full = f"{tbl['schema']}.{tbl['name']}"
            rows = [
                f'<TR><TD COLSPAN="2" BGCOLOR="#cccccc"><B>{full}</B></TD></TR>'
            ]
            for col in tbl['columns']:
                rows.append(
                    '<TR>'
                    f'<TD ALIGN="LEFT">{col["column_name"]}</TD>'
                    f'<TD ALIGN="LEFT">{col["data_type"]}</TD>'
                    '</TR>'
                )
            html = '<<TABLE BORDER="1" CELLBORDER="1" CELLSPACING="0">\n' + '\n'.join(rows) + '\n</TABLE>>'
            f.write(f'  "{full}" [label={html}];\n')

        f.write('\n')
        # Foreign key edges with XLabels
        for fk in fks:
            src = f"{fk['table_schema']}.{fk['table_name']}"
            dst = f"{fk['foreign_table_schema']}.{fk['foreign_table_name']}"
            cname = f"{fk['constraint_name']}"
            f.write(
                f'  "{src}" -> "{dst}" [xlabel="{cname}"];\n'
            )

        f.write('}\n')
    print(f"[+] DOT ER diagram generated: {dot_path}")


def render_png(dot_path, png_path):
    """Renders PNG using Graphviz"""
    try:
        subprocess.check_call(['dot', '-Tpng', dot_path, '-o', png_path])
        print(f"[+] PNG ER diagram generated: {png_path}")
    except FileNotFoundError:
        print("Error: Graphviz 'dot' not found. Install with: sudo apt install graphviz", file=os.sys.stderr)
    except subprocess.CalledProcessError as e:
        print(f"PNG generation error: {e}", file=os.sys.stderr)


def generate_report(data, md_path, dot_path, png_path):
    """Generates detailed Markdown report"""
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write(f"# Audit Report: `{data['db_name']}`\n")
        f.write(f"*Generated: {datetime.now():%Y-%m-%d %H:%M:%S}*\n\n")

        f.write("## General Info\n")
        f.write(f"- PostgreSQL: **{data['version']}**\n")
        f.write(f"- DB Size: **{data['db_size']}**\n")
        f.write(f"- Tables: **{len(data['tables'])}**\n\n")

        f.write("## Tables & Sample Data\n")
        for tbl in data['tables']:
            key = f"{tbl['schema']}.{tbl['name']}"
            f.write(f"### {key}\n")
            f.write(f"- Rows Estimate: `{tbl['row_estimate']}` | Size: `{tbl['size']}`\n")
            sample = data['samples'].get(key, [])
            if sample:
                cols = list(sample[0].keys())
                f.write("| " + " | ".join(cols) + " |\n")
                f.write("|" + "---|" * len(cols) + "\n")
                for row in sample:
                    vals = [str(row[c]) for c in cols]
                    f.write("| " + " | ".join(vals) + " |\n")
            else:
                f.write("No data sample.\n")
            f.write("\n")

        f.write("## ER Diagram\n")
        f.write(f"- DOT: `{dot_path}`  \n")
        f.write(f"- PNG: `{png_path}`  \n\n")

        f.write("## Functions\n")
        for fn in data['functions']:
            f.write(f"### {fn['schema']}.{fn['name']}({fn['args']}) -> {fn['return_type']}\n")
            f.write("```sql\n" + fn['definition'] + "\n```\n\n")

        f.write("## Triggers\n")
        if data['triggers']:
            for trg in data['triggers']:
                table = f"{trg['event_object_schema']}.{trg['event_object_table']}"
                f.write(f"### {trg['trigger_name']} ON {table}\n")
                f.write("```sql\n" + trg['action_statement'] + "\n```\n\n")
        else:
            f.write("No triggers found.\n")

    print(f"[+] Markdown report generated: {md_path}")


def main():
    parser = argparse.ArgumentParser(description="PostgreSQL Audit + ER Diagram")
    parser.add_argument("--conn", required=True, help="DB connection URI")
    parser.add_argument("--md", default=DEFAULT_MD_REPORT, help="Markdown report path")
    parser.add_argument("--dot", default=DOT_FILE, help="DOT file path")
    parser.add_argument("--png", default=PNG_FILE, help="PNG file path")
    args = parser.parse_args()

    conn = get_conn(args.conn)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    data = {}

    # Basic DB info
    cur.execute("SHOW server_version;")
    data['version'] = cur.fetchone()['server_version']
    cur.execute("SELECT current_database() AS db;")
    data['db_name'] = cur.fetchone()['db']
    cur.execute("SELECT pg_size_pretty(pg_database_size(current_database())) AS size;")
    data['db_size'] = cur.fetchone()['size']

    # Tables
    tables = fetch_all(cur, """
        SELECT schemaname AS schema, tablename AS name
        FROM pg_catalog.pg_tables
        WHERE schemaname NOT IN ('pg_catalog','information_schema')
        ORDER BY schema, name;
    """)
    for tbl in tables:
        sch, nm = tbl['schema'], tbl['name']
        cur.execute(f"SELECT reltuples::bigint AS estimate FROM pg_class WHERE oid = '{sch}.{nm}'::regclass;")
        tbl['row_estimate'] = cur.fetchone()['estimate']
        cur.execute(f"SELECT pg_size_pretty(pg_total_relation_size('{sch}.{nm}')); ")
        tbl['size'] = cur.fetchone()['pg_size_pretty']
        tbl['columns'] = fetch_all(cur, """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s
            ORDER BY ordinal_position;
        """, (sch, nm))
    data['tables'] = tables

    # Data samples
    samples = {}
    for tbl in tables:
        key = f"{tbl['schema']}.{tbl['name']}"
        try:
            samples[key] = fetch_all(cur, f"SELECT * FROM {key} LIMIT {SAMPLE_LIMIT};")
        except:
            samples[key] = []
    data['samples'] = samples

    # Foreign keys
    fks = fetch_all(cur, """
        SELECT kcu.table_schema, kcu.table_name,
               ccu.table_schema AS foreign_table_schema,
               ccu.table_name AS foreign_table_name,
               tc.constraint_name
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name=kcu.constraint_name
        JOIN information_schema.constraint_column_usage AS ccu
          ON ccu.constraint_name=tc.constraint_name
        WHERE tc.constraint_type='FOREIGN KEY';
    """)

    generate_er_dot(data['tables'], fks, args.dot)
    render_png(args.dot, args.png)

    # Functions
    data['functions'] = fetch_all(cur, """
        SELECT n.nspname AS schema, p.proname AS name,
               pg_get_function_arguments(p.oid) AS args,
               pg_get_function_result(p.oid) AS return_type,
               pg_get_functiondef(p.oid) AS definition
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace=n.oid
        WHERE n.nspname NOT IN ('pg_catalog','information_schema')
        ORDER BY n.nspname, p.proname;
    """)

    # Triggers
    data['triggers'] = fetch_all(cur, """
        SELECT trigger_name, event_object_schema, event_object_table, action_statement
        FROM information_schema.triggers
        WHERE trigger_schema NOT IN ('pg_catalog','information_schema');
    """)

    cur.close()
    conn.close()

    generate_report(data, args.md, args.dot, args.png)


if __name__ == '__main__':
    main()
