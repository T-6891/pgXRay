#!/usr/bin/env python3

"""
pgXRay
-----------------------------------------------------------------

Version: 1.3.0

Purpose
    Advanced PostgreSQL database audit:
      - Structure (tables, columns, indexes, constraints)
      - Sample data (up to SAMPLE_LIMIT rows from each table)
      - Full function and trigger definitions
      - ER diagram (DOT and PNG) with record-style nodes
        and cardinality indicators on edges

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
from typing import Dict, List, Any, Tuple

import psycopg2
import psycopg2.extras

class DatabaseConnector:
    def __init__(self, conn_str: str):
        self.conn = psycopg2.connect(conn_str)
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    def fetch_all(self, sql: str, params=None) -> List[Dict]:
        self.cur.execute(sql, params or ())
        return self.cur.fetchall()
    
    def fetch_one(self, sql: str, params=None) -> Dict:
        self.cur.execute(sql, params or ())
        return self.cur.fetchone()
    
    def close(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()


class DataExtractor:
    def __init__(self, db: DatabaseConnector):
        self.db = db
    
    def get_database_info(self) -> Dict:
        data = {}
        
        data['version'] = self.db.fetch_one("SHOW server_version;")['server_version']
        data['db_name'] = self.db.fetch_one("SELECT current_database() AS db;")['db']
        data['db_size'] = self.db.fetch_one(
            "SELECT pg_size_pretty(pg_database_size(current_database())) AS size;"
        )['size']
        
        return data
    
    def get_tables(self) -> List[Dict]:
        tables = self.db.fetch_all("""
            SELECT schemaname AS schema, tablename AS name
            FROM pg_catalog.pg_tables
            WHERE schemaname NOT IN ('pg_catalog','information_schema')
            ORDER BY schema, name;
        """)
        
        for tbl in tables:
            sch, nm = tbl['schema'], tbl['name']
            tbl['row_estimate'] = self.db.fetch_one(
                f"SELECT reltuples::bigint AS estimate FROM pg_class WHERE oid = '{sch}.{nm}'::regclass;"
            )['estimate']
            tbl['size'] = self.db.fetch_one(
                f"SELECT pg_size_pretty(pg_total_relation_size('{sch}.{nm}'));"
            )['pg_size_pretty']
            
            tbl['columns'] = self.db.fetch_all("""
                SELECT 
                    c.column_name, c.data_type,
                    CASE WHEN k.column_name IS NOT NULL THEN true ELSE false END AS is_primary_key
                FROM information_schema.columns c
                LEFT JOIN (
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu 
                      ON tc.constraint_name = kcu.constraint_name
                    WHERE tc.constraint_type = 'PRIMARY KEY'
                      AND tc.table_schema = %s
                      AND tc.table_name = %s
                ) k ON c.column_name = k.column_name
                WHERE c.table_schema = %s AND c.table_name = %s
                ORDER BY c.ordinal_position;
            """, (sch, nm, sch, nm))
            
            tbl['foreign_keys'] = self.db.fetch_all("""
                SELECT
                    kcu.column_name,
                    ccu.table_schema AS foreign_table_schema,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name,
                    tc.constraint_name
                FROM information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage AS ccu
                  ON ccu.constraint_name = tc.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY'
                  AND tc.table_schema = %s
                  AND tc.table_name = %s;
            """, (sch, nm))
            
            for col in tbl['columns']:
                col['is_foreign_key'] = False
                col['references'] = None
                
                for fk in tbl['foreign_keys']:
                    if col['column_name'] == fk['column_name']:
                        col['is_foreign_key'] = True
                        col['references'] = {
                            'schema': fk['foreign_table_schema'],
                            'table': fk['foreign_table_name'],
                            'column': fk['foreign_column_name'],
                            'constraint': fk['constraint_name']
                        }
                
        return tables
    
    def get_samples(self, tables: List[Dict]) -> Dict[str, List[Dict]]:
        samples = {}
        for tbl in tables:
            key = f"{tbl['schema']}.{tbl['name']}"
            try:
                samples[key] = self.db.fetch_all(f"SELECT * FROM {key} LIMIT {SAMPLE_LIMIT};")
            except Exception as e:
                print(f"Warning: Error sampling table {key}: {e}")
                samples[key] = []
        return samples
    
    def get_foreign_keys(self) -> List[Dict]:
        return self.db.fetch_all("""
            SELECT 
                kcu.table_schema, kcu.table_name,
                ccu.table_schema AS foreign_table_schema,
                ccu.table_name AS foreign_table_name,
                tc.constraint_name,
                kcu.column_name, 
                ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY';
        """)
    
    def get_functions(self) -> List[Dict]:
        return self.db.fetch_all("""
            SELECT n.nspname AS schema, p.proname AS name,
                   pg_get_function_arguments(p.oid) AS args,
                   pg_get_function_result(p.oid) AS return_type,
                   pg_get_functiondef(p.oid) AS definition
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname NOT IN ('pg_catalog','information_schema')
            ORDER BY n.nspname, p.proname;
        """)
    
    def get_triggers(self) -> List[Dict]:
        return self.db.fetch_all("""
            SELECT trigger_name, event_object_schema, event_object_table, action_statement
            FROM information_schema.triggers
            WHERE trigger_schema NOT IN ('pg_catalog','information_schema');
        """)
    
    def get_all_data(self) -> Dict:
        data = self.get_database_info()
        data['tables'] = self.get_tables()
        data['samples'] = self.get_samples(data['tables'])
        data['foreign_keys'] = self.get_foreign_keys()
        data['functions'] = self.get_functions()
        data['triggers'] = self.get_triggers()
        return data


class ERDiagramGenerator:
    def __init__(self, tables: List[Dict], foreign_keys: List[Dict]):
        self.tables = tables
        self.foreign_keys = foreign_keys
        self.schemas = set(tbl['schema'] for tbl in tables)
    
    def get_column_details(self, columns: List[Dict]) -> List[str]:
        """Получение отформатированных деталей столбцов для HTML-таблицы"""
        result = []
        
        for col in columns:
            pk_marker = '<TD BGCOLOR="#E0FFE0"><B>PK</B></TD>' if col['is_primary_key'] else '<TD></TD>'
            fk_marker = '<TD BGCOLOR="#E0E0FF"><B>FK</B></TD>' if col['is_foreign_key'] else '<TD></TD>'
            
            # Форматирование строки с типом столбца
            col_name = col['column_name']
            data_type = col['data_type']
            
            # Создание строки HTML-таблицы для данного столбца
            result.append(f'<TR><TD ALIGN="LEFT">{col_name}</TD><TD ALIGN="LEFT">{data_type}</TD>{pk_marker}{fk_marker}</TR>')
        
        return result

    def generate_table_html(self, table: Dict) -> str:
        """Создает HTML-представление таблицы для использования в Graphviz"""
        table_name = table['name']
        column_details = self.get_column_details(table['columns'])
        column_rows = "\n".join(column_details)
        
        html = f'''<
            <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">
                <TR>
                    <TD COLSPAN="4" BGCOLOR="#4D7A97"><FONT COLOR="white"><B>{table_name}</B></FONT></TD>
                </TR>
                <TR>
                    <TD BGCOLOR="#EEEEFF"><B>Column</B></TD>
                    <TD BGCOLOR="#EEEEFF"><B>Type</B></TD>
                    <TD BGCOLOR="#EEEEFF"><B>PK</B></TD>
                    <TD BGCOLOR="#EEEEFF"><B>FK</B></TD>
                </TR>
                {column_rows}
            </TABLE>
        >'''
        return html

    def generate_er_dot(self, dot_path: str):
        """Генерирует улучшенный DOT-файл для ER-диаграммы с использованием HTML-таблиц"""
        with open(dot_path, 'w', encoding='utf-8') as f:
            f.write('digraph ER {\n')
            f.write('  graph [rankdir=LR, fontname="Helvetica", fontsize=12, pad="0.5", nodesep="0.5", ranksep="1.5"];\n')
            f.write('  node [shape=plain, fontname="Helvetica", fontsize=10];\n')
            f.write('  edge [arrowhead=crow, arrowtail=none, dir=both, fontname="Helvetica", fontsize=9, penwidth=1.0];\n\n')
            
            # Создаем кластеры (подграфы) для каждой схемы
            for schema in sorted(self.schemas):
                f.write(f'  subgraph cluster_{schema} {{\n')
                f.write(f'    label="Schema: {schema}";\n')
                f.write('    style="filled";\n')
                f.write('    color="#EEEEEE";\n')
                f.write('    fontname="Helvetica-Bold";\n')
                f.write('    fontsize=12;\n')
                
                # Добавляем таблицы для текущей схемы
                for tbl in self.tables:
                    if tbl['schema'] == schema:
                        full_name = f"{tbl['schema']}.{tbl['name']}"
                        table_html = self.generate_table_html(tbl)
                        f.write(f'    "{full_name}" [label={table_html}];\n')
                
                f.write('  }\n\n')
            
            # Добавляем ребра (связи между таблицами)
            for fk in self.foreign_keys:
                src = f"{fk['table_schema']}.{fk['table_name']}"
                dst = f"{fk['foreign_table_schema']}.{fk['foreign_table_name']}"
                
                # Создаем метку для связи
                label = f"{fk['constraint_name']}"
                
                # Добавляем ребро с улучшенным стилем и обозначением кардинальности
                f.write(f'  "{src}" -> "{dst}" [label="{label}", fontname="Helvetica", fontsize=8, ')
                f.write('color="#5D8AA8", style="solid", arrowhead=normal, arrowtail=crow];\n')
            
            f.write('}\n')
        
        print(f"[+] DOT ER diagram generated: {dot_path}")

    def render_png(self, dot_path: str, png_path: str):
        """Преобразует DOT-файл в PNG с помощью Graphviz с улучшенными параметрами отрисовки"""
        try:
            # Используем улучшенный рендеринг с более высоким DPI
            subprocess.check_call(['dot', '-Tpng', '-Gdpi=300', dot_path, '-o', png_path])
            print(f"[+] PNG ER diagram generated: {png_path}")
        except FileNotFoundError:
            print("Error: Graphviz 'dot' not found. Install with: sudo apt install graphviz", file=os.sys.stderr)
        except subprocess.CalledProcessError as e:
            print(f"PNG generation error: {e}", file=os.sys.stderr)


class ReportGenerator:
    def __init__(self, data: Dict):
        self.data = data
    
    def escape_markdown(self, text):
        if text is None:
            return ""
        escape_chars = ['|', '_', '*', '`', '[', ']', '(', ')', '#', '+', '-', '.', '!']
        result = str(text)
        for char in escape_chars:
            result = result.replace(char, '\\' + char)
        return result
    
    def generate_markdown_report(self, md_path: str, dot_path: str, png_path: str):
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write(f"# Audit Report: `{self.data['db_name']}`\n")
            f.write(f"*Generated: {datetime.now():%Y-%m-%d %H:%M:%S}*\n\n")
            
            f.write("## General Info\n")
            f.write(f"- PostgreSQL: **{self.data['version']}**\n")
            f.write(f"- DB Size: **{self.data['db_size']}**\n")
            f.write(f"- Tables: **{len(self.data['tables'])}**\n\n")
            
            f.write("## Tables & Sample Data\n")
            for tbl in self.data['tables']:
                key = f"{tbl['schema']}.{tbl['name']}"
                f.write(f"### {key}\n")
                f.write(f"- Rows Estimate: `{tbl['row_estimate']}` | Size: `{tbl['size']}`\n")
                
                f.write("#### Columns\n\n")
                
                f.write("| Name | Type | Key | References |\n")
                f.write("| ---- | ---- | --- | ---------- |\n")
                
                for col in tbl['columns']:
                    key_type = ""
                    references = ""
                    
                    if col['is_primary_key']:
                        key_type = "PK"
                    
                    if col['is_foreign_key']:
                        key_type += " FK" if key_type else "FK"
                        ref = col['references']
                        references = f"{ref['schema']}.{ref['table']}.{ref['column']}"
                    
                    col_name = self.escape_markdown(col['column_name'])
                    data_type = self.escape_markdown(col['data_type'])
                    key_type = self.escape_markdown(key_type)
                    references = self.escape_markdown(references)
                    
                    f.write(f"| {col_name} | {data_type} | {key_type} | {references} |\n")
                
                f.write("\n#### Sample Data\n\n")
                sample = self.data['samples'].get(key, [])
                if sample:
                    cols = list(sample[0].keys())
                    
                    header_row = " | ".join([self.escape_markdown(col) for col in cols])
                    f.write(f"| {header_row} |\n")
                    
                    separator = " | ".join(["----" for _ in cols])
                    f.write(f"| {separator} |\n")
                    
                    for row in sample:
                        vals = [self.escape_markdown(row[c]) for c in cols]
                        data_row = " | ".join(vals)
                        f.write(f"| {data_row} |\n")
                else:
                    f.write("No data sample.\n")
                f.write("\n")
            
            f.write("## ER Diagram\n")
            f.write(f"- DOT: `{dot_path}`  \n")
            f.write(f"- PNG: `{png_path}`  \n\n")
            
            f.write("## Functions\n")
            for fn in self.data['functions']:
                f.write(f"### {fn['schema']}.{fn['name']}({fn['args']}) -> {fn['return_type']}\n")
                f.write("```sql\n" + fn['definition'] + "\n```\n\n")
            
            f.write("## Triggers\n")
            if self.data['triggers']:
                for trg in self.data['triggers']:
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
    
    try:
        db = DatabaseConnector(args.conn)
        
        extractor = DataExtractor(db)
        data = extractor.get_all_data()
        
        erd_generator = ERDiagramGenerator(data['tables'], data['foreign_keys'])
        erd_generator.generate_er_dot(args.dot)
        erd_generator.render_png(args.dot, args.png)
        
        report_generator = ReportGenerator(data)
        report_generator.generate_markdown_report(args.md, args.dot, args.png)
        
        db.close()
        
        print(f"[+] All done! Report is available at {args.md}")
        
    except Exception as e:
        print(f"Error: {e}", file=os.sys.stderr)
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main())
