from datetime import datetime
from typing import Dict

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
            
            # Общая информация
            f.write("## General Info\n")
            f.write(f"- PostgreSQL: **{self.data['version']}**\n")
            f.write(f"- DB Size: **{self.data['db_size']}**\n")
            f.write(f"- Tables: **{len(self.data['tables'])}**\n")
            f.write(f"- Views: **{len(self.data['views'])}**\n\n")
            
            # Информация о таблицах и образцах данных
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
            
            # Информация о представлениях (views)
            f.write("## Views\n")
            if self.data['views']:
                for view in self.data['views']:
                    view_type = "Materialized View" if view['is_materialized'] else "View"
                    full_name = f"{view['schema']}.{view['name']}"
                    f.write(f"### {full_name} ({view_type})\n")
                    
                    if view['description']:
                        f.write(f"*{self.escape_markdown(view['description'])}*\n\n")
                    
                    f.write(f"- Rows Estimate: `{view['row_estimate']}`\n")
                    f.write(f"- Size: `{view['size']}`\n\n")
                    
                    # Столбцы представления
                    f.write("#### Columns\n\n")
                    f.write("| Name | Type |\n")
                    f.write("| ---- | ---- |\n")
                    
                    for col in view['columns']:
                        col_name = self.escape_markdown(col['column_name'])
                        data_type = self.escape_markdown(col['data_type'])
                        f.write(f"| {col_name} | {data_type} |\n")
                    
                    # Зависимости представления
                    f.write("\n#### Dependencies\n\n")
                    if view['dependencies']:
                        for dep in view['dependencies']:
                            ref = f"{dep['ref_table_schema']}.{dep['ref_table_name']}"
                            f.write(f"- {self.escape_markdown(ref)}\n")
                    else:
                        f.write("No direct dependencies found.\n")
                    
                    # Определение представления
                    f.write("\n#### Definition\n")
                    f.write("```sql\n" + view['definition'] + "\n```\n\n")
            else:
                f.write("No views found.\n")
            
            # Информация о ER-диаграмме
            f.write("## ER Diagram\n")
            f.write(f"- DOT: `{dot_path}`  \n")
            f.write(f"- PNG: `{png_path}`  \n\n")
            
            # Информация о функциях
            f.write("## Functions\n")
            for fn in self.data['functions']:
                f.write(f"### {fn['schema']}.{fn['name']}({fn['args']}) -> {fn['return_type']}\n")
                f.write("```sql\n" + fn['definition'] + "\n```\n\n")
            
            # Информация о триггерах
            f.write("## Triggers\n")
            if self.data['triggers']:
                for trg in self.data['triggers']:
                    table = f"{trg['event_object_schema']}.{trg['event_object_table']}"
                    f.write(f"### {trg['trigger_name']} ON {table}\n")
                    f.write("```sql\n" + trg['action_statement'] + "\n```\n\n")
            else:
                f.write("No triggers found.\n")
        
        print(f"[+] Markdown report generated: {md_path}")
