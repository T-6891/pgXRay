#!/usr/bin/env python3

"""
pgXRay
-----------------------------------------------------------------

Version: 3.2.1

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

#############################################
# Модуль работы с базой данных
#############################################

class DatabaseConnector:
    """Работа с подключением к PostgreSQL и выполнение запросов"""
    
    def __init__(self, conn_str: str):
        """Инициализация с строкой подключения"""
        self.conn = psycopg2.connect(conn_str)
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    def fetch_all(self, sql: str, params=None) -> List[Dict]:
        """Выполнение запроса с возвратом всех результатов"""
        self.cur.execute(sql, params or ())
        return self.cur.fetchall()
    
    def fetch_one(self, sql: str, params=None) -> Dict:
        """Выполнение запроса с возвратом одной строки"""
        self.cur.execute(sql, params or ())
        return self.cur.fetchone()
    
    def close(self):
        """Закрытие соединения"""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()


#############################################
# Модуль извлечения данных
#############################################

class DataExtractor:
    """Извлечение всех необходимых данных из PostgreSQL"""
    
    def __init__(self, db: DatabaseConnector):
        """Инициализация с подключением к БД"""
        self.db = db
    
    def get_database_info(self) -> Dict:
        """Получение общей информации о базе данных"""
        data = {}
        
        data['version'] = self.db.fetch_one("SHOW server_version;")['server_version']
        data['db_name'] = self.db.fetch_one("SELECT current_database() AS db;")['db']
        data['db_size'] = self.db.fetch_one(
            "SELECT pg_size_pretty(pg_database_size(current_database())) AS size;"
        )['size']
        
        return data
    
    def get_tables(self) -> List[Dict]:
        """Получение списка таблиц с информацией о столбцах"""
        tables = self.db.fetch_all("""
            SELECT schemaname AS schema, tablename AS name
            FROM pg_catalog.pg_tables
            WHERE schemaname NOT IN ('pg_catalog','information_schema')
            ORDER BY schema, name;
        """)
        
        # Получаем дополнительную информацию для каждой таблицы
        for tbl in tables:
            sch, nm = tbl['schema'], tbl['name']
            tbl['row_estimate'] = self.db.fetch_one(
                f"SELECT reltuples::bigint AS estimate FROM pg_class WHERE oid = '{sch}.{nm}'::regclass;"
            )['estimate']
            tbl['size'] = self.db.fetch_one(
                f"SELECT pg_size_pretty(pg_total_relation_size('{sch}.{nm}'));"
            )['pg_size_pretty']
            
            # Получаем информацию о столбцах
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
            
            # Получаем информацию о внешних ключах
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
            
            # Обновляем атрибуты столбцов с информацией о внешних ключах
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
        """Получение образцов данных для всех таблиц"""
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
        """Получение всех внешних ключей БД"""
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
        """Получение всех пользовательских функций"""
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
        """Получение всех триггеров"""
        return self.db.fetch_all("""
            SELECT trigger_name, event_object_schema, event_object_table, action_statement
            FROM information_schema.triggers
            WHERE trigger_schema NOT IN ('pg_catalog','information_schema');
        """)
    
    def get_all_data(self) -> Dict:
        """Получение всех данных для отчёта"""
        data = self.get_database_info()
        data['tables'] = self.get_tables()
        data['samples'] = self.get_samples(data['tables'])
        data['foreign_keys'] = self.get_foreign_keys()
        data['functions'] = self.get_functions()
        data['triggers'] = self.get_triggers()
        return data


#############################################
# Модуль генерации ER-диаграммы
#############################################

class ERDiagramGenerator:
    """Класс для генерации ER-диаграмм в формате DOT и PNG"""
    
    def __init__(self, tables: List[Dict], foreign_keys: List[Dict]):
        """Инициализация с таблицами и внешними ключами"""
        self.tables = tables
        self.foreign_keys = foreign_keys
    
    def format_columns_for_record(self, columns: List[Dict]) -> str:
        """Форматирование списка столбцов для record-стиля"""
        result = []
        for col in columns:
            prefix = ""
            if col['is_primary_key']:
                prefix = "+"  # Обозначаем первичный ключ +
            elif col['is_foreign_key']:
                prefix = "#"  # Обозначаем внешний ключ #
            else:
                prefix = "~"  # Обозначаем обычные атрибуты ~
            
            suffix = ""
            if col['is_primary_key']:
                suffix = " (PK)"
            if col['is_foreign_key']:
                suffix = " (FK)"
                
            result.append(f"{prefix} {col['column_name']}{suffix}: {col['data_type']}")
            
        return "\\n".join(result)

    def determine_cardinality(self, fk: Dict) -> Tuple[str, str]:
        """Определение кардинальности для отношений"""
        # По умолчанию для отношений БД используем "1" для PK и "N" для FK
        # так как обычно внешний ключ может содержать множество ссылок
        return "1", "N"
    
    def generate_er_dot(self, dot_path: str):
        """Генерирует DOT-файл в стиле record для ER-диаграммы"""
        with open(dot_path, 'w', encoding='utf-8') as f:
            f.write('digraph ER {\n')
            f.write('  graph [rankdir=LR, splines=ortho, bgcolor="white"];\n')
            f.write('  node [shape=record, style=rounded, fontname="Helvetica", fontsize=10];\n')
            f.write('  edge [dir=both, arrowhead=normal, arrowtail=none, fontsize=9, fontname="Helvetica"];\n\n')
            
            # Генерация узлов таблиц с атрибутами в стиле record
            for tbl in self.tables:
                full = f"{tbl['schema']}.{tbl['name']}"
                columns_formatted = self.format_columns_for_record(tbl['columns'])
                
                # Разделяем имя сущности и атрибуты вертикальной линией
                label = f"{full}|{columns_formatted}"
                
                # Используем формат записи для разделения частей
                f.write(f'  "{full}" [label="{{{label}}}"];\n')
            
            f.write('\n')
            
            # Генерация рёбер отношений с кардинальностью
            for fk in self.foreign_keys:
                src = f"{fk['table_schema']}.{fk['table_name']}"
                dst = f"{fk['foreign_table_schema']}.{fk['foreign_table_name']}"
                relation_name = fk['constraint_name']
                
                # Определяем кардинальность
                tail_card, head_card = self.determine_cardinality(fk)
                
                # Создаём ребро с кардинальностью и именем отношения
                # Используем xlabel вместо label для совместимости с ортогональными линиями
                f.write(
                    f'  "{src}" -> "{dst}" [taillabel="{tail_card}", '
                    f'headlabel="{head_card}", xlabel="{relation_name}"];\n'
                )
            
            f.write('}\n')
        
        print(f"[+] DOT ER diagram generated: {dot_path}")
    
    def render_png(self, dot_path: str, png_path: str):
        """Преобразует DOT-файл в PNG с помощью Graphviz"""
        try:
            subprocess.check_call(['dot', '-Tpng', dot_path, '-o', png_path])
            print(f"[+] PNG ER diagram generated: {png_path}")
        except FileNotFoundError:
            print("Error: Graphviz 'dot' not found. Install with: sudo apt install graphviz", file=os.sys.stderr)
        except subprocess.CalledProcessError as e:
            print(f"PNG generation error: {e}", file=os.sys.stderr)


#############################################
# Модуль генерации отчёта
#############################################

class ReportGenerator:
    """Класс для генерации отчётов в формате Markdown"""
    
    def __init__(self, data: Dict):
        """Инициализация с данными аудита"""
        self.data = data
    
    def escape_markdown(self, text):
        """Экранирует специальные символы Markdown"""
        if text is None:
            return ""
        # Экранирование специальных символов Markdown
        escape_chars = ['|', '_', '*', '`', '[', ']', '(', ')', '#', '+', '-', '.', '!']
        result = str(text)
        for char in escape_chars:
            result = result.replace(char, '\\' + char)
        return result
    
    def generate_markdown_report(self, md_path: str, dot_path: str, png_path: str):
        """Генерирует детализированный отчёт в формате Markdown"""
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write(f"# Audit Report: `{self.data['db_name']}`\n")
            f.write(f"*Generated: {datetime.now():%Y-%m-%d %H:%M:%S}*\n\n")
            
            # Общая информация
            f.write("## General Info\n")
            f.write(f"- PostgreSQL: **{self.data['version']}**\n")
            f.write(f"- DB Size: **{self.data['db_size']}**\n")
            f.write(f"- Tables: **{len(self.data['tables'])}**\n\n")
            
            # Таблицы и образцы данных
            f.write("## Tables & Sample Data\n")
            for tbl in self.data['tables']:
                key = f"{tbl['schema']}.{tbl['name']}"
                f.write(f"### {key}\n")
                f.write(f"- Rows Estimate: `{tbl['row_estimate']}` | Size: `{tbl['size']}`\n")
                
                # Описание столбцов с метками PK/FK
                f.write("#### Columns\n\n")
                
                # Корректная таблица с заголовком и разделителями
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
                    
                    # Экранирование специальных символов Markdown
                    col_name = self.escape_markdown(col['column_name'])
                    data_type = self.escape_markdown(col['data_type'])
                    key_type = self.escape_markdown(key_type)
                    references = self.escape_markdown(references)
                    
                    f.write(f"| {col_name} | {data_type} | {key_type} | {references} |\n")
                
                # Образцы данных
                f.write("\n#### Sample Data\n\n")
                sample = self.data['samples'].get(key, [])
                if sample:
                    cols = list(sample[0].keys())
                    
                    # Корректный заголовок таблицы
                    header_row = " | ".join([self.escape_markdown(col) for col in cols])
                    f.write(f"| {header_row} |\n")
                    
                    # Корректный разделитель
                    separator = " | ".join(["----" for _ in cols])
                    f.write(f"| {separator} |\n")
                    
                    # Строки данных
                    for row in sample:
                        # Экранирование всех значений
                        vals = [self.escape_markdown(row[c]) for c in cols]
                        # Форматирование и запись строки
                        data_row = " | ".join(vals)
                        f.write(f"| {data_row} |\n")
                else:
                    f.write("No data sample.\n")
                f.write("\n")
            
            # ER-диаграмма
            f.write("## ER Diagram\n")
            f.write(f"- DOT: `{dot_path}`  \n")
            f.write(f"- PNG: `{png_path}`  \n\n")
            
            # Функции
            f.write("## Functions\n")
            for fn in self.data['functions']:
                f.write(f"### {fn['schema']}.{fn['name']}({fn['args']}) -> {fn['return_type']}\n")
                f.write("```sql\n" + fn['definition'] + "\n```\n\n")
            
            # Триггеры
            f.write("## Triggers\n")
            if self.data['triggers']:
                for trg in self.data['triggers']:
                    table = f"{trg['event_object_schema']}.{trg['event_object_table']}"
                    f.write(f"### {trg['trigger_name']} ON {table}\n")
                    f.write("```sql\n" + trg['action_statement'] + "\n```\n\n")
            else:
                f.write("No triggers found.\n")
        
        print(f"[+] Markdown report generated: {md_path}")


#############################################
# Основной модуль
#############################################

def main():
    """Основная функция программы"""
    # Разбор аргументов командной строки
    parser = argparse.ArgumentParser(description="PostgreSQL Audit + ER Diagram")
    parser.add_argument("--conn", required=True, help="DB connection URI")
    parser.add_argument("--md", default=DEFAULT_MD_REPORT, help="Markdown report path")
    parser.add_argument("--dot", default=DOT_FILE, help="DOT file path")
    parser.add_argument("--png", default=PNG_FILE, help="PNG file path")
    args = parser.parse_args()
    
    try:
        # Подключение к БД
        db = DatabaseConnector(args.conn)
        
        # Извлечение данных
        extractor = DataExtractor(db)
        data = extractor.get_all_data()
        
        # Генерация ER-диаграммы
        erd_generator = ERDiagramGenerator(data['tables'], data['foreign_keys'])
        erd_generator.generate_er_dot(args.dot)
        erd_generator.render_png(args.dot, args.png)
        
        # Генерация отчёта
        report_generator = ReportGenerator(data)
        report_generator.generate_markdown_report(args.md, args.dot, args.png)
        
        # Закрытие подключения
        db.close()
        
        print(f"[+] All done! Report is available at {args.md}")
        
    except Exception as e:
        print(f"Error: {e}", file=os.sys.stderr)
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main())
