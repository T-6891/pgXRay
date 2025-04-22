#!/usr/bin/env python3

"""
pgXRay

Version    : 2.0.1
Author     : Vladimir Smelnitskiy  <master@t-brain.ru>
Repository : https://github.com/T-6891/pgXRay
"""


DOT_FILE = 'er_diagram.dot'
PNG_FILE = 'er_diagram.png'
DEFAULT_MD_REPORT = 'audit_report.md'

import os
import argparse
import sys

from db_connector import DatabaseConnector
from data_extractor import DataExtractor, SAMPLE_LIMIT
from er_diagram_generator import ERDiagramGenerator
from report_generator import ReportGenerator

def main():
    parser = argparse.ArgumentParser(description="PostgreSQL Audit + ER Diagram")
    parser.add_argument("--conn", required=True, help="DB connection URI")
    parser.add_argument("--md", default=DEFAULT_MD_REPORT, help="Markdown report path")
    parser.add_argument("--dot", default=DOT_FILE, help="DOT file path")
    parser.add_argument("--png", default=PNG_FILE, help="PNG file path")
    args = parser.parse_args()
    
    try:
        # Подключение к базе данных
        db = DatabaseConnector(args.conn)
        
        # Извлечение данных
        extractor = DataExtractor(db)
        data = extractor.get_all_data()
        
        # Генерация ER-диаграммы
        erd_generator = ERDiagramGenerator(data['tables'], data['foreign_keys'], data['views'])
        erd_generator.generate_er_dot(args.dot)
        erd_generator.render_png(args.dot, args.png)
        
        # Генерация отчета
        report_generator = ReportGenerator(data)
        report_generator.generate_markdown_report(args.md, args.dot, args.png)
        
        # Закрытие соединения
        db.close()
        
        print(f"[+] All done! Report is available at {args.md}")
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main())
