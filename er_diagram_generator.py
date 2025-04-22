import os
import subprocess
from typing import Dict, List

class ERDiagramGenerator:
    def __init__(self, tables: List[Dict], foreign_keys: List[Dict], views: List[Dict]):
        self.tables = tables
        self.foreign_keys = foreign_keys
        self.views = views
        self.schemas = set(tbl['schema'] for tbl in tables).union(
            set(view['schema'] for view in views)
        )
    
    def get_column_details(self, columns: List[Dict]) -> List[str]:
        result = []
        
        for col in columns:
            pk_marker = '<TD BGCOLOR="#E0FFE0"><B>PK</B></TD>' if col['is_primary_key'] else '<TD></TD>'
            fk_marker = '<TD BGCOLOR="#E0E0FF"><B>FK</B></TD>' if col['is_foreign_key'] else '<TD></TD>'
            
            col_name = col['column_name']
            data_type = col['data_type']
            
            result.append(f'<TR><TD ALIGN="LEFT">{col_name}</TD><TD ALIGN="LEFT">{data_type}</TD>{pk_marker}{fk_marker}</TR>')
        
        return result

    def generate_table_html(self, table: Dict) -> str:
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
    
    def generate_view_html(self, view: Dict) -> str:
        view_name = view['name']
        view_type = "Materialized View" if view['is_materialized'] else "View"
        
        column_rows = "\n".join([
            f'<TR><TD ALIGN="LEFT">{col["column_name"]}</TD><TD ALIGN="LEFT">{col["data_type"]}</TD></TR>'
            for col in view['columns']
        ])
        
        html = f'''<
            <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">
                <TR>
                    <TD COLSPAN="2" BGCOLOR="#6A8759"><FONT COLOR="white"><B>{view_name}</B> ({view_type})</FONT></TD>
                </TR>
                <TR>
                    <TD BGCOLOR="#EEEEFF"><B>Column</B></TD>
                    <TD BGCOLOR="#EEEEFF"><B>Type</B></TD>
                </TR>
                {column_rows}
            </TABLE>
        >'''
        return html

    def generate_er_dot(self, dot_path: str):
        with open(dot_path, 'w', encoding='utf-8') as f:
            f.write('digraph ER {\n')
            f.write('  graph [rankdir=LR, fontname="Helvetica", fontsize=12, pad="0.5", nodesep="0.5", ranksep="1.5"];\n')
            f.write('  node [shape=plain, fontname="Helvetica", fontsize=10];\n')
            f.write('  edge [arrowhead=crow, arrowtail=none, dir=both, fontname="Helvetica", fontsize=9, penwidth=1.0];\n\n')
            
            for schema in sorted(self.schemas):
                f.write(f'  subgraph cluster_{schema} {{\n')
                f.write(f'    label="Schema: {schema}";\n')
                f.write('    style="filled";\n')
                f.write('    color="#EEEEEE";\n')
                f.write('    fontname="Helvetica-Bold";\n')
                f.write('    fontsize=12;\n')
                
                for tbl in self.tables:
                    if tbl['schema'] == schema:
                        full_name = f"{tbl['schema']}.{tbl['name']}"
                        table_html = self.generate_table_html(tbl)
                        f.write(f'    "{full_name}" [label={table_html}];\n')
                
                for view in self.views:
                    if view['schema'] == schema:
                        full_name = f"{view['schema']}.{view['name']}"
                        view_html = self.generate_view_html(view)
                        f.write(f'    "{full_name}" [label={view_html}, style="dashed"];\n')
                
                f.write('  }\n\n')
            
            for fk in self.foreign_keys:
                src = f"{fk['table_schema']}.{fk['table_name']}"
                dst = f"{fk['foreign_table_schema']}.{fk['foreign_table_name']}"
                
                label = f"{fk['constraint_name']}"
                
                f.write(f'  "{src}" -> "{dst}" [label="{label}", fontname="Helvetica", fontsize=8, ')
                f.write('color="#5D8AA8", style="solid", arrowhead=normal, arrowtail=crow];\n')
            
            for view in self.views:
                src = f"{view['schema']}.{view['name']}"
                for dep in view['dependencies']:
                    dst = f"{dep['ref_table_schema']}.{dep['ref_table_name']}"
                    f.write(f'  "{src}" -> "{dst}" [style="dashed", arrowhead="vee", color="#7B8B6F"];\n')
            
            f.write('}\n')
        
        print(f"[+] DOT ER diagram generated: {dot_path}")

    def render_png(self, dot_path: str, png_path: str):
        try:
            subprocess.check_call(['dot', '-Tpng', '-Gdpi=300', dot_path, '-o', png_path])
            print(f"[+] PNG ER diagram generated: {png_path}")
        except FileNotFoundError:
            print("Error: Graphviz 'dot' not found. Install with: sudo apt install graphviz", file=os.sys.stderr)
        except subprocess.CalledProcessError as e:
            print(f"PNG generation error: {e}", file=os.sys.stderr)
