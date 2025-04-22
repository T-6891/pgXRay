from typing import Dict, List, Any, Tuple

SAMPLE_LIMIT = 10

class DataExtractor:
    def __init__(self, db):
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
    
    def get_views(self) -> List[Dict]:
        views = self.db.fetch_all("""
            SELECT 
                c.relname AS name,
                n.nspname AS schema,
                CASE WHEN c.relkind = 'm' THEN true ELSE false END AS is_materialized,
                pg_get_viewdef(c.oid) AS definition,
                obj_description(c.oid, 'pg_class') AS description,
                c.reltuples::bigint AS row_estimate,
                pg_size_pretty(pg_relation_size(c.oid)) AS size
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('v', 'm')  -- v = view, m = materialized view
              AND n.nspname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY n.nspname, c.relname;
        """)
        
        for view in views:
            sch, nm = view['schema'], view['name']
            view['columns'] = self.db.fetch_all("""
                SELECT 
                    a.attname AS column_name,
                    pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type
                FROM pg_catalog.pg_attribute a
                JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
                JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                WHERE a.attnum > 0 
                  AND NOT a.attisdropped
                  AND c.relname = %s
                  AND n.nspname = %s
                ORDER BY a.attnum;
            """, (nm, sch))
            
            view['dependencies'] = self.db.fetch_all("""
                WITH RECURSIVE view_deps AS (
                    SELECT DISTINCT 
                        cl.oid AS view_id,
                        ref.relname AS ref_table_name,
                        ref_ns.nspname AS ref_table_schema
                    FROM pg_class cl
                    JOIN pg_namespace ns ON cl.relnamespace = ns.oid
                    JOIN pg_depend dep ON dep.refobjid <> cl.oid AND dep.objid = cl.oid
                    JOIN pg_rewrite rw ON rw.oid = dep.objid
                    JOIN pg_class ref ON ref.oid = dep.refobjid
                    JOIN pg_namespace ref_ns ON ref_ns.oid = ref.relnamespace
                    WHERE cl.relname = %s
                      AND ns.nspname = %s
                      AND ref.relkind IN ('r', 'v', 'm')
                )
                SELECT DISTINCT ref_table_schema, ref_table_name
                FROM view_deps;
            """, (nm, sch))
        
        return views
    
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
        data['views'] = self.get_views()
        return data
