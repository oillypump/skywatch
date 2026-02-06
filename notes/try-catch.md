try: # 1. Coba load table
table = catalog.load_table(table_id)
print(f"ℹ️ Table {table_id} found. Appending data...")
except NoSuchTableError: # 2. Jika tidak ada, coba buat baru
print(f"✨ Table not found. Creating new table {table_id}...")
try:
table = catalog.create_table(
identifier=table_id,
schema=arrow_table.schema,
location=f"s3a://lakehouse/{tgt_namespace}/{table_name}",
)
except TableAlreadyExistsError: # 3. KONDISI KHUSUS: Hive bilang ada tapi Iceberg bilang nggak ada
print(f"⚠️ Metadata mismatch! Hive thinks it exists. Dropping and recreating...")
catalog.drop_table(table_id)
table = catalog.create_table(
identifier=table_id,
schema=arrow_table.schema,
location=f"s3a://lakehouse/{tgt_namespace}/{table_name}",
)

table.append(arrow_table)
