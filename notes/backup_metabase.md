# dump backup metabase

```
echo "\c metabasedb;" > ./docker-configs/postgres/02-init-metabase_bu.sql && \
docker exec -t postgres pg_dump -U metabase metabasedb | grep -v "restrict" >> ./docker-configs/postgres/02-init-metabase_bu.sql

```
