#/bin/bash
if [ "$HOSTNAME" = tools ]; then
  echo "We don't need to update hosts in the tools container. Exiting."
  exit 1
fi

if grep "FUN host entries" /etc/hosts >/dev/null; then
  echo "Already done!"
  exit 0
fi

cat << EOF | sudo tee -a /etc/hosts >/dev/null

# FUN host entries
127.0.0.1 hive-metastore
127.0.0.1 minio
127.0.0.1 trino

EOF
echo Done!