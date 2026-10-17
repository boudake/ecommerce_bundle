# ecommerce_bundle

docker compose up -d --build

docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy \
  --packages org.apache.hadoop:hadoop-aws:3.4.1,io.delta:delta-spark_2.13:4.4.0 /
  /opt/spark-apps/main.py



docker compose exec spark-master ls /opt/spark-apps/


docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
   --conf spark.jars.ivy=/tmp/.ivy \
  --packages org.apache.hadoop:hadoop-aws:3.4.1,io.delta:delta-spark_2.13:4.4.0 /
  /opt/spark-apps/main.py



docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=/opt/spark-apps/config/log4j2.properties" \
  /opt/spark-apps/src/main.py


  docker compose exec spark-worker-1 ls -lh /opt