#!/bin/bash

#!/bin/bash

cd spark-3.2.3 # clone한 폴더 이름에 맞게 변경
sudo ./build/mvn -pl :spark-core_2.12,:spark-sql_2.12 -DskipTests clean install
cp ./sql/core/target/spark-sql_2.12-3.2.3.jar ./assembly/target/scala-2.12/jars/
cp ./core/target/spark-core_2.12-3.2.3.jar ./assembly/target/scala-2.12/jars/
cd ..
