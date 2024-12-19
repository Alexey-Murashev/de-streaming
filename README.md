This project is attempt to implement stream processing using Apache Flink for the following requirements:
1. produce records stream from csv file with some rate
2. filter stream by price >= 50000 and livingArea >= 20
3. group by district and type
4. aggregate average price, average livingArea, average price per square meter, total amount of offers
5. update aggregated result every 30 seconds

Prerequisites:
* JDK 11
* K8s (tested in minikube)

How to use:
1. download source data: https://www.kaggle.com/datasets/luvathoms/portugal-real-estate-2024/data
2. build app:mvn shade:shade 
3. run flink in k8s using descriptors from k8s folder
4. copy built jar file and downloaded source data to the folder mounted to the k8s
5. open shell session to the Flink's job-manager container: kubectl exec -it <podname> -- bash
6. run app using Flink CLI: flink run /path-to-jar /path-to-data.csv

For better experience forward Flink Job Manager endpoint to access to Web UI
example for minikube:
kubectl port-forward <flink-jobmanager-pod> 8081:8081

Now one can watch job progress and details in WebUI

Issues:
it doesn't update aggregated result by intervals, only at the end of stream