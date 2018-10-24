# create a redshift cluster
aws redshift create-cluster \
--node-type dw.hs1.xlarge \
--number-of-nodes 2 \
--master-username <> \
--master-user-password <> \
--cluster-identifier insightdw
