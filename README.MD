# ----------------------------------------------
# STEP 1: Log in to Azure
# ----------------------------------------------
az login

# ----------------------------------------------
# STEP 2: Verify infrastructure
# ----------------------------------------------
az group show --name rg-dev-westeurope-meim
az aks show --resource-group rg-dev-westeurope-meim --name aks-dev-westeurope-meim
az acr show --name acrdevwesteuropemeim
az storage account show --name stdevwesteuropemeim --resource-group rg-dev-westeurope-meim

# ----------------------------------------------
# STEP 3: Get AKS credentials for kubectl (8.2 Set kubeconfig.yaml as Default for kubectl in Current Terminal Session)
# ----------------------------------------------
az aks get-credentials --resource-group rg-dev-westeurope-meim --name aks-dev-westeurope-meim

# ----------------------------------------------
# STEP 4: Test Kubernetes access (8.3 Verify Kubernetes Cluster Connectivity:)
# ----------------------------------------------
kubectl get nodes
kubectl get pods -A

# ----------------------------------------------
# STEP 5: Log in to Azure Container Registry (ACR)
# ----------------------------------------------
#Run Docker.desktop app
cd "C:\Users\HP\Documents\M06_SparkBasics_PYTHON_AZURE\docker"
az acr login --name acrdevwesteuropemeim

# ----------------------------------------------
# STEP 6: Build Docker image locally
# ----------------------------------------------
cd "C:\Users\HP\Documents\M06_SparkBasics_PYTHON_AZURE\docker"
wsl dos2unix ./entrypoint.sh

docker build -t spark-python-06:latest .

#docker build -t acrdevwesteuropemeim.azurecr.io/spark-python-06:latest .

# ----------------------------------------------
# STEP 7: Tag Docker image for ACR
# ----------------------------------------------
docker tag spark-python-06:latest acrdevwesteuropemeim.azurecr.io/spark-python-06:latest

# ----------------------------------------------
# STEP 8: Push Docker image to ACR
# ----------------------------------------------
docker push acrdevwesteuropemeim.azurecr.io/spark-python-06:latest

#increase nodes to 2
az aks nodepool scale `
  --resource-group rg-dev-westeurope-meim `
  --cluster-name aks-dev-westeurope-meim `
  --name default `
  --node-count 2


# ----------------------------------------------
# STEP 8.2: Set environment variables
# ----------------------------------------------

$env:AZURE_STORAGE_KEY = ""
$env:OPENCAGE_API_KEY=""

# ----------------------------------------------
# STEP 9: Submit Spark job to AKS
# ----------------------------------------------
spark-submit `
  --master k8s://https://bdccdev-6qwtltcj.hcp.westeurope.azmk8s.io:443 `
  --deploy-mode cluster `
  --name sparkbasics `
  --conf spark.kubernetes.container.image=acrdevwesteuropemeim.azurecr.io/spark-python-06:latest `
  --conf spark.kubernetes.namespace=default `
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark `
  --conf spark.kubernetes.executor.instances=1 `
  --conf spark.kubernetes.driver.request.memory=500m `
  --conf spark.kubernetes.executor.request.memory=500m `
  --conf spark.kubernetes.executor.request.cores=500m `
  --conf spark.pyspark.python=python3 `
  --conf spark.pyspark.driver.python=python3 `
  --conf spark.kubernetes.file.upload.path=wasbs://output@stdevwesteuropemeim.blob.core.windows.net/ `
  --py-files local:///opt/sparkbasics-1.0.0-py3.10.egg `
  local:///opt/src/main/etl_job.py

# ----------------------------------------------
# STEP 11: Monitor Spark job
# ----------------------------------------------
kubectl get pods
# Replace with actual pod name:
kubectl logs <your-driver-pod-name>
kubectl describe pod <your-driver-pod-name>

#kubectl logs sparkbasics-2f73c3963f6c7d40-driver


# ----------------------------------------------
# STEP 12: Check output in Azure Blob Storage
# ----------------------------------------------
az storage blob list `
  --account-name stdevwesteuropemeim `
  --container-name output `
  --auth-mode login `
  --output table

# ----------------------------------------------
# STEP 13: List Docker images in ACR
# ----------------------------------------------
az acr repository list --name acrdevwesteuropemeim --output table
az acr repository show-tags --name acrdevwesteuropemeim --repository spark-python-06 --output table
