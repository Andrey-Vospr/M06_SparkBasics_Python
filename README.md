https://github.com/Andrey-Vospr/M06_SparkBasics_Python/blob/main/README.MD

# Filename: run_spark_etl.ps1

# ----------------------------------------------
# STEP 0: Variables - customize if needed
# ----------------------------------------------
# Azure resource information
$StorageResourceGroup = "M06_SparkBasic"                
$AKSResourceGroup = "rg-dev-westeurope-meim"             

# Azure credentials
$AzureStorageKey = "......"
$OpenCageApiKey = "......."

# Azure services names
$StorageAccountName = "m06storageaccountbasic"
$AKSClusterName = "aks-dev-westeurope-meim"
$ACRName = "acrdevwesteuropemeim"
$ContainerName = "result"

# Spark and Kubernetes info
$SparkImage = "$ACRName.azurecr.io/spark-python-06:latest"
$K8sApiServer = "bdccdev-6qwtltcj.hcp.westeurope.azmk8s.io"

# ----------------------------------------------
# STEP 1: Log in to Azure
# ----------------------------------------------
az login
az account set --subscription "e685390e-c7a8-49d3-8b16-e950f057a403"

# ----------------------------------------------
# STEP 2: Verify infrastructure
# ----------------------------------------------
az group show --name $StorageResourceGroup
az group show --name $AKSResourceGroup
az aks show --resource-group $AKSResourceGroup --name $AKSClusterName
az acr show --name $ACRName
az storage account show --name $StorageAccountName --resource-group $StorageResourceGroup
az storage account keys list --account-name $StorageAccountName

# ----------------------------------------------
# STEP 3: Get AKS credentials for kubectl
# ----------------------------------------------
az aks get-credentials --resource-group $AKSResourceGroup --name $AKSClusterName

# ----------------------------------------------
# STEP 4: Test Kubernetes access
# ----------------------------------------------
kubectl get nodes
kubectl get pods -A

# ----------------------------------------------
# STEP 5: Log in to ACR
# ----------------------------------------------
az acr login --name $ACRName

# ----------------------------------------------
# STEP 6: Build and push Docker image
# ----------------------------------------------
cd "C:\Users\HP\Documents\M06_SparkBasics_PYTHON_AZURE\docker"
wsl dos2unix ./entrypoint.sh

docker build --no-cache -t spark-python-06:latest .
docker tag spark-python-06:latest $SparkImage
docker push $SparkImage

# ----------------------------------------------
# STEP 7: Scale AKS nodepool if needed (optional)
# ----------------------------------------------
# az aks nodepool scale --resource-group $AKSResourceGroup --cluster-name $AKSClusterName --name default --node-count 2

# ----------------------------------------------
# STEP 8: Set environment variables
# ----------------------------------------------
$env:AZURE_STORAGE_KEY = $AzureStorageKey
$env:OPENCAGE_API_KEY = $OpenCageApiKey

# ----------------------------------------------
# STEP 9: Create Azure Storage Secret in Kubernetes
# ----------------------------------------------
kubectl delete secret azure-storage-secret --ignore-not-found
kubectl create secret generic azure-storage-secret --from-literal=key1=$env:AZURE_STORAGE_KEY

# ----------------------------------------------
# STEP 10: Delete old Driver Pods (optional but recommended)
# ----------------------------------------------
kubectl delete pod -l spark-role=driver --ignore-not-found

# ----------------------------------------------
# STEP 11: Submit Spark job
# ----------------------------------------------
spark-submit `
  --master k8s://https://$K8sApiServer:443 `
  --deploy-mode cluster `
  --name sparkbasics `
  --conf spark.kubernetes.container.image=$SparkImage `
  --conf spark.kubernetes.namespace=default `
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark `
  --conf spark.hadoop.fs.azure.account.key.$StorageAccountName.blob.core.windows.net=$env:AZURE_STORAGE_KEY `
  --conf spark.kubernetes.driver.secretKeyRef.AZURE_STORAGE_KEY=azure-storage-secret:key1 `
  --conf spark.kubernetes.executor.secretKeyRef.AZURE_STORAGE_KEY=azure-storage-secret:key1 `
  --conf spark.kubernetes.file.upload.path=wasbs://$ContainerName@$StorageAccountName.blob.core.windows.net/ `
  --conf spark.pyspark.python=python3 `
  --conf spark.pyspark.driver.python=python3 `
  --conf spark.kubernetes.executor.instances=1 `
  --conf spark.kubernetes.driver.request.memory=500m `
  --conf spark.kubernetes.executor.request.memory=500m `
  --conf spark.kubernetes.executor.request.cores=500m `
  --py-files local:///opt/sparkbasics-1.0.0-py3.10.egg `
  local:///opt/src/etl_job.py

# ----------------------------------------------
# STEP 12: Monitor Spark job
# ----------------------------------------------
kubectl get pods -A

# ----------------------------------------------
# STEP 13: Check output in Azure Blob Storage
# ----------------------------------------------
az storage blob list --account-name $StorageAccountName --container-name $ContainerName --auth-mode login --output table

# ----------------------------------------------
# STEP 14: Verify Docker images in ACR
# ----------------------------------------------
az acr repository list --name $ACRName --output table
az acr repository show-tags --name $ACRName --repository spark-python-06 --output table