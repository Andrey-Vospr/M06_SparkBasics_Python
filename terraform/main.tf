# Setup azurerm as a state backend
terraform {
  backend "azurerm" {
    resource_group_name  = "M06_SparkBasic"
    storage_account_name = "m06storageaccountbasic" # Provide Storage Account name, where Terraform Remote state is stored
    container_name       = "tfstate"
    key                  = "terraform.tfstate"
  }
}

# Configure the Microsoft Azure Provider
provider "azurerm" {
  features {}
  subscription_id = "e685390e-c7a8-49d3-8b16-e950f057a403"
}

provider "kubernetes" {
  host                   = azurerm_kubernetes_cluster.bdcc.kube_config.0.host
  client_certificate     = base64decode(azurerm_kubernetes_cluster.bdcc.kube_config.0.client_certificate)
  client_key             = base64decode(azurerm_kubernetes_cluster.bdcc.kube_config.0.client_key)
  cluster_ca_certificate = base64decode(azurerm_kubernetes_cluster.bdcc.kube_config.0.cluster_ca_certificate)
}

resource "random_string" "suffix" {
  length  = 4
  special = false
  upper   = false
}

locals {
  acr_full_name = "acr${var.ENV}${var.LOCATION}${random_string.suffix.result}"
}

data "azurerm_client_config" "current" {}

resource "azurerm_resource_group" "bdcc" {
  name     = "rg-${var.ENV}-${var.LOCATION}-${random_string.suffix.result}"
  location = var.LOCATION

  lifecycle {
    prevent_destroy = false
  }

  tags = {
    region = var.BDCC_REGION
    env    = var.ENV
  }
}

resource "azurerm_storage_account" "bdcc" {
  depends_on = [
  azurerm_resource_group.bdcc]

  name                     = "st${var.ENV}${var.LOCATION}${random_string.suffix.result}"
  resource_group_name      = azurerm_resource_group.bdcc.name
  location                 = azurerm_resource_group.bdcc.location
  account_tier             = "Standard"
  account_replication_type = var.STORAGE_ACCOUNT_REPLICATION_TYPE
  is_hns_enabled           = "true"

  network_rules {
    default_action = "Allow"
    ip_rules       = values(var.IP_RULES)
  }

  lifecycle {
    prevent_destroy = false
  }

  tags = {
    region = var.BDCC_REGION
    env    = var.ENV
  }
}

resource "azurerm_storage_data_lake_gen2_filesystem" "gen2_data" {
  depends_on = [
  azurerm_storage_account.bdcc]

  name               = "data"
  storage_account_id = azurerm_storage_account.bdcc.id

  lifecycle {
    prevent_destroy = false
  }
}


resource "azurerm_kubernetes_cluster" "bdcc" {
  depends_on = [
  azurerm_resource_group.bdcc]

  name                = "aks-${var.ENV}-${var.LOCATION}-${random_string.suffix.result}"
  location            = azurerm_resource_group.bdcc.location
  resource_group_name = azurerm_resource_group.bdcc.name
  dns_prefix          = "bdcc${var.ENV}"

  default_node_pool {
    name       = "default"
    node_count = 1
    vm_size    = "Standard_B2ms" 
    temporary_name_for_rotation = "tempnp"
  }

  identity {
    type = "SystemAssigned"
  }

  tags = {
    region = var.BDCC_REGION
    env    = var.ENV
  }
}

# Create Azure Container Registry (ACR)
resource "azurerm_container_registry" "acr" {
  name                = local.acr_full_name
  resource_group_name = azurerm_resource_group.bdcc.name
  location            = azurerm_resource_group.bdcc.location
  sku                 = var.ACR_SKU
  admin_enabled       = false

  tags = {
    region = var.BDCC_REGION
    env    = var.ENV
  }
}

resource "kubernetes_service_account" "spark" {
  metadata {
    name      = "spark"
    namespace = "default"
  }
  depends_on = [azurerm_kubernetes_cluster.bdcc] # Ensure AKS is provisioned first
}

resource "kubernetes_cluster_role" "spark_role" {
  metadata {
    name = "spark-role"
  }

  rule {
    api_groups = [""]
    resources  = ["pods", "pods/log", "services", "configmaps", "persistentvolumeclaims", "secrets"]
    verbs      = ["create", "get", "list", "watch", "delete", "deletecollection"]
  }

  rule {
    api_groups = [""]
    resources  = ["events"]
    verbs      = ["get", "list", "watch"]
  }

  rule {
    api_groups = ["batch"]
    resources  = ["jobs"]
    verbs      = ["create", "delete", "get", "list", "watch"]
  }

  rule {
    api_groups = ["apps"]
    resources  = ["deployments"]
    verbs      = ["create", "delete", "get", "list", "watch"]
  }
  depends_on = [azurerm_kubernetes_cluster.bdcc]
}

resource "kubernetes_cluster_role_binding" "spark_role_binding" {
  metadata {
    name = "spark-role-binding"
  }

  subject {
    kind      = "ServiceAccount"
    name      = "spark"
    namespace = "default"
  }

  role_ref {
    kind      = "ClusterRole"
    name      = "spark-role"
    api_group = "rbac.authorization.k8s.io"
  }
  depends_on = [kubernetes_service_account.spark, kubernetes_cluster_role.spark_role]
}

# Assign AcrPull role to AKS so it can pull images from ACR
resource "azurerm_role_assignment" "aks_acr_pull" {
  principal_id         = azurerm_kubernetes_cluster.bdcc.kubelet_identity[0].object_id
  role_definition_name = "AcrPull"
  scope               = azurerm_container_registry.acr.id
}

output "client_certificate" {
  value = azurerm_kubernetes_cluster.bdcc.kube_config.0.client_certificate
  sensitive = true
}

output "aks_kubeconfig" {
  sensitive = true
  value     = azurerm_kubernetes_cluster.bdcc.kube_config_raw
}

output "acr_login_server" {
  value       = azurerm_container_registry.acr.login_server
  description = "The login server of the Azure Container Registry."
}

output "aks_api_server_url" {
  sensitive = true
  description = "The Kubernetes API server endpoint for AKS."
  value       = azurerm_kubernetes_cluster.bdcc.kube_config.0.host
}

output "resource_group_name" {
  description = "The name of the created Azure Resource Group."
  value       = azurerm_resource_group.bdcc.name
}

output "storage_account_name" {
  description = "The name of the created Azure Storage Account."
  value       = azurerm_storage_account.bdcc.name
}

output "aks_name" {
  description = "The name of the AKS cluster"
  value       = azurerm_kubernetes_cluster.bdcc.name
}
