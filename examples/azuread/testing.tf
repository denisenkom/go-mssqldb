#
# Terraform setup for Azure SQL with Azure Active Directory authentication
#

#
# Set up Terraform provider versions
#

terraform {
  required_providers {
    azuread = {
      source  = "hashicorp/azuread"
      version = "=1.1.1"
    }

    azurerm = {
      source = "hashicorp/azurerm"
      version = "=2.40.0"
    }

    http = {
      source = "hashicorp/http"
      version = "=2.0.0"
    }

    random = {
      version = "=3.0.0"
    }

    tls = {
      version = "=3.0.0"
    }
  }
}

provider "azurerm" {
  features {}
}

#
# Variables
#
# These variables allow limited overrides to control the resource creation.
# To specify, run terraform apply -var name1=value1 [-var name2=value2]...
# E.g. terraform apply -var prefix=my-stuff 
# will use "my-stuff" in place of the randomly generated ID that is used by default.
#
variable "prefix" {
  description = "Prefix for Azure resource names"
  type = string
  default = ""
}

variable "location" {
  description = "Azure location for resources"
  type = string
  default = "East US"
}

variable "vm_admin_name" {
  description = "Name of administrative user on virtual machine"
  type = string
  default = "azureuser"
}

variable "ssh_key" {
  description = "Path to RSA SSH private key (unencrypted)"
  type = string
  default = "~/.ssh/id_rsa"
}

variable "workstation_ip" {
  description = "IP address of this workstation to add to SQL server firewall rules"
  type = string
  default = ""
}

#
# If the prefix is not specified via the variable, a sixteen character alphanumeric suffix is
# generated and then the prefix is set to "go-mssql-test-" + <random string>
#
resource "random_string" "random_prefix" {
  length = 16
  lower = true
  number = true
  upper = false
  special = false
}

#
# Set up a local variable to capture the prefix to use - either the user-specified from the
# variable, or else the generated name using the random string above.
#
# Some resource names (e.g. SQL server) are more restricted than others - e.g. hyphens are
# not permitted - so we create a restricted name prefix as well as a regular name prefix.
#
locals {
  regular_name_prefix = var.prefix != "" ? var.prefix : "go-mssql-test-${random_string.random_prefix.result}"
  restricted_name_prefix = var.prefix != "" ? lower(replace(var.prefix, "/[^A-Za-z0-9]/", "")) : "gomssqltest${random_string.random_prefix.result}"
}

#
# SSH Key - generate if not available at the file named in the variable.
# Terraform will complain if var.ssh_key is empty as this is interpreted as referring to the
# current working directory, and that is not a file. Instead, if you want to avoid using an
# existing SSH key, make it a literal "no" or some other string that is not an existing file or
# directory.
#
data "tls_public_key" "file_ssh_key" {
  count            = fileexists(var.ssh_key) ? 1 : 0
  private_key_pem  = fileexists(var.ssh_key) ? file(var.ssh_key) : ""
}

resource "tls_private_key" "rand_ssh_key" {
  algorithm   = "ECDSA"
}

locals {
  private_key_pem     = fileexists(var.ssh_key) ? data.tls_public_key.file_ssh_key.0.private_key_pem : tls_private_key.rand_ssh_key.private_key_pem
  public_key_pem      = fileexists(var.ssh_key) ? data.tls_public_key.file_ssh_key.0.public_key_pem : tls_private_key.rand_ssh_key.public_key_pem
  public_key_openssh  = fileexists(var.ssh_key) ? data.tls_public_key.file_ssh_key.0.public_key_openssh : tls_private_key.rand_ssh_key.public_key_openssh
}

#
# Retrieve tenant, subscription and default domain information based on the current Azure login.
#
data "azurerm_client_config" "current" {
}

data "azurerm_subscription" "current" {
}

data "azuread_domains" "current" {
  only_default = "true"
}

#
# Use ipify.org to determine workstation IP if not provided.
# If this guesses incorrectly, specify your workstation IP with -var worstation_ip=<your-IP>
# when you run terraform apply.
#
data "http" "workstation_ip" {
  url = "https://api.ipify.org/"
}

locals {
  workstation_ip = var.workstation_ip != "" ? var.workstation_ip : chomp(data.http.workstation_ip.body)
}

#
# Set up the Azure resource group for all the test resources.
#
resource "azurerm_resource_group" "rg" {
  name     = "${local.regular_name_prefix}-rg"
  location = var.location
}

#
# Set up an AD User to use as AD Administrator for the Azure SQL server.
#
# Using a regular user account makes it simpler to log in as the user with SSMS or the Go
# driver when setting up the other permissions for the identities that will be tested.
# It appears to although you can make the AD Administrator a service principal, doing so
# leads to issues during logins that do not occur when the AD Administrator is a normal
# AD User account.
#
resource "random_password" "sql_ad_admin_sp_password" {
  length = 32
  special = true
}

resource "azuread_user" "sql_ad_admin" {
  user_principal_name = "SQLAdmin.${local.restricted_name_prefix}@${data.azuread_domains.current.domains[0].domain_name}"
  display_name        = "SQL Admin for ${local.restricted_name_prefix}"
  mail_nickname       = "SQLAdmin.${local.restricted_name_prefix}"
  password            = random_password.sql_ad_admin_sp_password.result
}

#
# Set up the Azure SQL Server
#
# A normal (non-AD) administrator username and password are also provisioned. However, it is
# not possible to create AD users without logging in via an AD-authenticated account, so this
# non-AD administrator is not able to create new AD user accounts.
#
resource "random_password" "sql_admin_password" {
  length = 16
  special = true
}

resource "azurerm_sql_server" "sql_server" {
  name                     = local.restricted_name_prefix
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  
  version                  = "12.0"
  administrator_login      = "sql-admin"
  administrator_login_password = random_password.sql_admin_password.result
}

resource "azurerm_sql_active_directory_administrator" "sql_server" {
  server_name         = azurerm_sql_server.sql_server.name
  resource_group_name = azurerm_sql_server.sql_server.resource_group_name
  login               = "sql-ad-admin"
  tenant_id           = data.azurerm_client_config.current.tenant_id
  object_id           = azuread_user.sql_ad_admin.id
}

resource "azurerm_sql_firewall_rule" "sql_server_allow_azure" {
  name                = "AllowAzureAccess"
  server_name         = azurerm_sql_server.sql_server.name
  resource_group_name = azurerm_sql_server.sql_server.resource_group_name
  start_ip_address    = "0.0.0.0"
  end_ip_address      = "0.0.0.0"
}

resource "azurerm_sql_firewall_rule" "sql_server_allow_workstation" {
  name                = "AllowWorkstationAccess"
  server_name         = azurerm_sql_server.sql_server.name
  resource_group_name = azurerm_sql_server.sql_server.resource_group_name
  start_ip_address    = local.workstation_ip
  end_ip_address      = local.workstation_ip
}

#
# Set up the test database on the Azure SQL server
#
resource "azurerm_sql_database" "sql_db" {
  name                = "go-mssqldb"
  
  server_name         = azurerm_sql_server.sql_server.name
  resource_group_name = azurerm_sql_server.sql_server.resource_group_name
  location            = azurerm_sql_server.sql_server.location

  requested_service_objective_name = "S0"
}

#
# Create a service principal that will be granted access to the database,
# representing an application login to the database.
#
resource "azuread_application" "app" {
  name                  = "${local.regular_name_prefix}-app"
}

resource "azuread_service_principal" "app_sp" {
  application_id               = azuread_application.app.application_id
  app_role_assignment_required = false
}

resource "random_password" "app_sp_password" {
  length = 32
  special = true
}

resource "azuread_service_principal_password" "app_sp" {
  service_principal_id  = azuread_service_principal.app_sp.id
  value                 = random_password.app_sp_password.result
  end_date_relative     = "8760h"
}


#
# Create a user-assigned identity that we will add to the VM in addition to the
# system-assigned identity.
#
resource "azurerm_user_assigned_identity" "vm_user_id" {
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location

  name = "${local.restricted_name_prefix}-user-id"
}

#
# Create an Azure VM for testing managed identity authentication.
#
# To support the Azure VM, we need a virtual network, a subnet, the public IP, the network
# security group, and the network interface. The network security group allows incoming SSH
# from the anywhere on the internet.
#
resource "azurerm_virtual_network" "vm_vnet" {
  name                = "${local.regular_name_prefix}-vnet"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  address_space       = ["10.0.0.0/16"]
}

resource "azurerm_subnet" "vm_subnet" {
  name                 = "${local.regular_name_prefix}-vm-sn"
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.vm_vnet.name
  address_prefixes     = ["10.0.2.0/24"]
}

resource "azurerm_public_ip" "vm_ip" {
  name                    = "${local.regular_name_prefix}-vm-ip"
  resource_group_name     = azurerm_resource_group.rg.name
  location                = azurerm_resource_group.rg.location
  allocation_method       = "Dynamic"
  idle_timeout_in_minutes = 30
}

resource "azurerm_network_security_group" "vm_nsg" {
  name                = "${local.regular_name_prefix}-vm-nsg"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
    
  security_rule {
      name                       = "SSH"
      priority                   = 1001
      direction                  = "Inbound"
      access                     = "Allow"
      protocol                   = "Tcp"
      source_port_range          = "*"
      destination_port_range     = "22"
      source_address_prefix      = "*"
      destination_address_prefix = "*"
  }
}

resource "azurerm_network_interface" "vm_nic" {
  name                        = "${local.regular_name_prefix}-vm-nic"
  resource_group_name         = azurerm_resource_group.rg.name
  location                    = azurerm_resource_group.rg.location

  ip_configuration {
    name                          = "${local.regular_name_prefix}-vm-nic-config"
    subnet_id                     = azurerm_subnet.vm_subnet.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.vm_ip.id
  }
}

resource "azurerm_network_interface_security_group_association" "vm_nic_nsg" {
  network_interface_id      = azurerm_network_interface.vm_nic.id
  network_security_group_id = azurerm_network_security_group.vm_nsg.id
}

#
# Given the networking setup, now create the Azure VM
#
resource "azurerm_virtual_machine" "vm" {
  name                  = "${local.regular_name_prefix}-vm"
  resource_group_name   = azurerm_resource_group.rg.name
  location              = azurerm_resource_group.rg.location
  network_interface_ids = [azurerm_network_interface.vm_nic.id]
  vm_size               = "Standard_B1s"

  storage_os_disk {
    name              = "${local.regular_name_prefix}-vm-os"
    caching           = "ReadWrite"
    create_option     = "FromImage"
    managed_disk_type = "Standard_LRS"
  }

  storage_image_reference {
    publisher = "Canonical"
    offer     = "UbuntuServer"
    sku       = "18.04-LTS"
    version   = "latest"
  }

  os_profile {
    computer_name  = "${local.regular_name_prefix}-vm"
    admin_username = var.vm_admin_name
  }

  os_profile_linux_config {
    disable_password_authentication = true
    ssh_keys {
      path     = "/home/${var.vm_admin_name}/.ssh/authorized_keys"
      key_data = local.public_key_openssh
    }
  }

  # Configure the VM with both SystemAssigned and a UserAssigned identity
  identity {
    type          = "SystemAssigned, UserAssigned"
    identity_ids  = [azurerm_user_assigned_identity.vm_user_id.id]
  }
}

# Retrieve the application ID corresponding to the service principal ID assigned to the VM.
data "azuread_service_principal" "vm_sp" {
  object_id = azurerm_virtual_machine.vm.identity.0.principal_id
}

# Wait for public IP to be assigned after VM is created so we can report it in the outputs.
data "azurerm_public_ip" "vm_ip" {
  name                = azurerm_public_ip.vm_ip.name
  resource_group_name = azurerm_virtual_machine.vm.resource_group_name
}

#
# After provisioning or refreshing, Terraform will populate these outputs.
# These capture the necessary pieces of information to access the new infrastructure.
#
output "tenant_id" {
  description = "Azure tenant ID"
  value = data.azurerm_client_config.current.tenant_id
}

output "subscription_id" {
  description = "Azure subscription ID"
  value = data.azurerm_client_config.current.subscription_id
}

output "sql_server_name" {
  description = "Azure SQL server name"
  value = azurerm_sql_server.sql_server.name
}

output "sql_server_fqdn" {
  description = "Azure SQL server domain name"
  value = azurerm_sql_server.sql_server.fully_qualified_domain_name
}

output "sql_ad_admin_user" {
  description = "Azure SQL administrator name (AD authentication)"
  value = azuread_user.sql_ad_admin.user_principal_name
}

output "sql_ad_admin_password" {
  description = "Azure SQL administrator password (AD authentication)"
  value = random_password.sql_ad_admin_sp_password.result
  sensitive = true
}

output "sql_admin_user" {
  description = "Azure SQL administrator name (SQL server authentication)"
  value = azurerm_sql_server.sql_server.administrator_login
}

output "sql_admin_password" {
  description = "Azure SQL administrator password (SQL server authentication)"
  value = random_password.sql_admin_password.result
  sensitive = true
}

output "sql_database_name" {
  description = "Azure SQL database name"
  value = azurerm_sql_database.sql_db.name
}

output "vm_name" {
  description = "Azure virtual machine name"
  value = azurerm_virtual_machine.vm.name
}

output "vm_client_id" {
  description = "Azure VM system-assigned identity client ID"
  value = data.azuread_service_principal.vm_sp.application_id
}

output "vm_principal_id" {
  description = "Azure VM system-assigned identity principal ID"
  value = azurerm_virtual_machine.vm.identity.0.principal_id
}

output "vm_ip_address" {
  description = "Azure virtual machine public IP"
  value = data.azurerm_public_ip.vm_ip.ip_address
}

output "vm_admin_name" {
  description = "Azure virtual machine admin user name"
  value = var.vm_admin_name
}

output "vm_user_ssh_private_key" {
  description = "Azure virtual machine admin user private SSH key"
  value = local.private_key_pem
  sensitive = true
}

output "vm_user_ssh_openssh_key" {
  description = "Azure virtual machine admin user SSH public key"
  value = local.public_key_openssh
  sensitive = true
}

output "app_sp_client_id" {
  description = "Service principal client ID for application user"
  value = azuread_application.app.application_id
}

output "app_name" {
  description = "Service principal name for application user"
  value = azuread_application.app.name
}

output "app_sp_client_secret" {
  description = "Service principal client secret for application user"
  value = random_password.app_sp_password.result
  sensitive = true
}

output "user_assigned_identity_name" {
  description = "User-assigned identity for the Azure virtual machine"
  value = azurerm_user_assigned_identity.vm_user_id.name
}

output "user_assigned_identity_client_id" {
  description = "User-assigned identity client ID"
  value = azurerm_user_assigned_identity.vm_user_id.client_id
}
