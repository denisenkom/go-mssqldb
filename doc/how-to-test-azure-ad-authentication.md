# How to test Azure AD authentication

To test Azure AD authentication requires an Azure SQL server configured with an
[Active Directory administrator](https://docs.microsoft.com/en-us/azure/sql-database/sql-database-aad-authentication-configure).
To test managed identity authentication, an Azure virtual machine configured with
[system-assigned and/or user-assigned identities](https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/qs-configure-portal-windows-vm)
is also required.

The necessary resources can be set up through any means including the
[Azure Portal](https://portal.azure.com/), the Azure CLI, the Azure PowerShell cmdlets or
[Terraform](https://terraform.io/). To support these instructions, use the Terraform script at
[examples/azuread/testing.tf](../examples/azuread/testing.tf).

## Create Azure infrastructure

Download [Terraform](https://terraform.io/) to a location on your PATH.

Log in to Azure using the Azure CLI.

```console
you@workstation:~$ az login
you@workstation:~$ az account show
```

If your Azure account has access to multiple subscriptions, use
`az account set --subscription <name-or-ID>` to choose the correct one. You will need to have at
least Contributor access to the portal and permissions in Azure Active Directory to create users
and grants.

Check out this source repository (if you haven't already), change to the `examples/azuread`
directory and run Terraform:

```console
you@workstation:~$ git clone -b azure-auth https://github.com/wrosenuance/go-mssqldb.git
you@workstation:~$ cd go-mssqldb/examples/azuread
you@workstation:azuread$ terraform init
you@workstation:azuread$ terraform apply
```

This will create an Azure resource group, a SQL server with a database, a virtual machine with a
system-assigned identity and user-assigned identity. Resources are named based on a random
prefix: to specify the prefix, use `terraform apply -var prefix=<alphanumeric-and-hyphens-ok>`.

Upon successful completion, Terraform will display some key details of the infrastructure that has
 been created. This includes the SSH key to access the VM, the administrator account and password
 for the Azure SQL server, and all the relevant resource names.

Save the settings to a JSON file:

```console
you@workstation:azuread$ terraform output -json > settings.json
```

Save the SSH private key to a file:

```console
you@workstation:azuread$ terraform output vm_user_ssh_private_key > ssh-identity
```

Copy the `settings.json` to the new VM:

```console
you@workstation:azuread$ eval "VM_ADMIN_NAME=$(terraform output vm_admin_name)"
you@workstation:azuread$ eval "VM_IP_ADDRESS=$(terraform output vm_ip_address)"
you@workstation:azuread$ scp -i ssh-identity settings.json "${VM_ADMIN_NAME}@${VM_IP_ADDRESS}:"
```

## Set up Azure Virtual Machine for testing

SSH to the new VM to continue setup:

```console
you@workstation:azuread$ ssh -i ssh-identity "${VM_ADMIN_NAME}@${VM_IP_ADDRESS}"
```

Once on the VM, update the system and install some basic packages:

```console
azureuser@azure-vm:~$ sudo apt update -y
azureuser@azure-vm:~$ sudo apt upgrade -y
azureuser@azure-vm:~$ sudo apt install -y git openssl jq build-essential 
azureuser@azure-vm:~$ sudo snap install go --classic
```

Install the Azure CLI using the script as shown below, or follow the
[manual install instructions](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli-apt):

```console
azureuser@azure-vm:~$ curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
```

## Generate service principal certificate file

Log in to Azure on the VM and set the subscription:

```console
azureuser@azure-vm:~$ az login
azureuser@azure-vm:~$ az account set --subscription "$(jq -r '.subscription_id.value' settings.json)"
```

Use OpenSSL to create a new certificate and key in PEM format, using the :

```console
azureuser@azure-vm:~$ openssl rand -writerand ~/.rnd
azureuser@azure-vm:~$ openssl req -x509 -nodes -newkey rsa:4096 -keyout client.key -out client.crt \
                                    -subj "/C=US/ST=MA/L=Boston/O=Global Security/OU=IT Department/CN=AD-SP"
azureuser@azure-vm:~$ openssl rsa -out client.pem -in client.key -aes256 \
                                    -passout "pass:$(jq -r '.app_sp_client_secret.value' settings.json)"
azureuser@azure-vm:~$ cat client.crt >> client.pem
azureuser@azure-vm:~$ export APP_SP_CLIENT_CERT="$PWD/client.pem"
```

Use the Azure CLI to add the client certificate to the application service principal:

```console
azureuser@azure-vm:~$ az ad sp credential reset --append --cert @client.crt \
                                    --name "$(jq -r '.app_sp_client_id.value' settings.json)"
```

## Build source code and authorize users in database

Clone this repository, build and run the `examples/azuread` helper that verifies the database
exists and sets up access for the system-assigned and user-assigned identities.

```console
azureuser@azure-vm:~$ git clone -b azure-auth https://github.com/wrosenuance/go-mssqldb.git
azureuser@azure-vm:~$ cd go-mssqldb
azureuser@azure-vm:go-mssqldb$ (cd ./examples/azuread; go build -o ../../azuread-example .)
azureuser@azure-vm:go-mssqldb$ eval "$(jq -r -f examples/azuread/environment-settings.jq ../settings.json)"
azureuser@azure-vm:go-mssqldb$ ./azuread-example -fedauth ActiveDirectoryPassword
```

For some basic connectivity tests, use the `examples/simple` helper. Run these commands on the
Azure VM so that identity authentication is possible.

```console
azureuser@azure-vm:go-mssqldb$ eval "$(jq -r --arg certpath "$(realpath ../client.pem)" -f examples/azuread/dsn-variables.jq ../settings.json)"
azureuser@azure-vm:go-mssqldb$ go build -o simple ./examples/simple
azureuser@azure-vm:go-mssqldb$ ./simple -debug -dsn "$AD_APP_CERT_DSN"
azureuser@azure-vm:go-mssqldb$ ./simple -debug -dsn "$AD_APP_PWD_DSN"
azureuser@azure-vm:go-mssqldb$ ./simple -debug -dsn "$AD_MSI_SYS_DSN"
azureuser@azure-vm:go-mssqldb$ ./simple -debug -dsn "$AD_MSI_USER_DSN"
azureuser@azure-vm:go-mssqldb$ ./simple -debug -dsn "$AD_USER_PWD_DSN"
azureuser@azure-vm:go-mssqldb$ ./simple -debug -dsn "$SQL_USER_PWD_DSN"
```

## Running the integration tests

Now that your environment is configured, you can run `go test`:

```console
azureuser@azure-vm:go-mssqldb$ export SQLSERVER_DSN="$AD_APP_CERT_DSN"
azureuser@azure-vm:go-mssqldb$ go test -coverprofile=coverage.out . ./azuread ./batch ./internal/...
azureuser@azure-vm:go-mssqldb$ go tool cover -html=coverage.out -o coverage.html
```

## Tear down environment

After you complete your testing, use Terraform to destroy the infrastructure you created.

```console
you@workstation:azuread$ terraform destroy
```

## Troubleshooting

After Terraform runs you should be able to see resources that were created in the
[Azure Portal](https://portal.azure.com/).

If the Azure SQL server is successfully created you can connect to it using the AD admin user
and password in SSMS. SSMS will prompt you to create firewall rules if they are missing. You
can read the AD admin user and password from the `settings.json`, or run:

```console
you@workstation:azuread$ terraform output sql_ad_admin_user
you@workstation:azuread$ terraform output sql_ad_admin_password
```

