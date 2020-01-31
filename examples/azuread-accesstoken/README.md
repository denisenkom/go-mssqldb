## Azure Managed Identity example

This example shows how Azure Managed Identity can be used to access SQL Azure. Take note of the
trust boundary before using MSI to prevent exposure of the tokens outside of the trust boundary.

This example can only be run from a Azure Virtual Machine with Managed Identity configured.
You can follow the steps from this tutorial to turn on managed identity for your VM and grant the
VM access to a SQL Azure database:
https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/tutorial-windows-vm-access-sql
