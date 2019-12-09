# Convert Terraform settings to shell environment exports.
[
    "set -a",
    "SQL_SERVER=" + (.sql_server_fqdn.value | @sh),
    "SQL_ADMIN_USER=" + (.sql_admin_user.value | @sh),
    "SQL_ADMIN_PASSWORD=" + (.sql_admin_password.value | @sh),
    "SQL_AD_ADMIN_USER=" + (.sql_ad_admin_user.value | @sh),
    "SQL_AD_ADMIN_PASSWORD=" + (.sql_ad_admin_password.value | @sh),
    "APP_SP_CLIENT_ID=" + (.app_sp_client_id.value | @sh),
    "APP_SP_CLIENT_SECRET=" + (.app_sp_client_secret.value | @sh),
    "SQL_DATABASE=" + (.sql_database_name.value | @sh),
    "APP_NAME=" + (.app_name.value | @sh),
    "VM_NAME=" + (.vm_name.value | @sh),
    "VM_CLIENT_ID=" + (.vm_client_id.value | @sh),
    "UA_NAME=" + (.user_assigned_identity_name.value | @sh),
    "UA_CLIENT_ID=" + (.user_assigned_identity_client_id.value | @sh),
    "AZURE_SUBSCRIPTION_ID=" + (.subscription_id.value | @sh),
    "AZURE_TENANT_ID=" + (.tenant_id.value | @sh),
    "set +a"
] | map([.]) | .[] | @tsv
