[
    "set -a",
    "AD_APP_CERT_DSN=" + (@uri "sqlserver://\(.app_sp_client_id.value)%40\(.tenant_id.value):\(.app_sp_client_secret.value)@\(.sql_server_fqdn.value)?database=\(.sql_database_name.value)&encrypt=true&fedauth=ActiveDirectoryApplication&clientcertpath=\($certpath)" | @sh),
    "AD_APP_PWD_DSN=" + (@uri "sqlserver://\(.app_sp_client_id.value)%40\(.tenant_id.value):\(.app_sp_client_secret.value)@\(.sql_server_fqdn.value)?database=\(.sql_database_name.value)&encrypt=true&fedauth=ActiveDirectoryApplication" | @sh),
    "AD_MSI_SYS_DSN=" + (@uri "sqlserver://\(.sql_server_fqdn.value)?database=\(.sql_database_name.value)&encrypt=true&fedauth=ActiveDirectoryMSI" | @sh),
    "AD_MSI_USER_DSN=" + (@uri "sqlserver://\(.user_assigned_identity_client_id.value)@\(.sql_server_fqdn.value)?database=\(.sql_database_name.value)&encrypt=true&fedauth=ActiveDirectoryMSI" | @sh),
    "AD_USER_PWD_DSN=" + (@uri "sqlserver://\(.sql_ad_admin_user.value):\(.sql_ad_admin_password.value)@\(.sql_server_fqdn.value)?database=\(.sql_database_name.value)&encrypt=true&fedauth=ActiveDirectoryPassword" | @sh),
    "SQL_USER_PWD_DSN=" + (@uri "sqlserver://\(.sql_admin_user.value):\(.sql_admin_password.value)@\(.sql_server_fqdn.value)?database=\(.sql_database_name.value)&encrypt=true" | @sh),
    "set +a"
] | map([.]) | .[] | @tsv
