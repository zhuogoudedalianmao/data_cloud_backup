#!/usr/bin/env python
# -*- coding:utf-8 -*-
__author__ = 'dalianmao'

mysql_user_name = 'bak'
mysql_password = 'axfchonga'
back_up_dir = '/backup'
date_format_string = '%Y-%m-%d_%H'
date_string = '%Y-%m-%d'
full_amount_backup_dir_name = 'full_amount_backup'
incremental_backup_dir_name = 'incremental_backup'
upload_path = '/'
upload_source_path = '/backup_zip'
ftp_user_name = 'ftp'
ftp_password = 'ftp'
ftp_host = ''
log_path = 'log'
save_uploaded_files = 5
keep_days = 5
access_id = 'LTAIQWNIK62nQkL8'
access_secret = 'RZCGydooZKA2tivwUHbl09DXn59QDS'
endpoint = 'oss-cn-shenzhen.aliyuncs.com'
bucket_name = 'syanzi'
remote_keep_days = 30

full_amount_backup_time = {'minute': 33,'hour': 18}
incremental_backup_time = {'minutes': 1}
upload_backup_file_time = {'minutes': 1}
clear_ftp_time = {'minutes': 200}

