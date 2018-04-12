#!/usr/bin/env python
# -*- coding:UTF-8 -*-
import oss2
import config
import sys
import oss_task

if __name__ == '__main__':
    auth = oss2.Auth(config.access_id, config.access_secret)
    bucket = oss2.Bucket(auth, config.endpoint, config.bucket_name)

    if len(sys.argv) != 2:
        print "error - usage:\n $ python main.py full|inc|show|detail|download"
        print "你可以使用help参考简介"
    else:
        if sys.argv[1] == "full":
            oss_task.full_backup(bucket)
        elif sys.argv[1] == "show":
            oss_task.show_bucket_file(bucket)
        elif sys.argv[1] == "detail":
            oss_task.object_dir_detail(bucket)
        elif sys.argv[1] == "inc":
            oss_task.incremental_backup(bucket)
        elif sys.argv[1] == "download":
            oss_task.remote_download_file(bucket)
        elif sys.argv[1] == "verify":
            oss_task.verify_remote_file(bucket)
        elif sys.argv[1] == "help":
            oss_task.help()
        else:
            print "error input, $python oss_task.py full|inc|show|detail|download"
            print "你可以使用help参考简介"
