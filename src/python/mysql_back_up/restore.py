#!/usr/bin/env python
# -*- coding:utf-8 -*-

import oss_task
import config
import oss2

if __name__ == '__main__':
    auth = oss2.Auth(config.access_id, config.access_secret)
    bucket = oss2.Bucket(auth, config.endpoint, config.bucket_name)

    oss_task.restore(bucket)
