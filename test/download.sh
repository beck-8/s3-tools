#!/usr/bin/env bash 
export dst_endpoint=http://127.0.0.1:9000
export dst_ak=minioadmin
export dst_sk=minioadmin
export dst_bucket=test
export dst_prefix=dddd/
export concurrent=5
export filelist=download.txt

../s3-migrate download