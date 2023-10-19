#!/usr/bin/env bash 
export src_endpoint=http://127.0.0.1:9000
export src_ak=minioadmin
export src_sk=minioadmin
export src_bucket=test
export src_prefix=cmd
export dst_endpoint=http://127.0.0.1:9000
export dst_ak=minioadmin
export dst_sk=minioadmin
export dst_bucket=test2
export dst_prefix=
export watch=1
export concurrent=1
export filelist=filelist.txt
./s3-migrate