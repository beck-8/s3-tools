# s3-migrate
## Usage
```
$ ./s3-migrate -h
NAME:
   s3-migrate - s3 to s3 tools

USAGE:
   
   src_endpoint and dst_endpoint must use type scheme://domain:port, example http://example.com:80


VERSION:
   0.0.1+git.e51ecd0b3b

COMMANDS:
   help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --src_endpoint value   [$src_endpoint]
   --src_ak value         [$src_ak]
   --src_sk value         [$src_sk]
   --src_bucket value     [$src_bucket]
   --src_prefix value     [$src_prefix]
   --dst_endpoint value   [$dst_endpoint]
   --dst_ak value         [$dst_ak]
   --dst_sk value         [$dst_sk]
   --dst_bucket value     [$dst_bucket]
   --dst_prefix value     [$dst_prefix]
   --filelist value      specify the list to be migrated, one object per line [$filelist]
   --concurrent value    (default: 10) [$concurrent]
   --watch               loop to check if there is new data (default: false) [$watch]
   --remove              delete after completion (default: false) [$remove]
   --src_uuid value      src storage uuid [$src_uuid]
   --dst_uuid value      dst storage uuid [$dst_uuid]
   --rpc value           miner rpc, http://localhost:2345/rpc/v0 [$rpc]
   --token value         miner admin token [$token]
   --help, -h            show help
   --version, -v         print the version
```

## s3 迁移到 s3
```
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

# 迁移后删除源集群数据
export remove=1

# export src_uuid=
# export dst_uuid=
# export rpc=
# export token=
./s3-migrate
```

## s3 迁移到 s3, 并修改unsealed 索引
```
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

# 迁移后删除源集群数据
export remove=1

export src_uuid=
export dst_uuid=
export rpc=
export token=
./s3-migrate
```