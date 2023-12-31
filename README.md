# s3-tools
## 支持功能
> ~~使用域名可自动解析成IPV4随机访问。只允许域名访问的endpoint应使用`--disable_lookup`禁用此功能（公网S3服务大部分都需要加此参数，否则会返回乱七八糟的错误）~~ 已修改，此参数删除

- 支持 s3 到 s3 迁移
- 支持增量迁移（--watch）
- 支持文件列表迁移（--filelist）
- 删除源数据（默认关闭）
- 更改unsealed索引（默认关闭，启动需配置全参数）
- 支持从 http/https 下载到s3
- 支持从本地文件系统上传文件到 s3

## Usage
```
$ ./s3-tools -h
NAME:
   s3-tools - s3 tools

USAGE:
   s3-tools [global options] command [command options] [arguments...]

VERSION:
   0.0.1+git.0ff5473f14

COMMANDS:
   migrate   s3 to s3 migrate
   download  from http[s] download to s3
   help, h   Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --help, -h     show help
   --version, -v  print the version
```
```
$ ./s3-tools migrate -h
NAME:
   s3-tools migrate - s3 to s3 migrate

USAGE:
   
   src_endpoint and dst_endpoint must use type scheme://domain[:port], example http://example.com[:80]


OPTIONS:
   --src_endpoint value     [$src_endpoint]
   --src_ak value           [$src_ak]
   --src_sk value           [$src_sk]
   --src_bucket value       [$src_bucket]
   --src_prefix value       [$src_prefix]
   --dst_endpoint value     [$dst_endpoint]
   --dst_ak value           [$dst_ak]
   --dst_sk value           [$dst_sk]
   --dst_bucket value       [$dst_bucket]
   --dst_prefix value       [$dst_prefix]
   --filelist value        specify the list to be migrated, one object per line [$filelist]
   --PartSize value        (default: "16MiB") [$PartSize]
   --NumThreads value      (default: 4) [$NumThreads]
   --EnableMemCache        after turning it on, it will obviously occupy memory. PartSize*NumThreads (default: false) [$EnableMemCache]
   --DisableMultipart      (default: true) [$DisableMultipart]
   --DisableContentSha256  (default: true) [$DisableContentSha256]
   --concurrent value      (default: 10) [$concurrent]
   --watch                 loop to check if there is new data (default: false) [$watch]
   --remove                delete after completion (default: false) [$remove]
   --src_uuid value        src storage uuid [$src_uuid]
   --dst_uuid value        dst storage uuid [$dst_uuid]
   --rpc value             miner rpc, http://localhost:2345/rpc/v0 [$rpc]
   --token value           miner admin token [$token]
   --help, -h              show help
```
- 以下参数仅在网络质量不好且文件较大时尝试调整使用。
- 理论内网环境使用不调整则是最优。
- 如果数据大小大于目标集群part上限，需要开启分片上传。
```bash
   --PartSize value        (default: "16MiB") [$PartSize]
   --NumThreads value      (default: 4) [$NumThreads]
   --EnableMemCache        after turning it on, it will obviously occupy memory. PartSize*NumThreads (default: false) [$EnableMemCache]
   --DisableMultipart      (default: true) [$DisableMultipart]
   --DisableContentSha256  (default: true) [$DisableContentSha256]
```
```
$ ./s3-tools download -h
NAME:
   s3-tools download - from http[s] download to s3

USAGE:
   s3-tools download [command options] [arguments...]

OPTIONS:
   --dst_endpoint value     [$dst_endpoint]
   --dst_ak value           [$dst_ak]
   --dst_sk value           [$dst_sk]
   --dst_bucket value       [$dst_bucket]
   --dst_prefix value       [$dst_prefix]
   --filelist value        specify the list to be downloaded, one url per line [$filelist]
   --PartSize value        (default: "16MiB") [$PartSize]
   --NumThreads value      (default: 4) [$NumThreads]
   --EnableMemCache        after turning it on, it will obviously occupy memory. PartSize*NumThreads (default: false) [$EnableMemCache]
   --DisableMultipart      (default: true) [$DisableMultipart]
   --DisableContentSha256  (default: true) [$DisableContentSha256]
   --concurrent value      (default: 10) [$concurrent]
   --help, -h              show help
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

# 使用文件列表
# export filelist=filelist.txt

# 迁移后删除源集群数据
# export remove=1

# export src_uuid=
# export dst_uuid=
# export rpc=
# export token=
./s3-tools migrate
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

# 使用文件列表
# export filelist=filelist.txt

# 迁移后删除源集群数据
export remove=1

export src_uuid=
export dst_uuid=
export rpc=
export token=
./s3-tools migrate
```
## 从http/https下载到S3
```
#!/usr/bin/env bash 
export dst_endpoint=http://127.0.0.1:9000
export dst_ak=minioadmin
export dst_sk=minioadmin
export dst_bucket=test
export dst_prefix=dddd/
export concurrent=5
export filelist=download.txt

./s3-tools download
```
## 从本地文件系统上传文件到 s3
```
#!/usr/bin/env bash 
export dst_endpoint=http://127.0.0.1:9000
export dst_ak=minioadmin
export dst_sk=minioadmin
export dst_bucket=test
export dst_prefix=
export concurrent=5
export dir=./
# or
# export filelist=upload.txt

../s3-tools upload
```