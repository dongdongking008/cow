module cow

go 1.14

require (
	github.com/aead/chacha20 v0.0.0-20180709150244-8b13a72661da // indirect
	github.com/cyfdecyf/bufio v0.0.0-20130801052708-9601756e2a6b
	github.com/cyfdecyf/color v0.0.0-20130827105946-31d518c963d2
	github.com/cyfdecyf/leakybuf v0.0.0-20140618011800-ffae040843be
	github.com/go-redis/redis/v7 v7.0.0-beta.4
	github.com/go-redis/redis_rate/v8 v8.0.0
	github.com/prometheus/client_golang v1.5.1
	github.com/shadowsocks/shadowsocks-go v0.0.0-20190614083952-6a03846ca9c0
	golang.org/x/crypto v0.0.0-20200302210943-78000ba7a073
	golang.org/x/sys v0.0.0-20200122134326-e047566fdf82
)

replace (
	golang.org/x/crypto v0.0.0-20200302210943-78000ba7a073 => github.com/golang/crypto v0.0.0-20200302210943-78000ba7a073
	golang.org/x/sys v0.0.0-20190412213103-97732733099d => github.com/golang/sys v0.0.0-20190412213103-97732733099d
)
