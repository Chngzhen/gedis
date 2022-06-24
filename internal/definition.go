package internal

import (
	"time"
)

type Property struct {
	Host     string   `yaml:"host"`
	Port     int      `yaml:"port"`
	Database int      `yaml:"database"`
	Password string   `yaml:"password"`
	Cluster  *Cluster `yaml:"cluster"`
}

type Cluster struct {
	MaxRedirects   int  `yaml:"max-redirects"`
	ReadOnly       bool `yaml:"read-only"`
	RouteByLatency bool `yaml:"route-by-latency"`
	RouteRandomly  bool `yaml:"route-randomly"`
	// 最大连接数。默认：5 * runtime.NumCPU。
	PoolSize int `yaml:"pool-size"`
	// 初始并允许空闲的连接数量。
	MinIdleConns int `yaml:"min-idle-conns"`
	// 命令执行失败时的最大重试次数。为0则不重试。默认：0。
	MaxRetries int `yaml:"max-retries"`
	// 每次计算重试间隔时间的下限。为-1则取消间隔。默认：8 * time.Millisecond。
	MinRetryBackoff time.Duration `yaml:"min-retry-backoff"`
	// 每次计算重试间隔时间的上限。为-1则取消间隔。默认：512 * time.Millisecond。
	MaxRetryBackoff time.Duration `yaml:"max-retry-backoff"`
	// 建立连接的超时时长。默认：5 * time.Second。
	DialTimeout time.Duration `yaml:"dial-timeout"`
	// 读超时时长。为-1则关闭读超时。默认：3 * time.Second。
	ReadTimeout time.Duration `yaml:"read-timeout"`
	// 写超时时长。为-1则关闭写超时。默认：ReadTimeout。
	WriteTimeout time.Duration `yaml:"write-timeout"`
	// 等待可用连接的最大时长。默认：ReadTimeout + 1。
	PoolTimeout time.Duration `yaml:"pool-timeout"`
	// 连接空闲检查的频率。为-1则在客户端获取连接时才检查超时。
	IdleCheckFrequency time.Duration `yaml:"idle-check-frequency"`
	// 连接空闲的最大时长。为-1则不对连接做超时检查。默认：5 * time.Minute。
	IdleTimeout time.Duration `yaml:"idle-timeout"`
	// 连接存活的最大时长。从创建开始计时，超过设定则关闭；为0则永远不关闭。默认：0。
	MaxConnAge time.Duration `yaml:"max-conn-age"`
	Nodes      []string      `yaml:"nodes"`
}

type Client interface {
	// Count 统计指定格式的键值对数量
	Count(format string) map[string]uint64
	// Clear 清除指定格式的键值对
	Clear(format string) map[string]uint64
	// Close 关闭客户端
	Close()
}
