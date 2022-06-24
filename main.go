package main

import (
	"context"
	"flag"
	"strconv"
	"strings"

	"gedis/internal"

	"github.com/Chngzhen/log4g"
)

var isCluster, printVersion bool
var nodes, password, operation, format string
var database int
var version = "1.1.1"

// ./gedis -c -n 127.0.0.1:7001 -a 'Password' -o count -f user:info:136*
func main() {
	flag.StringVar(&password, "a", "", "若Redis设置了访问密码，则必填。")
	flag.BoolVar(&isCluster, "c", false, "可选。是否集群模式。默认：false。注意，布尔参数需要以-flag=true的方式设置，-flag默认取值true。")
	flag.StringVar(&nodes, "n", "", "必填。Redis实例的地址，多个用英文逗号分隔。如127.0.0.1:7001,127.0.0.1:7002。")
	flag.StringVar(&operation, "o", "", "必填。操作类型。取值：del | count。")
	flag.StringVar(&format, "f", "", "键名格式。")
	flag.IntVar(&database, "d", 0, "数据库下标。默认：0。该选项对集群无效。")
	flag.BoolVar(&printVersion, "v", false, "可选。是否输出版本。默认：false。注意，启用该选项时会忽略其他选项。")
	flag.Parse()

	if printVersion {
		println("Gedis V" + version)
		return
	}

	// 创建客户端
	var client internal.Client
	var err error
	ctx := context.Background()
	if isCluster {
		// 集群模式
		redisProperty := &internal.Property{
			Password: password,
			Cluster: &internal.Cluster{
				MaxRedirects: 9,
				PoolSize:     10,
				MinIdleConns: 2,
				Nodes:        strings.Split(nodes, ","),
			},
		}

		client, err = internal.NewClusterClient(ctx, redisProperty)
		if err != nil {
			log4g.Error("集群客户端创建失败：%+v", err)
			return
		}
		defer client.Close()
	} else {
		// 单例模式
		if database < 0 {
			log4g.Error("数据库索引不能小于0：%d", database)
			return
		}

		address := strings.TrimSpace(nodes)
		if address == "" {
			log4g.Error("未提供Redis服务地址")
			return
		}
		addresses := strings.Split(address, ":")
		if len(addresses) != 2 {
			log4g.Error("Redis服务地址不合法：%s", addresses)
			return
		}
		port, err := strconv.Atoi(addresses[1])
		if err != nil {
			log4g.Error("Redis服务地址不合法：%s", addresses)
			return
		}
		config := &internal.Property{
			Host:     addresses[0],
			Port:     port,
			Password: password,
			Database: database,
		}

		client, err = internal.NewSingleClient(ctx, config)
		if err != nil {
			log4g.Error("单例客户端创建失败：%+v", err)
			return
		}
		log4g.Info("单例客户端创建成功！")
		defer client.Close()
	}

	// 执行操作
	switch operation {
	case "del":
		results := client.Clear(format)
		output(results)
		break
	case "count":
		results := client.Count(format)
		output(results)
		break
	default:
		log4g.Error("未定义的操作：%s", operation)
	}
}

func output(data map[string]uint64) {
	if nil != data {
		log4g.Info("********** 开始统计 **********")
		var total uint64
		for k, v := range data {
			total += v
			log4g.Info("节点[%s]的匹配数量：%d", k, v)
		}
		log4g.Info("所有节点的匹配数量：%d", total)
		log4g.Info("********** 结束统计 **********")
	}
}
