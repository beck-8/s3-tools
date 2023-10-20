package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"time"

	"github.com/filecoin-project/go-address"
)

var srcUuid, dstUuid, rpc, token string
var disableLookupDomain bool

func nslookupShuf(input string) string {
	if disableLookupDomain {
		return input
	}
	parsedURL, err := url.Parse("http://" + input)
	if err != nil {
		log.Fatalln(err)
	}
	host := parsedURL.Hostname()
	port := parsedURL.Port()
	addrs, err := net.LookupIP(host)
	if err != nil {
		log.Fatalln(err)
	}
	var ipv4Addrs []string
	for _, addr := range addrs {
		if ipv4 := addr.To4(); ipv4 != nil {
			ipv4Addrs = append(ipv4Addrs, ipv4.String())
		}
	}

	// 设置随机数种子
	rand.Seed(time.Now().UnixNano())
	// 从 IP 列表中随机选择一个 IP
	randomIndex := rand.Intn(len(ipv4Addrs))
	randomIP := ipv4Addrs[randomIndex]

	if port == "" {
		return fmt.Sprint(randomIP)
	} else {
		return fmt.Sprintf("%s:%s", randomIP, port)
	}
}

// 根据object key（filename），在目标位置声明，在原位置删除
func changeStorage(object string, srcUuid string, dstUuid string) error {
	re := regexp.MustCompile(`.*s-(t\d+)-(\d+)`)
	match := re.FindStringSubmatch(object)
	if len(match) != 3 {
		return fmt.Errorf("to abi.SectorID failed, input type error")
	}
	minerAdd := match[1]
	sectorNum := match[2]

	addr, err := address.NewFromString(minerAdd)
	if err != nil {
		return err
	}
	mid, err := address.IDFromAddress(addr)
	if err != nil {
		return err
	}
	snum, err := strconv.ParseUint(sectorNum, 10, 64)
	if err != nil {
		return err
	}

	client := &http.Client{}
	request := func(payload map[string]interface{}) error {
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("error encoding JSON: %s", err)
		}

		req, err := http.NewRequest("POST", rpc, bytes.NewBuffer(payloadBytes))
		if err != nil {
			return fmt.Errorf("error creating HTTP request: %s", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("error sending request: %s", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("error code: %d", resp.StatusCode)
		}
		return nil
	}

	declarePlayload := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "Filecoin.StorageDeclareSector",
		"params": []interface{}{
			dstUuid,
			map[string]interface{}{
				"Miner":  mid,
				"Number": snum,
			},
			1,
			true,
		},
		"id": 1,
	}
	err = request(declarePlayload)
	if err != nil {
		return err
	}
	log.Printf("declare %s in %s\n", object, dstUuid)

	dropPlayload := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "Filecoin.StorageDropSector",
		"params": []interface{}{
			srcUuid,
			map[string]interface{}{
				"Miner":  mid,
				"Number": snum,
			},
			1,
		},
		"id": 1,
	}
	err = request(dropPlayload)
	if err != nil {
		return err
	}
	log.Printf("drop %s in %s\n", object, srcUuid)

	return nil
}

// 超过 hours 时间的数据清理掉
func deleteOldEntries(m map[string]time.Time, hours int) {
	for key, val := range m {
		if time.Since(val) > time.Duration(hours)*time.Hour {
			delete(m, key)
		}
	}
}
