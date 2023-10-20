package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
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
	host, port, err := net.SplitHostPort(input)
	if err != nil {
		log.Fatalln(err)
	}
	ips, err := net.LookupHost(host)
	if err != nil {
		log.Fatalln(err)
	}

	// 设置随机数种子
	rand.Seed(time.Now().UnixNano())
	// 从 IP 列表中随机选择一个 IP
	randomIndex := rand.Intn(len(ips))
	randomIP := ips[randomIndex]
	return fmt.Sprintf("%s:%s", randomIP, port)

}

// 根据object key（filename） 返回 abi.SectorID
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
