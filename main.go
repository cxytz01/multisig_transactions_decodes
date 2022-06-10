package main

import (
	"bufio"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"os/exec"
	"time"

	"github.com/jszwec/csvutil"

	// "github.com/filecoin-project/lily/lens/util"
	// "github.com/ipfs/go-cid"
	_ "github.com/lib/pq"
)

var (
	DB *sql.DB

	Code = map[string]string{
		"fil/1/multisig":         "bafkqadtgnfwc6mjpnv2wy5djonuwo",
		"fil/2/multisig":         "bafkqadtgnfwc6mrpnv2wy5djonuwo",
		"fil/3/multisig":         "bafkqadtgnfwc6mzpnv2wy5djonuwo",
		"fil/4/multisig":         "bafkqadtgnfwc6nbpnv2wy5djonuwo",
		"fil/5/multisig":         "bafkqadtgnfwc6njpnv2wy5djonuwo",
		"fil/6/multisig":         "bafkqadtgnfwc6nrpnv2wy5djonuwo",
		"fil/7/multisig":         "bafkqadtgnfwc6nzpnv2wy5djonuwo",
		"fil/1/storageminer":     "bafkqaetgnfwc6mjpon2g64tbm5sw22lomvza",
		"fil/2/storageminer":     "bafkqaetgnfwc6mrpon2g64tbm5sw22lomvza",
		"fil/3/storageminer":     "bafkqaetgnfwc6mzpon2g64tbm5sw22lomvza",
		"fil/4/storageminer":     "bafkqaetgnfwc6nbpon2g64tbm5sw22lomvza",
		"fil/5/storageminer":     "bafkqaetgnfwc6njpon2g64tbm5sw22lomvza",
		"fil/6/storageminer":     "bafkqaetgnfwc6nrpon2g64tbm5sw22lomvza",
		"fil/7/storageminer":     "bafkqaetgnfwc6nzpon2g64tbm5sw22lomvza",
		"fil/1/account":          "bafkqadlgnfwc6mjpmfrwg33vnz2a",
		"fil/2/account":          "bafkqadlgnfwc6mrpmfrwg33vnz2a",
		"fil/3/account":          "bafkqadlgnfwc6mzpmfrwg33vnz2a",
		"fil/4/account":          "bafkqadlgnfwc6nbpmfrwg33vnz2a",
		"fil/5/account":          "bafkqadlgnfwc6njpmfrwg33vnz2a",
		"fil/6/account":          "bafkqadlgnfwc6nrpmfrwg33vnz2a",
		"fil/7/account":          "bafkqadlgnfwc6nzpmfrwg33vnz2a",
		"fil/1/storagepower":     "bafkqaetgnfwc6mjpon2g64tbm5sxa33xmvza",
		"fil/2/storagepower":     "bafkqaetgnfwc6mrpon2g64tbm5sxa33xmvza",
		"fil/3/storagepower":     "bafkqaetgnfwc6mzpon2g64tbm5sxa33xmvza",
		"fil/4/storagepower":     "bafkqaetgnfwc6nbpon2g64tbm5sxa33xmvza",
		"fil/5/storagepower":     "bafkqaetgnfwc6njpon2g64tbm5sxa33xmvza",
		"fil/6/storagepower":     "bafkqaetgnfwc6nrpon2g64tbm5sxa33xmvza",
		"fil/7/storagepower":     "bafkqaetgnfwc6nzpon2g64tbm5sxa33xmvza",
		"fil/1/verifiedregistry": "bafkqaftgnfwc6mjpozsxe2lgnfswi4tfm5uxg5dspe",
		"fil/2/verifiedregistry": "bafkqaftgnfwc6mrpozsxe2lgnfswi4tfm5uxg5dspe",
		"fil/3/verifiedregistry": "bafkqaftgnfwc6mzpozsxe2lgnfswi4tfm5uxg5dspe",
		"fil/4/verifiedregistry": "bafkqaftgnfwc6nbpozsxe2lgnfswi4tfm5uxg5dspe",
		"fil/5/verifiedregistry": "bafkqaftgnfwc6njpozsxe2lgnfswi4tfm5uxg5dspe",
		"fil/6/verifiedregistry": "bafkqaftgnfwc6nrpozsxe2lgnfswi4tfm5uxg5dspe",
		"fil/7/verifiedregistry": "bafkqaftgnfwc6nzpozsxe2lgnfswi4tfm5uxg5dspe",
		"fil/1/reward":           "bafkqaddgnfwc6mjpojsxoylsmq",
		"fil/2/reward":           "bafkqaddgnfwc6mrpojsxoylsmq",
		"fil/3/reward":           "bafkqaddgnfwc6mzpojsxoylsmq",
		"fil/4/reward":           "bafkqaddgnfwc6nbpojsxoylsmq",
		"fil/5/reward":           "bafkqaddgnfwc6njpojsxoylsmq",
		"fil/6/reward":           "bafkqaddgnfwc6nrpojsxoylsmq",
		"fil/7/reward":           "bafkqaddgnfwc6nzpojsxoylsmq",
		"fil/1/storagemarket":    "bafkqae3gnfwc6mjpon2g64tbm5sw2ylsnnsxi",
		"fil/2/storagemarket":    "bafkqae3gnfwc6mrpon2g64tbm5sw2ylsnnsxi",
		"fil/3/storagemarket":    "bafkqaeactgnfwc6nrpnfxgs5a",
		"fil/7/init":             "bafkqactgnfwc6nzpnfxgs5a",
	}
)

type MultisigTransaction struct {
	MultisigID    string `pg:",pk,notnull"`
	StateRoot     string `pg:",pk,notnull"`
	Height        int64  `pg:",pk,notnull,use_zero"`
	TransactionID int64  `pg:",pk,notnull,use_zero"`

	// Transaction State
	To            string `pg:",notnull"`
	Value         string `pg:",notnull"`
	Method        uint64 `pg:",notnull,use_zero"`
	Params        string
	ParamsDecoded string
	Approved      string
	ActorName     string
	ActorCode     string
}

type Actor struct {
	// Epoch when this actor was created or updated.
	Height int64 `pg:",pk,notnull,use_zero"`
	// ID Actor address.
	ID string `pg:",pk,notnull"`
	// CID of the state root when this actor was created or changed.
	StateRoot string `pg:",pk,notnull"`
	// Human-readable identifier for the type of the actor.
	Code string `pg:",notnull"`
	// CID of the root of the state tree for the actor.
	Head string `pg:",notnull"`
	// Balance of Actor in attoFIL.
	Balance string `pg:",notnull"`
	// The next Actor nonce that is expected to appear on chain.
	Nonce uint64 `pg:",use_zero"`
}

func InitDB() {
	var err error
	url := "xxx"
	DB, err = sql.Open("postgres", url)
	if err != nil {
		log.Fatal(err)
	}
	if err := DB.Ping(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Initialize DB Ok.")
}

func ScanMultisigTransactions() []MultisigTransaction {
	fmt.Printf("Scaning multisig_transactions tables on: %v\n", time.Now().Format("2006-01-02 15:04:05"))
	start := time.Now()
	defer func(start time.Time) {
		t := time.Now()
		elapsed := t.Sub(start)
		fmt.Printf("End on: %v, elapsed: %v\n", time.Now().Format("2006-01-02 15:04:05"), elapsed)
	}(start)

	rows, err := DB.Query("select multisig_id, state_root, height, transaction_id, \"to\", value, method, params, approved from multisig_transactions where params is not null")
	if err != nil {
		log.Fatal(err)
	}

	mt_array := make([]MultisigTransaction, 0, 80000)

	for rows.Next() {
		var mts MultisigTransaction
		var approved []uint8
		var param []byte
		if err := rows.Scan(&mts.MultisigID, &mts.StateRoot, &mts.Height, &mts.TransactionID, &mts.To, &mts.Value, &mts.Method, &param, &approved); err != nil {
			log.Fatal(err)
		}

		mts.Approved = string(approved)
		mts.Params = "\\x" + hex.EncodeToString(param)

		mt_array = append(mt_array, mts)

		// fmt.Printf("%+v\n", mts)
	}

	rows.Close()

	fmt.Printf("len(multisig_transactions) = %v\n", len(mt_array))

	return mt_array
}

func ActorsScaner(mt_array []MultisigTransaction) {
	fmt.Printf("Scaning actors tables on: %v\n", time.Now().Format("2006-01-02 15:04:05"))
	start := time.Now()
	defer func(start time.Time) {
		t := time.Now()
		elapsed := t.Sub(start)
		fmt.Printf("End on: %v, elapsed: %v\n", time.Now().Format("2006-01-02 15:04:05"), elapsed)
	}(start)

	for i, mt := range mt_array {
		var actor Actor
		sql := fmt.Sprintf("select code from actors where height = %v and id = '%v' and state_root = '%v' limit 1", mt.Height, mt.MultisigID, mt.StateRoot)
		if err := DB.QueryRow(sql).Scan(
			//&actor.ID,
			&actor.Code,
			//&actor.Head,
			//&actor.Nonce,
			//&actor.Balance,
			//&actor.StateRoot,
			//&actor.Height,
		); err != nil {
			log.Fatal(err)
		}

		actorCode, ok := Code[actor.Code]
		if !ok {
			continue
		}
		mt_array[i].ActorName = actor.Code
		mt_array[i].ActorCode = actorCode

		fmt.Printf("ActorScaner handle index: %v\n", i)
	}
}

func DecodeParams(mt_array []MultisigTransaction) {
	fmt.Printf("Decode params on: %v\n", time.Now().Format("2006-01-02 15:04:05"))
	start := time.Now()
	defer func(start time.Time) {
		t := time.Now()
		elapsed := t.Sub(start)
		fmt.Printf("End on: %v, elapsed: %v\n", time.Now().Format("2006-01-02 15:04:05"), elapsed)
	}(start)

	var result int

	for i, mt := range mt_array {
		// actorCid, err := cid.Decode(mt.ActorCode)
		// if err != nil {
		// 	continue
		// }

		// parsed, _, err := util.ParseParams(mt.Params, abi.MethodNum(mt.Method), actorCid)
		// if err != nil {
		// 	continue
		// }
		// fmt.Printf("%+v\n", parsed)

		str0 := "/home/ec2-user/prj/lily/lily"
		str1 := "chain"
		str2 := "parse-msg-params"
		str3 := fmt.Sprintf("--actor-code=%v", mt.ActorCode)
		str4 := fmt.Sprintf("--hex-params=%v", mt.Params[2:])
		str5 := fmt.Sprintf("--method-number=%v", mt.Method)
		fmt.Printf("DecodeParams handle i: %v, cmd: %v %v %v %v %v %v\n", i, str0, str1, str2, str3, str4, str5)

		cmd := exec.Command(str0, str1, str2, str3, str4, str5)
		stdoutStderr, err := cmd.CombinedOutput()
		if err != nil {
			log.Println(err)
			result++
		}
		fmt.Printf("	%s\n", stdoutStderr)

		mt_array[i].ParamsDecoded = string(stdoutStderr)
	}

	fmt.Printf("Failed count: %v\n", result)
}

func ExportToCSV(mt_array []MultisigTransaction) {
	fmt.Printf("Export to csv on: %v\n", time.Now().Format("2006-01-02 15:04:05"))
	start := time.Now()
	defer func(start time.Time) {
		t := time.Now()
		elapsed := t.Sub(start)
		fmt.Printf("End on: %v, elapsed: %v\n", time.Now().Format("2006-01-02 15:04:05"), elapsed)
	}(start)

	// file, err := os.OpenFile("/Users/xy/prj/multisig_transactions_decodes/test.csv", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
	file, err := os.OpenFile("/home/ec2-user/prj/multisig_transactions_decodes/test.csv", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	b, err := csvutil.Marshal(mt_array)
	if err != nil {
		log.Fatal(err)
	}

	writer.Write(b)
	writer.Flush()
}

func main() {
	InitDB()

	mts := ScanMultisigTransactions()
	ActorsScaner(mts)
	DecodeParams(mts)

	ExportToCSV(mts)
}
