package internal

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"time"
)

var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func RandSeq(n int) string {
	rand.Seed(time.Now().UTC().UnixNano())
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

type opConfig struct {
	Op      string            `json:"op"`
	Cnt     int               `json:"cnt"`
	DBName  string            `json:"db"`
	ColName string            `json:"cl"`
	Cmds    []json.RawMessage `json:"dc"`
	Adv     json.RawMessage   `json:"adv"`
}

type advInsert struct {
	Values    []string `json:"values"`
	MnValLen  int      `json:"mn_val_len"`
	MxValLen  int      `json:"mx_val_len"`
	Keys      []string `json:"keys"`
	MnKeyLen  int      `json:"mn_key_len"`
	MxKeyLen  int      `json:"mx_key_len"`
	MnDocsCnt int      `json:"mn_docs_cnt"`
	MxDocsCnt int      `json:"mx_docs_cnt"`
	MnKeysCnt int      `json:"mn_keys_cnt"`
	MxKeysCnt int      `json:"mx_keys_cnt"`
}

type advDelete struct {
	Values   []string `json:"values"`
	MnValLen int      `json:"mn_val_len"`
	MxValLen int      `json:"mx_val_len"`
	Keys     []string `json:"keys"`
	MnKeyLen int      `json:"mn_key_len"`
	MxKeyLen int      `json:"mx_key_len"`
}

type advSleep struct {
	Time float32 `json:"time"`
}

type ammoConfig struct {
	PatronName string     `json:"name"`
	OpConfig   []opConfig `json:"config"`
}

var id int

func updateMnMx(inMn, inMx, eMn, eMx int) (int, int) {
	if inMn == 0 {
		inMn = eMn
	}
	if inMx == 0 {
		inMx = eMx
	}
	if inMx < inMn {
		inMx = inMn
	}
	return inMn, inMx
}

func valueGenF(values []string, mnLen, mxLen int) func() string {
	if len(values) == 0 {
		return func() string {
			mnLen, mxLen = updateMnMx(mnLen, mxLen, 2, 10)
			ln := rand.Intn(mxLen-mnLen+1) + mnLen
			return RandSeq(ln)
		}
	}
	return func() string {
		idx := rand.Intn(len(values))
		return values[idx]
	}
}

var processOp = map[string]func(config opConfig) (string, error){
	"insert": func(config opConfig) (string, error) {
		var adv advInsert
		if len(config.Adv) != 0 {
			err := json.Unmarshal(config.Adv, &adv)
			if err != nil {
				fmt.Println(err)
				return "", err
			}
		}
		valueGen := valueGenF(adv.Values, adv.MnValLen, adv.MxValLen)
		keyGen := valueGenF(adv.Keys, adv.MnKeyLen, adv.MxKeyLen)
		docsGen := func() string {
			adv.MnDocsCnt, adv.MxDocsCnt = updateMnMx(adv.MnDocsCnt, adv.MxDocsCnt, 1, 3)
			adv.MnKeysCnt, adv.MxKeysCnt = updateMnMx(adv.MnKeysCnt, adv.MxKeysCnt, 1, 3)
			dCnt := rand.Intn(adv.MxDocsCnt-adv.MnDocsCnt+1) + adv.MnDocsCnt
			kCnt := rand.Intn(adv.MxKeysCnt-adv.MnKeysCnt+1) + adv.MnDocsCnt
			docs := "["
			for d := 0; d < dCnt; d++ {
				doc := "{"
				for k := 0; k < kCnt; k++ {
					doc += fmt.Sprintf(`"%s": "%s"`, keyGen(), valueGen())
					if k != kCnt-1 {
						doc += ", "
					}
				}
				doc += "}"
				if d != dCnt-1 {
					doc += ", "
				}
				docs += doc
			}
			docs += "]"
			return docs
		}

		id++
		return fmt.Sprintf(`{"op":"c", "db":"%s", "id": %d, "dc":{"insert":"%s", "documents": %s}}`,
			config.DBName, id, config.ColName, docsGen()), nil

	},
	"delete": func(config opConfig) (string, error) {
		var adv advDelete
		if len(config.Adv) != 0 {
			err := json.Unmarshal(config.Adv, &adv)
			if err != nil {
				fmt.Println(err)
				return "", err
			}
		}
		delsGen := func() string {
			return `[{"q": {}, "limit": 0}]`
		}

		id++
		return fmt.Sprintf(`{"op":"c", "db":"%s", "id": %d, "dc":{"delete":"%s", "deletes": %s}}`,
			config.DBName, id, config.ColName, delsGen()), nil
	},
	"sleep": func(config opConfig) (string, error) {
		var adv advSleep
		if len(config.Adv) != 0 {
			err := json.Unmarshal(config.Adv, &adv)
			if err != nil {
				fmt.Println(err)
				return "", err
			}
		}
		id++
		return fmt.Sprintf(`{"op":"sleep", "db":"%s", "cl": "%s", "id": %d, "time": %f}`,
			config.DBName, config.ColName, id, adv.Time), nil
	},
}

func addIndeciesAfterOp(str string) string {
	i := strings.Index(str, `"op"`)
	if i == -1 {
		return str
	}
	id++
	return str[:i] + fmt.Sprintf(`"id": %d, "op"`, id) + addIndeciesAfterOp(str[(i+4):])
}

func generateOp(writer io.Writer, config opConfig, lastComma bool) error {
	if config.Cmds != nil {
		if config.Op != "" {
			return fmt.Errorf("if explicit cmds is used, op field cannot be set")
		}
		var res []string
		for _, cmd := range config.Cmds {
			fstr := strings.Map(func(r rune) rune {
				if strings.Contains(" \n\t\r", string(r)) { //nolint: gocritic
					return -1
				}
				return r
			}, string(cmd))
			fstr = addIndeciesAfterOp(fstr)
			res = append(res, fstr)
		}
		cmdLine := strings.Join(res, ",\n")
		if lastComma {
			cmdLine += ","
		}
		cmdLine += "\n"
		_, err := writer.Write([]byte(cmdLine))
		if err != nil {
			return fmt.Errorf("cannot generate op %s: %v", config.Op, err)
		}
		return nil
	}
	for i := 0; i < config.Cnt; i++ {
		cmdLine, err := processOp[config.Op](config)
		if err != nil {
			return fmt.Errorf("cannot generate op %s: %v", config.Op, err)
		}
		if i != config.Cnt-1 || lastComma {
			cmdLine += ","
		}
		cmdLine += "\n"
		_, err = writer.Write([]byte(cmdLine))
		if err != nil {
			return fmt.Errorf("cannot generate op %s: %v", config.Op, err)
		}
	}
	return nil
}

func generateAmmo(config ammoConfig, w io.Writer) error {
	_, _ = w.Write([]byte("[\n"))
	for i, opConfig := range config.OpConfig {
		err := generateOp(w, opConfig, i != len(config.OpConfig)-1)
		if err != nil {
			return fmt.Errorf("cannot generate patron %s: %v", config.PatronName, err)
		}
	}
	_, _ = w.Write([]byte("]\n"))
	return nil
}

func GenerateAmmo(r io.Reader, w io.Writer) error {
	rand.Seed(time.Now().UnixNano())

	decoder := json.NewDecoder(r)
	var configs []ammoConfig
	err := decoder.Decode(&configs)
	if err != nil {
		return fmt.Errorf("cannot decode config JSON: %v", err)
	}
	for i := range configs {
		id = 0
		if err := generateAmmo(configs[i], w); err != nil {
			return err
		}
	}
	return nil
}
