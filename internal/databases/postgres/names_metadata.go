package postgres

type DatabasesByNames map[string]DatabaseObjectsInfo

// TODO : add tables to info
type DatabaseObjectsInfo struct {
	Oid int `json:"oid"`
}

// TODO : improve that
func (meta DatabasesByNames) appendDatabaseInfos(infos []PgDatabaseInfo) {
	for _, info := range infos {
		// TODO : check that it is in default tablespace
		meta[info.Name] = DatabaseObjectsInfo{int(info.Oid)}
	}
}
