package xbstream

//func TestMetadataParser(t *testing.T) {
//	var file = strings.Trim(`
//		page_size = 16384
//		zip_size = 0
//		space_id = 8
//		space_flags = 33`, " \t\n\r")
//
//	reader := bytes.NewReader([]byte(file))
//	meta, err := readMetadata(reader)
//
//	assert.NoError(t, err)
//	assert.Equal(t, deltaInfo{
//		pageSize:   16 * 1024,
//		zipSize:    0,
//		spaceId:    8,
//		spaceFlags: 33,
//	}, meta)
//}
