package policies

var Default = MergeAllStorages

// MergeAllStorages implies merging all storages into one as if it was a single storage. So, if a file with the same
// path exists in several storages, only the one from the first storage is taken.
var MergeAllStorages = Policies{
	Exists: ExistsPolicyAny,
	Read:   ReadPolicyFoundFirst,
	List:   ListPolicyFoundFirst,
	Put:    PutPolicyUpdateFirstFound,
	Delete: DeletePolicyAll,
	Copy:   CopyPolicyAll,
}

// UniteAllStorages implies uniting all storages into one so that multiple files with the same path could exist.
var UniteAllStorages = Policies{
	Exists: ExistsPolicyAny,
	Read:   ReadPolicyFoundFirst,
	List:   ListPolicyAll,
	Put:    PutPolicyUpdateAllFound,
	Delete: DeletePolicyAll,
	Copy:   CopyPolicyAll,
}

// TakeFirstStorage implies that only the first storage is used, as if it is not a multi-storage at all.
var TakeFirstStorage = Policies{
	Exists: ExistsPolicyFirst,
	Read:   ReadPolicyFirst,
	List:   ListPolicyFirst,
	Put:    PutPolicyFirst,
	Delete: DeletePolicyFirst,
	Copy:   CopyPolicyFirst,
}

// Policies define the behavior of the multi-storage folder in terms of selecting which underlying storages should be
// used to perform different operations.
type Policies struct {
	Exists ExistsPolicy
	Read   ReadPolicy
	List   ListPolicy
	Put    PutPolicy
	Delete DeletePolicy
	Copy   CopyPolicy
}

type ExistsPolicy int

const (
	ExistsPolicyFirst ExistsPolicy = iota
	ExistsPolicyAny
	ExistsPolicyAll
)

type ReadPolicy int

const (
	ReadPolicyFirst ReadPolicy = iota
	ReadPolicyFoundFirst
)

type ListPolicy int

const (
	ListPolicyFirst ListPolicy = iota
	ListPolicyFoundFirst
	ListPolicyAll
)

type PutPolicy int

const (
	PutPolicyFirst PutPolicy = iota
	PutPolicyUpdateFirstFound
	PutPolicyAll
	PutPolicyUpdateAllFound
)

type DeletePolicy int

const (
	DeletePolicyFirst DeletePolicy = iota
	DeletePolicyAll
)

type CopyPolicy int

const (
	CopyPolicyFirst CopyPolicy = iota
	CopyPolicyAll
)
