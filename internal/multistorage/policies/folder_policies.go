package policies

var Default = MergeAllStorages

var MergeAllStorages = Policies{
	Exists: ExistsPolicyAny,
	Read:   ReadPolicyFoundFirst,
	List:   ListPolicyFoundFirst,
	Put:    PutPolicyUpdateFirstFound,
	Delete: DeletePolicyAll,
	Copy:   CopyPolicyAll,
}

var UniteAllStorages = Policies{
	Exists: ExistsPolicyAny,
	Read:   ReadPolicyFoundFirst,
	List:   ListPolicyAll,
	Put:    PutPolicyUpdateAllFound,
	Delete: DeletePolicyAll,
	Copy:   CopyPolicyAll,
}

var TakeFirstStorage = Policies{
	Exists: ExistsPolicyFirst,
	Read:   ReadPolicyFirst,
	List:   ListPolicyFirst,
	Put:    PutPolicyFirst,
	Delete: DeletePolicyFirst,
	Copy:   CopyPolicyFirst,
}

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
