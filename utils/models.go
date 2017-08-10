package utils

type Option struct {
	ChooseOnly    bool
	ChooseOptions []string
	Default       string
	DefaultNoUse  bool
	Description   string
}

type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
