package utils

type Option struct {
	KeyName       string
	ChooseOnly    bool
	ChooseOptions []interface{}
	Default       string
	DefaultNoUse  bool
	Description   string
	CheckRegex    string
	Type          string `json:"Type,omitempty"`
}

type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
