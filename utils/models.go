package utils

type Option struct {
	KeyName       string
	ChooseOnly    bool
	ChooseOptions []interface{}
	Default       interface{}
	DefaultNoUse  bool
	Description   string
	CheckRegex    string
	Type          string `json:"Type,omitempty"`
	Secret        bool
}

type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
