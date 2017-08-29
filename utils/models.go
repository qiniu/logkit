package utils

type Option struct {
	KeyName       string
	ChooseOnly    bool
	ChooseOptions []string
	Default       string
	DefaultNoUse  bool
	Description   string
	CheckRegex    string
}

type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
