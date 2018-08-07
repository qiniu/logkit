package models

type Resetable interface {
	Reset() error
}
