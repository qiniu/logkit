package models

type Resetable interface {
	Reset() error
}

type Deleteable interface {
	Delete() error
}
