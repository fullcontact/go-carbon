package tag

import (
	tvindex "github.com/lomik/go-carbon/tags/index/tv"
)

type TagInode struct {
	Name   string
	Values *tvindex.Tree
}
