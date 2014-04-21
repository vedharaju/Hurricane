package master

import (
	hood "github.com/eaigner/hood"
)

func (rdd *Rdd) getSegments(tx *hood.Hood) []Segment {
	var results []Segment
	err := tx.Where("rdd_id", "=", rdd.Id).Find(&results)
	if err != nil {
		panic(err)
	}

	return results
}
