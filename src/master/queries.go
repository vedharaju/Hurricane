package master

import (
	hood "github.com/eaigner/hood"
)

func (rdd *Rdd) getSegments(tx *hood.Hood) []*Segment {
	var results []Segment
	err := tx.Where("rdd_id", "=", rdd.Id).Find(&results)
	if err != nil {
		panic(err)
	}

	// Should return pointers to the result objects so that
	// they can be mutated
	pointerResults := make([]*Segment, len(results))
	for i := range results {
		pointerResults[i] = &results[i]
	}

	return pointerResults
}
