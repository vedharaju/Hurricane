package master

import (
	hood "github.com/eaigner/hood"
)

func (rdd *Rdd) getSegments(tx *hood.Hood) []*Segment {
	var results []*Segment
	err := tx.Where("RddId", "=", rdd.Id).Find(&results)
	if err != nil {
		panic(err)
	}

	return results
}

func (rdd *Rdd) getJob(tx *hood.Hood) *Job {
  var results []*Job
  err := tx.Where("JobId", "=", rdd.JobId).Limit(1).Find(&results)
  if err != nil {
    panic(err)
  }

  return results[0]
}
