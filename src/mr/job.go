package mr

import (
	"log"
	"sync"
	"time"
)

type JobType int

const (
	MAPREADY JobType = iota
	MAP
	REDUCEREADY
	REDUCE
	DONE
)

type JobId int

type IDGenerator struct {
	id JobId
	lk sync.Mutex
}

func (i *IDGenerator) GetId() (id JobId) {
	i.lk.Lock()
	i.lk.Unlock()
	id = i.id
	i.id++
	return
}

var id_generator = &IDGenerator{id: 0}

type Job struct {
	Type      JobType
	Id        JobId
	Filename  []string
	NReduce   int
	StartTime time.Time
}

func NewJob(typ JobType, filename []string, nreduce int) (j *Job) {
	if typ != MAPREADY && typ != REDUCEREADY {
		log.Printf("Job created should be in ready state, but in %v instead", typ)
		return nil
	}

	j = &Job{
		Type:      typ,
		Id:        id_generator.GetId(),
		Filename:  filename,
		NReduce:   nreduce,
		StartTime: time.Now(),
	}
	return
}
