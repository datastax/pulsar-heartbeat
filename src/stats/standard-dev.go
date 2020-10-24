package stats

import "math"

// StandardDeviation is the struct to calculate and store standard deviation
// specifically this is a population standard deviation
type StandardDeviation struct {
	Name    string
	Sum     float64
	Mean    float64
	Buckets []float64
	Std     float64 // σ
}

// NewStandardDeviation creates a new standard dev object
func NewStandardDeviation(name string) StandardDeviation {
	return StandardDeviation{
		Name: name,
	}
}

// Push a float64 to calculate standard deviation and returns σ and whether the number is over 6σ in positive right side of bell curve
// 6σ is at odd of every three weeks
func (sd *StandardDeviation) Push(num float64) (std, mean float64, within6Sigma bool) {
	sd.Buckets = append(sd.Buckets, num)
	sd.Sum += num
	counter := len(sd.Buckets)
	sd.Mean = sd.Sum / float64(counter)

	for _, v := range sd.Buckets {
		std += math.Pow(v-sd.Mean, 2)
	}

	std = math.Sqrt(std / float64(counter))
	sd.Std = std

	// 6σ evaluation only applies to 10 more data samples
	return std, sd.Mean, num-sd.Mean < 6*std || counter < 10

}

// Add a float64 sample to the bucket
func (sd *StandardDeviation) Add(num float64) {
	sd.Buckets = append(sd.Buckets, num)
}
