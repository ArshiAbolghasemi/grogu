package voice

import (
	"math"

	"git.mci.dev/mse/sre/phoenix/golang/grogu/internal/asr"
)

type OverlapResult map[string]bool

// CheckChannelOverlaps checking overlaps between segments due
func CheckChannelInterrupts(segs []asr.Segment, threshold float64) OverlapResult {
	result := make(OverlapResult)

	for i := range segs {
		for j := range segs {
			checkInterrupt(&segs[i], &segs[j], threshold, result)
		}
	}

	fillMissingChannels(segs, result)

	return result
}

func checkInterrupt(primary, condidate *asr.Segment, threshold float64, result OverlapResult) {
	if primary.Channel == condidate.Channel {
		return
	}

	if primary.End <= condidate.Start || condidate.End <= primary.Start {
		return
	}

	start := math.Max(primary.Start, condidate.Start)
	end := math.Min(primary.End, condidate.End)
	duration := end - start

	if duration <= threshold {
		return
	}

	if primary.Start > condidate.Start {
		result[primary.Channel] = true
	} else {
		result[condidate.Channel] = true
	}
}

func fillMissingChannels(segs []asr.Segment, result OverlapResult) {
	for i := range segs {
		ch := segs[i].Channel
		if _, exists := result[ch]; !exists {
			result[ch] = false
		}
	}
}
