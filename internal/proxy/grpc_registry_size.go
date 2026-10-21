package proxy

import (
	"errors"
	"math"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// grpcMessagePrefix starts every message grpc-go gives for a message over a receive limit.
const grpcMessagePrefix = "grpc: "

// ErrRPCTargetRequestTooLarge is returned by CallRegistryMethod when a grpc-go target refused the request with
// its own receive limit.
var ErrRPCTargetRequestTooLarge = errors.New("rpc registry target refused the request as too large")

// registryMessageTooLarge classifies a grpc-go "larger than max" error of a call whose request has requestLen
// bytes, whose reply bound is bound, and whose target is compressed or not. tooLarge reports that the error is
// one of these. reply reports that the reply was over bound: the message names bound, or math.MaxInt, the largest
// int, which grpc-go names when a length does not fit in an int. Otherwise a grpc-go target refused the request. A
// compressed target reports the compressed size, which the request size cannot match, so it always counts as a refused
// request. Only messages which start with the grpc-go prefix count, so an application error with similar text does not.
func registryMessageTooLarge(err error, requestLen int, bound int, compressed bool) (tooLarge bool, reply bool) {
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.ResourceExhausted {
		return false, false
	}
	message := st.Message()
	if !strings.HasPrefix(message, grpcMessagePrefix) || !strings.Contains(message, "larger than max") {
		return false, false
	}
	if compressed {
		return true, false
	}
	reported, limit, sized := reportedSizes(message)
	if sized {
		if reported == requestLen {
			return true, false
		}
		if limit == bound || limit == math.MaxInt {
			return true, true
		}
		return false, false
	}
	named := strings.HasSuffix(message, " "+strconv.Itoa(bound)) || strings.HasSuffix(message, " "+strconv.Itoa(math.MaxInt))
	return true, named
}

// reportedSizes returns the two sizes of a grpc-go message in parentheses, as in "(2000 vs. 1024)": the size of the
// message and the limit. sized is false when the message has no such pair.
func reportedSizes(message string) (reported int, limit int, sized bool) {
	_, after, found := strings.Cut(message, "(")
	if !found {
		return 0, 0, false
	}
	inside, _, closed := strings.Cut(after, ")")
	if !closed {
		return 0, 0, false
	}
	left, right, paired := strings.Cut(inside, " vs. ")
	if !paired {
		return 0, 0, false
	}
	reported, leftErr := strconv.Atoi(left)
	limit, rightErr := strconv.Atoi(right)
	if leftErr != nil || rightErr != nil {
		return 0, 0, false
	}
	return reported, limit, true
}
