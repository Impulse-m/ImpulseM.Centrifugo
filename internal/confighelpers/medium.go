package confighelpers

import (
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/centrifugal/centrifuge"
)

// MediumOptionsFromChannelOptions converts channel options into centrifuge channel medium options.
func MediumOptionsFromChannelOptions(chOpts configtypes.ChannelOptions) centrifuge.ChannelMediumOptions {
	return centrifuge.ChannelMediumOptions{
		SharedPositionSync: chOpts.SharedPositionSync,
	}
}
