package confighelpers

import (
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/centrifugal/centrifuge"
)

// MediumEnabled reports whether channel options enable any channel medium feature.
func MediumEnabled(chOpts configtypes.ChannelOptions) bool {
	opts := MediumOptionsFromChannelOptions(chOpts)
	return opts.SharedPositionSync || opts.KeepLatestPublication
}

// MediumOptionsFromChannelOptions converts channel options into centrifuge channel medium options.
func MediumOptionsFromChannelOptions(chOpts configtypes.ChannelOptions) centrifuge.ChannelMediumOptions {
	return centrifuge.ChannelMediumOptions{
		SharedPositionSync: chOpts.SharedPositionSync,
	}
}
