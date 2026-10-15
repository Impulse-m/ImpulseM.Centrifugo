package confighelpers

import (
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/centrifugal/centrifuge"
)

// MediumEnabled reports whether channel options enable any channel medium feature.
// It derives from MediumOptionsFromChannelOptions so a new medium option is picked up
// in one place. Core's isMediumEnabled is the reference for the same rule.
func MediumEnabled(chOpts configtypes.ChannelOptions) bool {
	mediumOpts := MediumOptionsFromChannelOptions(chOpts)

	return mediumOpts.SharedPositionSync || mediumOpts.KeepLatestPublication
}

// MediumOptionsFromChannelOptions converts channel options into centrifuge channel medium options.
func MediumOptionsFromChannelOptions(chOpts configtypes.ChannelOptions) centrifuge.ChannelMediumOptions {
	return centrifuge.ChannelMediumOptions{
		SharedPositionSync:    chOpts.SharedPositionSync,
		KeepLatestPublication: chOpts.KeepLatestPublication,
	}
}
