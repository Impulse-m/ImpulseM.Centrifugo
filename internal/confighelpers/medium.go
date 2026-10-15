package confighelpers

import (
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/centrifugal/centrifuge"
)

// MediumEnabled reports whether channel options enable any channel medium feature. It compares the
// converted options with their zero value, so every medium option counts without being listed here.
// Core's isMediumEnabled is the reference for the same rule.
func MediumEnabled(chOpts configtypes.ChannelOptions) bool {
	return MediumOptionsFromChannelOptions(chOpts) != (centrifuge.ChannelMediumOptions{})
}

// MediumOptionsFromChannelOptions converts channel options into centrifuge channel medium options.
func MediumOptionsFromChannelOptions(chOpts configtypes.ChannelOptions) centrifuge.ChannelMediumOptions {
	return centrifuge.ChannelMediumOptions{
		SharedPositionSync:    chOpts.SharedPositionSync,
		KeepLatestPublication: chOpts.KeepLatestPublication,
	}
}
