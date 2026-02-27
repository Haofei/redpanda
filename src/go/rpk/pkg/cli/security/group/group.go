// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package group manages IDP group-to-role mappings for Group-Based Access Control (GBAC).
package group

import (
	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/redpanda"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

var minVersion = redpanda.Version{Major: 26, Feature: 1}

// NewCommand aggregates the IDP group subcommands.
func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "group",
		Args:  cobra.ExactArgs(0),
		Short: "Manage IDP group-to-role mappings",
	}
	cmd.AddCommand(
		listCommand(fs, p),
		describeCommand(fs, p),
		assignCommand(fs, p),
		unassignCommand(fs, p),
	)
	p.InstallAdminFlags(cmd)
	p.InstallFormatFlag(cmd)
	return cmd
}

func groupMember(name string) *adminv2.RoleMember {
	return &adminv2.RoleMember{
		Member: &adminv2.RoleMember_Group{
			Group: &adminv2.RoleGroup{Name: name},
		},
	}
}
