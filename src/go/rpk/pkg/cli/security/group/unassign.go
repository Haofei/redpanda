// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package group

import (
	"fmt"

	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func unassignCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var roleName string
	cmd := &cobra.Command{
		Use:   "unassign [GROUP] --role [ROLE]",
		Short: "Remove an IDP group from a Redpanda role",
		Long: `Remove an IDP group from a Redpanda role.

This command removes the mapping between an identity provider (IDP) group and a
Redpanda role.`,
		Example: `
Remove the "engineering" group from the "data-reader" role:
  rpk security group unassign engineering --role data-reader`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			f := p.Formatter
			if h, ok := f.Help([]string{}); ok {
				out.Exit(h)
			}
			prof, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitServerlessAdmin(prof)
			if prof.CheckFromCloud() {
				out.Die("rpk security group is not yet available in Redpanda Cloud")
			}

			groupName := args[0]

			cl, err := adminapi.NewClient(cmd.Context(), fs, prof)
			out.MaybeDie(err, "unable to initialize admin api client: %v", err)

			if !adminapi.HasMinimumVersion(cmd.Context(), cl, minVersion) {
				out.Die("rpk security group requires Redpanda version %s or later", minVersion.String())
			}

			_, err = cl.SecurityService().RemoveRoleMembers(cmd.Context(), connect.NewRequest(&adminv2.RemoveRoleMembersRequest{
				RoleName: roleName,
				Members:  []*adminv2.RoleMember{groupMember(groupName)},
			}))
			out.MaybeDie(err, "unable to unassign group %q from role %q: %v", groupName, roleName, err)

			if f.Kind == "text" {
				fmt.Printf("Successfully unassigned group %q from role %q\n", groupName, roleName)
			}
		},
	}

	cmd.Flags().StringVar(&roleName, "role", "", "Role to unassign the group from")
	cmd.MarkFlagRequired("role")
	return cmd
}
