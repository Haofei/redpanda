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

func assignCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var roleName string
	cmd := &cobra.Command{
		Use:   "assign [GROUP] --role [ROLE]",
		Short: "Assign an IDP group to a Redpanda role",
		Long: `Assign an IDP group to a Redpanda role.

This command maps an identity provider (IDP) group to a Redpanda role, granting
all members of the group the permissions associated with the role.`,
		Example: `
Assign the "engineering" group to the "data-reader" role:
  rpk security group assign engineering --role data-reader`,
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

			_, err = cl.SecurityService().AddRoleMembers(cmd.Context(), connect.NewRequest(&adminv2.AddRoleMembersRequest{
				RoleName: roleName,
				Members:  []*adminv2.RoleMember{groupMember(groupName)},
			}))
			out.MaybeDie(err, "unable to assign group %q to role %q: %v", groupName, roleName, err)

			if f.Kind == "text" {
				fmt.Printf("Successfully assigned group %q to role %q\n", groupName, roleName)
			}
		},
	}

	cmd.Flags().StringVar(&roleName, "role", "", "Role to assign the group to")
	cmd.MarkFlagRequired("role")
	return cmd
}
