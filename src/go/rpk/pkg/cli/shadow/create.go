// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package shadow

import (
	"fmt"

	controlplanev1beta2 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/controlplane/v1beta2"
	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/providers/auth0"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

func newCreateCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		noConfirm   bool
		cfgLocation string
	)
	cmd := &cobra.Command{
		Use:   "create",
		Args:  cobra.NoArgs,
		Short: "Create a Redpanda Shadow Link",
		Long: `Create a Redpanda Shadow Link.

This command creates a Shadow Link using a configuration file that defines the
connection details and synchronization settings.

Before you create a Shadow Link, generate a configuration file with 'rpk shadow
config generate' and update it with your source cluster details. The command
prompts you to confirm the creation. Use the --no-confirm flag to skip the
confirmation prompt.

When creating a Shadow Link for Redpanda Cloud, make sure to login and select
the cluster where you want to create the Shadow Link before running this 
command. See 'rpk cloud login' and 'rpk cloud select'.

After you create the Shadow Link, use 'rpk shadow status' to monitor the
replication progress.
`,
		Example: `
Create a Shadow Link using a configuration file:
  rpk shadow create --config-file shadow-link.yaml

Create a Shadow Link without confirmation prompt:
  rpk shadow create -c shadow-link.yaml --no-confirm
`,
		Run: func(cmd *cobra.Command, _ []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load rpk config: %v", err)
			prof := cfg.VirtualProfile()
			config.CheckExitServerlessAdmin(prof)

			slCfg, err := parseShadowLinkConfig(fs, cfgLocation)
			out.MaybeDie(err, "unable to parse Shadow Link configuration file: %v", err)

			// This is a naive client side validation, the server will do a full
			// validation and return proper errors if something is wrong.
			err = validateParsedShadowLinkConfig(slCfg)
			out.MaybeDie(err, "invalid Shadow Link configuration: %v", err)

			printShadowLinkCfgOverview(slCfg)
			if !noConfirm {
				ok, err := out.Confirm("Do you want to create this shadow link?")
				out.MaybeDie(err, "unable to confirm Shadow Link creation: %v", err)
				if !ok {
					out.Exit("Shadow Link creation cancelled")
				}
			}
			fmt.Println()

			successMsgTmpl := "Successfully created shadow link %q with ID %q. To query the status, run:\n  'rpk shadow status %[1]v'"
			if prof.CheckFromCloud() {
				cloudClient, err := publicapi.NewValidatedCloudClientSet(
					cfg.DevOverrides().PublicAPIURL,
					prof.CurrentAuth().AuthToken,
					auth0.NewClient(cfg.DevOverrides()).Audience(),
					[]string{prof.CurrentAuth().ClientID},
				)
				out.MaybeDieErr(err)

				op, err := cloudClient.ShadowLink.CreateShadowLink(cmd.Context(), connect.NewRequest(&controlplanev1beta2.CreateShadowLinkRequest{
					ShadowLink: shadowLinkConfigToCloudCreate(slCfg),
				}))
				out.MaybeDie(err, "unable to create Shadow Link: %v", err)

				isComplete, err := waitForOperation(cmd.Context(), cloudClient, op.Msg.GetOperation().GetId())
				out.MaybeDie(err, "unable to confirm Shadow Link creation: %v", err)
				if isComplete {
					out.Exit(successMsgTmpl, slCfg.Name, op.Msg.GetOperation().GetResourceId())
				}
				out.Exit("Shadow link creation is taking longer than expected. Please check the status of the shadow link using 'rpk shadow status %q'", slCfg.Name)
			}
			cl, err := adminapi.NewClient(cmd.Context(), fs, prof)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			link, err := cl.ShadowLinkService().CreateShadowLink(cmd.Context(), connect.NewRequest(&adminv2.CreateShadowLinkRequest{
				ShadowLink: shadowLinkConfigToProto(slCfg),
			}))
			out.MaybeDie(err, "unable to create shadow link: %v", handleConnectError(err, "create", slCfg.Name))

			out.Exit(successMsgTmpl, link.Msg.GetShadowLink().GetName(), link.Msg.GetShadowLink().GetUid())
		},
	}

	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "Disable confirmation prompt")
	cmd.Flags().StringVarP(&cfgLocation, "config-file", "c", "", "Path to configuration file to use for the shadow link; use --help for details")
	cmd.MarkFlagRequired("config-file")
	return cmd
}

func parseShadowLinkConfig(fs afero.Fs, path string) (*ShadowLinkConfig, error) {
	file, err := afero.ReadFile(fs, path)
	if err != nil {
		return nil, fmt.Errorf("unable to read Shadow Link config file %q: %w", path, err)
	}

	var slCfg ShadowLinkConfig
	err = yaml.Unmarshal(file, &slCfg)
	if err != nil {
		return nil, fmt.Errorf("unable to parse Shadow Link config file %q: %w", path, err)
	}
	return &slCfg, nil
}

func printShadowLinkCfgOverview(slCfg *ShadowLinkConfig) {
	tw := out.NewTable()
	defer tw.Flush()
	tw.Print("Link Name:", slCfg.Name)
	if slCfg.CloudOptions != nil {
		tw.Print("Destination Redpanda ID:", slCfg.CloudOptions.DestinationRedpandaID)
		if slCfg.CloudOptions.SourceRedpandaID != "" {
			tw.Print("Source Redpanda ID:", slCfg.CloudOptions.SourceRedpandaID)
		}
		tw.Print("Resource Group ID:", slCfg.CloudOptions.ResourceGroupID)
	}
	if len(slCfg.ClientOptions.BootstrapServers) > 0 {
		tw.Print("Bootstrap Servers:", "")
		for _, srv := range slCfg.ClientOptions.BootstrapServers {
			tw.Print("", fmt.Sprintf("- %s", srv))
		}
	}
}

func validateParsedShadowLinkConfig(slCfg *ShadowLinkConfig) error {
	if slCfg == nil {
		return fmt.Errorf("provided configuration file generated an empty configuration")
	}
	if slCfg.Name == "" {
		return fmt.Errorf("the Shadow Link name is required")
	}
	// Cloud configuration does not require bootstrap servers.
	if slCfg.CloudOptions == nil && len(slCfg.ClientOptions.BootstrapServers) == 0 {
		return fmt.Errorf("at least one bootstrap server is required")
	}
	if tls := slCfg.ClientOptions.TLSSettings; tls != nil && tls.TLSFileSettings != nil && tls.TLSPEMSettings != nil {
		return fmt.Errorf("only one of TLS file settings or PEM settings can be provided")
	}
	if ts := slCfg.TopicMetadataSyncOptions; ts != nil {
		var count int
		if ts.StartAtLatest != nil {
			count++
		}
		if ts.StartAtEarliest != nil {
			count++
		}
		if ts.StartAtTimestamp != nil {
			count++
		}
		if count > 1 {
			return fmt.Errorf("only one of start_at_latest, start_at_earliest, or start_at_timestamp can be provided")
		}
	}
	if slc := slCfg.CloudOptions; slc != nil {
		if slc.DestinationRedpandaID == "" {
			return fmt.Errorf("destination_redpanda_id is required in cloud options")
		}
		if slc.DestinationRedpandaID == slc.SourceRedpandaID {
			return fmt.Errorf("destination_redpanda_id and source_redpanda_id cannot be the same")
		}
		if slCfg.ClientOptions != nil && slCfg.ClientOptions.TLSSettings != nil && slCfg.ClientOptions.TLSSettings.TLSFileSettings != nil {
			return fmt.Errorf("TLS file settings are not supported when using cloud options; use tls_pem_settings instead")
		}
	}
	return nil
}
