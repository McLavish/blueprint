// Package main compiles the two PREDICTED topologies of the retry-pathology
// campaign, Single and Multichain.
//
// # Usage
//
//	go run . -w single     -o build/single
//	go run . -w multichain -o build/multichain
//
// The generated tree is what `harness deploy build` turns into container images
// (docs/BATCH1-PLAN.md WP4) and what gate K0a greps to prove the three codegen
// hooks reached every generated container.
package main

import (
	"github.com/blueprint-uservices/blueprint/examples/predicted/wiring/specs"
	"github.com/blueprint-uservices/blueprint/plugins/cmdbuilder"
	"github.com/blueprint-uservices/blueprint/plugins/workflow/workflowspec"
)

func main() {
	// Configure the location of our workflow spec
	workflowspec.AddModule("github.com/blueprint-uservices/blueprint/examples/predicted/workflow")

	cmdbuilder.MakeAndExecute(
		"Predicted",
		specs.Single,
		specs.Multichain,
	)
}
