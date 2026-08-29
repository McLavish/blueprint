module github.com/blueprint-uservices/blueprint/examples/predicted/wiring

go 1.25

require (
	github.com/blueprint-uservices/blueprint/blueprint v0.0.0-20240619221802-d064c5861c1e
	github.com/blueprint-uservices/blueprint/examples/predicted/workflow v0.0.0
	github.com/blueprint-uservices/blueprint/plugins v0.0.0-20240619221802-d064c5861c1e
)

replace github.com/blueprint-uservices/blueprint/examples/predicted/workflow => ../workflow
