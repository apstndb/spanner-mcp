package main

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	"github.com/go-viper/mapstructure/v2"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"google.golang.org/protobuf/encoding/prototext"
)

func main() {
	// Create MCP server
	s := server.NewMCPServer(
		"Spanner MCP",
		"0.1.0",
	)

	// Add tool
	tool := mcp.NewTool("plan",
		mcp.WithDescription("Get execution plan for the query"),
		mcp.WithString("query",
			mcp.Required(),
			mcp.Description("query text of SQL or GQL"),
		),
		mcp.WithString("project",
			mcp.Required(),
			mcp.Description("Google Cloud project"),
		),
		mcp.WithString("instance",
			mcp.Required(),
			mcp.Description("Spanner instance id"),
		),
		mcp.WithString("database",
			mcp.Required(),
			mcp.Description("Spanner database id"),
		),
	)

	// Add tool handler
	s.AddTool(tool, planHandler)

	// Start the stdio server
	if err := server.ServeStdio(s); err != nil {
		fmt.Printf("Server error: %v\n", err)
	}
}

func planHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	type planRequest struct {
		Query    string
		Project  string
		Instance string
		Database string
	}

	var req planRequest
	err := mapstructure.Decode(request.Params.Arguments, &req)
	if err != nil {
		return nil, err
	}

	client, err := spanner.NewClient(ctx, databasePath(req.Project, req.Instance, req.Database))
	if err != nil {
		return nil, err
	}
	defer client.Close()

	qp, err := client.Single().AnalyzeQuery(ctx, spanner.NewStatement(req.Query))
	if err != nil {
		return nil, err
	}
	return mcp.NewToolResultText(prototext.Format(qp)), nil
}

func databasePath(project string, instance string, database string) string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, database)
}
