package main

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/go-viper/mapstructure/v2"
	"github.com/golang/protobuf/proto"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/types/descriptorpb"
)

func mapToStruct[T any](m map[string]any) (T, error) {
	var zero T
	var result T
	if err := mapstructure.Decode(m, &result); err != nil {
		return zero, err
	}
	return result, nil
}

func main() {
	// Create MCP server
	s := server.NewMCPServer(
		"Spanner MCP",
		"0.1.0",
	)

	// Add tool
	plan := mcp.NewTool("plan",
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

	getDDL := mcp.NewTool("get_ddl",
		mcp.WithDescription("Get DDL of the database"),
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

	updateDDL := mcp.NewTool("update_ddl",
		mcp.WithDescription("Update DDL of the database"),
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
		mcp.WithArray("statements",
			mcp.Required(),
			mcp.Description("DDL statements"),
		),
	)

	// Add plan handler
	s.AddTool(plan, planHandler)
	s.AddTool(getDDL, getDDLHandler)
	s.AddTool(updateDDL, updateDDLHandler)

	// Start the stdio server
	if err := server.ServeStdio(s); err != nil {
		fmt.Printf("Server error: %v\n", err)
	}
}

func planHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	req, err := mapToStruct[struct {
		Query    string
		Project  string
		Instance string
		Database string
	}](request.Params.Arguments)

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

func getDDLHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	req, err := mapToStruct[struct {
		Project  string
		Instance string
		Database string
	}](request.Params.Arguments)
	if err != nil {
		return nil, err
	}

	client, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	resp, err := client.GetDatabaseDdl(ctx, &databasepb.GetDatabaseDdlRequest{
		Database: databasePath(req.Project, req.Instance, req.Database),
	})
	if err != nil {
		return nil, err
	}

	var fds descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(resp.GetProtoDescriptors(), &fds); err != nil {
		return nil, err
	}

	return &mcp.CallToolResult{
		Content: []mcp.Content{
			mcp.NewTextContent(prototext.Format(resp)),
			mcp.NewTextContent(prototext.Format(&fds)),
		},
	}, nil
}

func updateDDLHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	req, err := mapToStruct[struct {
		Project    string
		Instance   string
		Database   string
		Statements []string
	}](request.Params.Arguments)
	if err != nil {
		return nil, err
	}

	client, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	resp, err := client.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   databasePath(req.Project, req.Instance, req.Database),
		Statements: req.Statements,
	})
	if err != nil {
		return nil, err
	}
	err = resp.Wait(ctx)
	if err != nil {
		return nil, err
	}

	metadata, err := resp.Metadata()
	if err != nil {
		return nil, err
	}

	return mcp.NewToolResultText(prototext.Format(metadata)), nil
}

func databasePath(project string, instance string, database string) string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, database)
}
