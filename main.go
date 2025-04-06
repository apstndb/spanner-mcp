package main

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/apstndb/lox"
	"github.com/go-viper/mapstructure/v2"
	"github.com/golang/protobuf/proto"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/olekukonko/tablewriter"
	"github.com/samber/lo"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/types/descriptorpb"
	"github.com/apstndb/spannerplanviz/plantree"
	"github.com/apstndb/spannerplanviz/queryplan"
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
		mcp.WithDescription("Get execution plan for the query. The first content is machine-readable prototext format of QueryPlan message. The second content is human-readable rendered query plan."),
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
		mcp.WithDescription("Get DDL of the database. The first content is the whole response, and the second content is unmarshalled proto_descriptors (optional)."),
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
		mcp.WithBoolean("include_proto_descriptors",
			mcp.DefaultBool(false),
			mcp.Description("Enable only if proto_descriptors is needed."),
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

	plan := queryplan.New(qp.GetPlanNodes())
	processed, err := plantree.ProcessPlan(plan)
	if err != nil {
		return nil, err
	}

	result, err := printResult(processed)
	if err != nil {
		return nil, err
	}

	return &mcp.CallToolResult{
		Content: []mcp.Content{
			mcp.NewTextContent(prototext.Format(qp)),
			mcp.NewTextContent(result),
		},
	}, nil
}

func getDDLHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	req, err := mapToStruct[struct {
		Project                 string
		Instance                string
		Database                string
		IncludeProtoDescriptors bool
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

	var contents []mcp.Content

	if req.IncludeProtoDescriptors {
		contents = append(contents, mcp.NewTextContent(prototext.Format(resp)))
		contents = append(contents, mcp.NewTextContent(prototext.Format(&fds)))
	} else {
		resp.ProtoDescriptors = nil
		contents = append(contents, mcp.NewTextContent(prototext.Format(resp)))
	}

	return &mcp.CallToolResult{
		Content: contents,
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

func printResult(rows []plantree.RowWithPredicates) (string, error) {
	var b strings.Builder
	table := tablewriter.NewWriter(&b)
	table.SetAutoFormatHeaders(false)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetColumnAlignment([]int{tablewriter.ALIGN_RIGHT, tablewriter.ALIGN_LEFT})
	table.SetAutoWrapText(false)

	for _, row := range rows {
		table.Append([]string{row.FormatID(), row.Text()})
	}
	table.SetHeader([]string{"ID", "Operator"})
	if len(rows) > 0 {
		table.Render()
	}

	var maxIDLength int
	for _, row := range rows {
		if length := len(fmt.Sprint(row.ID)); length > maxIDLength {
			maxIDLength = length
		}
	}

	var predicates []string
	var parameters []string
	for _, row := range rows {
		var prefix string
		for i, predicate := range row.Predicates {
			if i == 0 {
				prefix = fmt.Sprintf("%*d:", maxIDLength, row.ID)
			} else {
				prefix = strings.Repeat(" ", maxIDLength+1)
			}
			predicates = append(predicates, fmt.Sprintf("%s %s", prefix, predicate))
		}

		i := 0
		for _, t := range lox.EntriesSortedByKey(row.ChildLinks) {
			typ, childLinks := t.Key, t.Value
			if typ == "" {
				continue
			}

			if i == 0 {
				prefix = fmt.Sprintf("%*d:", maxIDLength, row.ID)
			} else {
				prefix = strings.Repeat(" ", maxIDLength+1)
			}

			join := strings.Join(lo.Map(childLinks, func(item *queryplan.ResolvedChildLink, index int) string {
				if varName := item.ChildLink.GetVariable(); varName != "" {
					return fmt.Sprintf("$%s=%s", item.ChildLink.GetVariable(), item.Child.GetShortRepresentation().GetDescription())
				} else {
					return item.Child.GetShortRepresentation().GetDescription()
				}
			}), ", ")
			if join == "" {
				continue
			}
			i++
			typePartStr := lo.Ternary(typ != "", typ+": ", "")
			parameters = append(parameters, fmt.Sprintf("%s %s%s", prefix, typePartStr, join))
		}
	}

	if len(predicates) > 0 {
		b.WriteString("Predicates(identified by ID):\n")
		for _, s := range predicates {
			b.WriteString(fmt.Sprintf(" %s\n", s))
		}
	}
	return b.String(), nil
}
