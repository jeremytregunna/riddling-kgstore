package query

import (
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name        string
		queryStr    string
		wantType    QueryType
		wantParams  map[string]string
		wantErr     bool
		wantErrText string
	}{
		{
			name:     "Find nodes by label",
			queryStr: "FIND_NODES_BY_LABEL(label: \"Person\")",
			wantType: QueryTypeFindNodesByLabel,
			wantParams: map[string]string{
				"label": "Person",
			},
			wantErr: false,
		},
		{
			name:     "Find edges by label",
			queryStr: "FIND_EDGES_BY_LABEL(label: \"KNOWS\")",
			wantType: QueryTypeFindEdgesByLabel,
			wantParams: map[string]string{
				"label": "KNOWS",
			},
			wantErr: false,
		},
		{
			name:     "Find neighbors",
			queryStr: "FIND_NEIGHBORS(nodeId: \"1\", direction: \"outgoing\")",
			wantType: QueryTypeFindNeighbors,
			wantParams: map[string]string{
				"nodeId":    "1",
				"direction": "outgoing",
			},
			wantErr: false,
		},
		{
			name:     "Find path",
			queryStr: "FIND_PATH(sourceId: \"1\", targetId: \"2\", maxHops: \"3\")",
			wantType: QueryTypeFindPath,
			wantParams: map[string]string{
				"sourceId": "1",
				"targetId": "2",
				"maxHops":  "3",
			},
			wantErr: false,
		},
		{
			name:     "JSON format",
			queryStr: `{"type": "FIND_NODES_BY_LABEL", "parameters": {"label": "Person"}}`,
			wantType: QueryTypeFindNodesByLabel,
			wantParams: map[string]string{
				"label": "Person",
			},
			wantErr: false,
		},
		{
			name:        "Empty query",
			queryStr:    "",
			wantErr:     true,
			wantErrText: "invalid query: empty query",
		},
		{
			name:        "Invalid query type",
			queryStr:    "INVALID_QUERY(label: \"Person\")",
			wantErr:     true,
			wantErrText: "invalid query: unknown query type",
		},
		{
			name:        "Missing required parameter for nodes by label",
			queryStr:    "FIND_NODES_BY_LABEL()",
			wantErr:     true,
			wantErrText: "invalid query: missing required parameter 'label'",
		},
		{
			name:        "Missing required parameter for neighbors",
			queryStr:    "FIND_NEIGHBORS(direction: \"outgoing\")",
			wantErr:     true,
			wantErrText: "invalid query: missing required parameter 'nodeId'",
		},
		{
			name:        "Invalid direction parameter",
			queryStr:    "FIND_NEIGHBORS(nodeId: \"1\", direction: \"invalid\")",
			wantErr:     true,
			wantErrText: "invalid query: invalid direction parameter",
		},
		{
			name:        "Missing parameters for path",
			queryStr:    "FIND_PATH(sourceId: \"1\")",
			wantErr:     true,
			wantErrText: "invalid query: missing required parameter 'targetId'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.queryStr)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				if err == nil || !contains(err.Error(), tt.wantErrText) {
					t.Errorf("Parse() error = %v, want error containing %v", err, tt.wantErrText)
				}
				return
			}
			if got.Type != tt.wantType {
				t.Errorf("Parse() got type = %v, want %v", got.Type, tt.wantType)
			}
			if len(got.Parameters) != len(tt.wantParams) {
				t.Errorf("Parse() got %d parameters, want %d", len(got.Parameters), len(tt.wantParams))
			}
			for k, v := range tt.wantParams {
				if got.Parameters[k] != v {
					t.Errorf("Parse() parameter %s = %v, want %v", k, got.Parameters[k], v)
				}
			}
		})
	}
}

func TestQuery_String(t *testing.T) {
	tests := []struct {
		name  string
		query *Query
		want  string
	}{
		{
			name: "Find nodes by label",
			query: &Query{
				Type: QueryTypeFindNodesByLabel,
				Parameters: map[string]string{
					"label": "Person",
				},
			},
			want: `FIND_NODES_BY_LABEL(label: "Person")`,
		},
		{
			name: "Find neighbors",
			query: &Query{
				Type: QueryTypeFindNeighbors,
				Parameters: map[string]string{
					"nodeId":    "1",
					"direction": "outgoing",
				},
			},
			want: `FIND_NEIGHBORS(nodeId: "1", direction: "outgoing")`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.query.String()
			// Note: The order of parameters in a map is not guaranteed,
			// so we check for the presence of key parts instead of exact equality
			if !contains(got, string(tt.query.Type)) ||
				!contains(got, "(") ||
				!contains(got, ")") {
				t.Errorf("Query.String() = %v, want to contain %v", got, tt.want)
			}
			for k, v := range tt.query.Parameters {
				if !contains(got, k) || !contains(got, v) {
					t.Errorf("Query.String() = %v, want to contain key %v and value %v", got, k, v)
				}
			}
		})
	}
}

// contains checks if a string contains a substring
func contains(s, substr string) bool {
	return s != "" && substr != "" && strings.Contains(s, substr)
}