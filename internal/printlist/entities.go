package printlist

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/jedib0t/go-pretty/table"
)

type Entity interface {
	PrintableFields() []TableField
}

type TableField struct {
	Name        string
	PrettyName  string
	Value       string
	PrettyValue *string
}

func List(entitiesInOrder []Entity, output io.Writer, pretty, json bool) error {
	if json {
		return listInJSON(entitiesInOrder, output, pretty)
	}
	if pretty {
		listInASCIITable(entitiesInOrder, output)
		return nil
	}
	return listInTabbedTable(entitiesInOrder, output)
}

// listInJSON prints entities in JSON format. All fields that aren't hidden by json tags are printed, not just ones
// returned from Entity.PrintableFields. If pretty flag is set, JSONs are indented.
func listInJSON(entities []Entity, output io.Writer, pretty bool) error {
	encoder := json.NewEncoder(output)
	if pretty {
		encoder.SetIndent("", "    ")
	}
	err := encoder.Encode(entities)
	if err != nil {
		return fmt.Errorf("encode to JSON: %w", err)
	}
	return nil
}

// listInASCIITable prints entities in a human-readable table with clearly visible columns and pretty-formatted fields.
// Rows are also numbered.
func listInASCIITable(entities []Entity, output io.Writer) {
	if len(entities) == 0 {
		return
	}

	writer := table.NewWriter()
	writer.SetOutputMirror(output)
	defer writer.Render()

	firstEntityFields := entities[0].PrintableFields()
	headerRow := table.Row{"#"}
	for _, field := range firstEntityFields {
		headerRow = append(headerRow, any(field.PrettyName))
	}
	writer.AppendHeader(headerRow)

	for i, e := range entities {
		row := table.Row{i}
		for _, field := range e.PrintableFields() {
			if field.PrettyValue != nil {
				row = append(row, *field.PrettyValue)
			} else {
				row = append(row, field.Value)
			}
		}
		writer.AppendRow(row)
	}
}

// listInASCIITable prints entities in a table aligned by tabs with simply formatted fields.
func listInTabbedTable(entities []Entity, output io.Writer) error {
	if len(entities) == 0 {
		return nil
	}

	writer := tabwriter.NewWriter(output, 0, 0, 1, ' ', 0)
	defer func() { _ = writer.Flush() }()

	firstEntityFields := entities[0].PrintableFields()
	fieldNames := make([]string, len(firstEntityFields))
	for i := range firstEntityFields {
		fieldNames[i] = firstEntityFields[i].Name
	}
	header := strings.Join(fieldNames, "\t")
	_, err := fmt.Fprintln(writer, header)
	if err != nil {
		return err
	}

	for _, entity := range entities {
		fields := entity.PrintableFields()
		vals := make([]string, 0, len(fields))
		for _, field := range fields {
			vals = append(vals, field.Value)
		}
		_, err = fmt.Fprintln(writer, strings.Join(vals, "\t"))
		if err != nil {
			return err
		}
	}
	return nil
}
