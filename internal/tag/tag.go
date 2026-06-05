package tag

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// Tag represents a Snowflake tag definition.
type Tag struct {
	Name          string
	Database      string
	Schema        string
	AllowedValues []string // optional restricted values
	Comment       string
	CreatedAt     time.Time
}

// TagAssignment represents a tag applied to an object.
type TagAssignment struct {
	TagName    string
	TagValue   string
	ObjectType string // TABLE, COLUMN, SCHEMA, DATABASE
	ObjectName string
	ColumnName string // only if ObjectType == COLUMN
}

// TagInfo is the read-only representation returned by ShowTags.
type TagInfo struct {
	Name          string
	Database      string
	Schema        string
	AllowedValues []string
	Comment       string
	CreatedAt     time.Time
}

// Manager manages tag definitions and assignments.
type Manager struct {
	mu          sync.RWMutex
	tags        map[string]*Tag            // key: DB.SCHEMA.TAG_NAME
	assignments map[string][]TagAssignment // key: object identifier
}

func tagKey(db, schema, name string) string {
	return strings.ToUpper(db) + "." + strings.ToUpper(schema) + "." + strings.ToUpper(name)
}

func objectKey(objectType, objectName, columnName string) string {
	k := strings.ToUpper(objectType) + ":" + strings.ToUpper(objectName)
	if columnName != "" {
		k += "." + strings.ToUpper(columnName)
	}
	return k
}

// NewManager creates a new tag manager.
func NewManager() *Manager {
	return &Manager{
		tags:        make(map[string]*Tag),
		assignments: make(map[string][]TagAssignment),
	}
}

// CreateTag creates a new tag definition.
func (m *Manager) CreateTag(db, schema, name string, allowedValues []string, comment string) error {
	key := tagKey(db, schema, name)

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.tags[key]; exists {
		return fmt.Errorf("tag %s already exists", key)
	}

	m.tags[key] = &Tag{
		Name:          strings.ToUpper(name),
		Database:      strings.ToUpper(db),
		Schema:        strings.ToUpper(schema),
		AllowedValues: allowedValues,
		Comment:       comment,
		CreatedAt:     time.Now(),
	}
	return nil
}

// DropTag removes a tag definition and all its assignments.
func (m *Manager) DropTag(db, schema, name string) error {
	key := tagKey(db, schema, name)
	upperName := strings.ToUpper(name)

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.tags[key]; !exists {
		return fmt.Errorf("tag %s does not exist", key)
	}

	delete(m.tags, key)

	// Remove all assignments for this tag.
	for objKey, assignments := range m.assignments {
		filtered := assignments[:0]
		for _, a := range assignments {
			if a.TagName != upperName {
				filtered = append(filtered, a)
			}
		}
		if len(filtered) == 0 {
			delete(m.assignments, objKey)
		} else {
			m.assignments[objKey] = filtered
		}
	}
	return nil
}

// SetTag assigns a tag value to an object. Validates against AllowedValues if set.
func (m *Manager) SetTag(tagDB, tagSchema, tagName, objectType, objectName, columnName, value string) error {
	tKey := tagKey(tagDB, tagSchema, tagName)
	oKey := objectKey(objectType, objectName, columnName)

	m.mu.Lock()
	defer m.mu.Unlock()

	tag, exists := m.tags[tKey]
	if !exists {
		return fmt.Errorf("tag %s does not exist", tKey)
	}

	// Validate allowed values.
	if len(tag.AllowedValues) > 0 {
		found := false
		for _, av := range tag.AllowedValues {
			if strings.EqualFold(av, value) {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("value %q is not in allowed values for tag %s", value, tKey)
		}
	}

	upperTagName := strings.ToUpper(tagName)

	// Remove existing assignment for this tag on this object if any.
	existing := m.assignments[oKey]
	for i, a := range existing {
		if a.TagName == upperTagName {
			m.assignments[oKey] = append(existing[:i], existing[i+1:]...)
			break
		}
	}

	m.assignments[oKey] = append(m.assignments[oKey], TagAssignment{
		TagName:    upperTagName,
		TagValue:   value,
		ObjectType: strings.ToUpper(objectType),
		ObjectName: strings.ToUpper(objectName),
		ColumnName: strings.ToUpper(columnName),
	})
	return nil
}

// UnsetTag removes a tag assignment from an object.
func (m *Manager) UnsetTag(tagDB, tagSchema, tagName, objectType, objectName, columnName string) error {
	tKey := tagKey(tagDB, tagSchema, tagName)
	oKey := objectKey(objectType, objectName, columnName)

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.tags[tKey]; !exists {
		return fmt.Errorf("tag %s does not exist", tKey)
	}

	upperTagName := strings.ToUpper(tagName)
	existing := m.assignments[oKey]
	for i, a := range existing {
		if a.TagName == upperTagName {
			m.assignments[oKey] = append(existing[:i], existing[i+1:]...)
			if len(m.assignments[oKey]) == 0 {
				delete(m.assignments, oKey)
			}
			return nil
		}
	}
	return fmt.Errorf("tag %s is not set on %s", tKey, oKey)
}

// GetTagValue returns the value of a specific tag on an object.
func (m *Manager) GetTagValue(objectType, objectName, columnName, tagName string) (string, error) {
	oKey := objectKey(objectType, objectName, columnName)
	upperTagName := strings.ToUpper(tagName)

	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, a := range m.assignments[oKey] {
		if a.TagName == upperTagName {
			return a.TagValue, nil
		}
	}
	return "", fmt.Errorf("tag %s not found on object %s", upperTagName, oKey)
}

// GetAllTags returns all tag assignments for an object.
func (m *Manager) GetAllTags(objectType, objectName string) []TagAssignment {
	oKey := objectKey(objectType, objectName, "")

	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]TagAssignment, len(m.assignments[oKey]))
	copy(result, m.assignments[oKey])
	return result
}

// ShowTags returns info for all tags in the given database and schema.
func (m *Manager) ShowTags(db, schema string) []TagInfo {
	prefix := strings.ToUpper(db) + "." + strings.ToUpper(schema) + "."

	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []TagInfo
	for key, tag := range m.tags {
		if strings.HasPrefix(key, prefix) {
			result = append(result, TagInfo{
				Name:          tag.Name,
				Database:      tag.Database,
				Schema:        tag.Schema,
				AllowedValues: tag.AllowedValues,
				Comment:       tag.Comment,
				CreatedAt:     tag.CreatedAt,
			})
		}
	}
	return result
}
