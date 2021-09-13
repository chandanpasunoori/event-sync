package pkg

type PubsubConfig struct {
	ProjectId              string `json:"projectId"`
	SubscriptionId         string `json:"subscriptionId"`
	MaxOutstandingMessages int    `json:"maxOutstandingMessages"`
	AttributeKeyName       string `json:"attributeKeyName"`
}
type Source struct {
	Type         string       `json:"type"`
	PubsubConfig PubsubConfig `json:"pubsubConfig"`
}
type BigqueryConfig struct {
	ProjectId string `json:"projectId"`
	Dataset   string `json:"dataset"`
}
type Destination struct {
	Type           string         `json:"type"`
	BatchSize      int            `json:"batchSize"`
	BigqueryConfig BigqueryConfig `json:"bigqueryConfig"`
}
type Target struct {
	Table string `json:"table"`
}
type Filter struct {
	Type   string `json:"type"`
	Name   string `json:"name"`
	Action string `json:"action"`
	Target Target `json:"target,omitempty"`
}
type Job struct {
	Name        string      `json:"name"`
	Suspend     bool        `json:"suspend"`
	Source      Source      `json:"source"`
	Filters     []Filter    `json:"filters"`
	Destination Destination `json:"destination,omitempty"`
}
type Config struct {
	Jobs []Job `json:"jobs"`
}
