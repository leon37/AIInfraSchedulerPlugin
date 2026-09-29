package apis

type JobType string

const (
	JobTypeTrain     JobType = "Train"
	JobTypeInference JobType = "Inference"
)
