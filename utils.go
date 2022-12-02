package gocosmos

const (
	httpHeaderContentType   = "Content-Type"
	httpHeaderAccept        = "Accept"
	httpHeaderAuthorization = "Authorization"
	httpHeaderIfMatch       = "If-Match"
	httpHeaderIfNoneMatch   = "If-None-Match"

	restApiHeaderVersion                        = "x-ms-version"
	restApiHeaderDate                           = "x-ms-date"
	restApiHeaderOfferThroughput                = "x-ms-offer-throughput"
	restApiHeaderOfferAutopilotSettings         = "x-ms-cosmos-offer-autopilot-settings"
	restApiHeaderIsUpsert                       = "x-ms-documentdb-is-upsert"
	restApiHeaderIndexingDirective              = "x-ms-indexing-directive"
	restApiHeaderPartitionKey                   = "x-ms-documentdb-partitionkey"
	restApiHeaderPartitionKeyRangeId            = "x-ms-documentdb-partitionkeyrangeid"
	restApiHeaderConsistencyLevel               = "x-ms-consistency-level"
	restApiHeaderSessionToken                   = "x-ms-session-token"
	restApiHeaderContinuation                   = "x-ms-continuation"
	restApiHeaderPageSize                       = "x-ms-max-item-count"
	restApiHeaderEnableCrossPartitionQuery      = "x-ms-documentdb-query-enablecrosspartition"
	restApiHeaderParallelizeCrossPartitionQuery = "x-ms-documentdb-query-parallelizecrosspartitionquery"
	restApiHeaderIsQuery                        = "x-ms-documentdb-isquery"
	restApiHeaderMigrateToManualThroughput      = "x-ms-cosmos-migrate-offer-to-manual-throughput"
	restApiHeaderMigrateToAutopilotThroughput   = "x-ms-cosmos-migrate-offer-to-autopilot"

	restApiParamIndexingPolicy  = "indexingPolicy"
	restApiParamUniqueKeyPolicy = "uniqueKeyPolicy"
	restApiParamPartitionKey    = "partitionKey"
	restApiParamQuery           = "query"
	restApiParamParameters      = "parameters"
	restApiParamContent         = "content"

	respHeaderRequestCharge = "X-MS-REQUEST-CHARGE"
	respHeaderSessionToken  = "X-MS-SESSION-TOKEN"
	respHeaderContinuation  = "X-MS-CONTINUATION"
	respHeaderEtag          = "ETAG"

	docFieldId = "id"
)
