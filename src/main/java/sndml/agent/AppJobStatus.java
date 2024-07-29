package sndml.agent;

import com.fasterxml.jackson.annotation.JsonFormat;

@JsonFormat(with = JsonFormat.Feature.ACCEPT_CASE_INSENSITIVE_VALUES)
public enum AppJobStatus {
	DRAFT,
	SCHEDULED,
	READY,
	PREPARE,
	RUNNING,
	COMPLETE,
	RENAME,
	CANCELLED,
	FAILED
}
