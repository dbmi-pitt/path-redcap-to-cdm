CREATE TABLE REDCAP_EVENT_MAPPING (
	PROJECT_ID VARCHAR2(20) NOT NULL,
	ARM_NUM VARCHAR2(20) NOT NULL,
	UNIQUE_EVENT_NAME VARCHAR2(100) NOT NULL,
	MODIFIER_CD VARCHAR2(50) NOT NULL
);
