-- Insert data from CTE
/*
INSERT INTO `data-migration-staging`.migrate_leads_v6 (
    leadId, firstName, middleName, lastName, mobilePhone, email, sundeskUserId,
    idNumber, companyRegionId, leadConvertedDate, paymentMethod, purchaseDate,
    agentProviderId, status, leadStatus, leadCategory, referralId, productOfInterest,
    preferredLanguage, agentId, createdById, lastUpdatedById, throughPartnerLeadId,
    isReshuffleLead, createdAt, updatedAt, phoneNumber, alternatePhoneNumber,
    name, kraPinNumber, companyName, source, paymentTerms, leadAmtCustomerId,
    entityType, customerTypeId, leadChannelId, referralType, employeeReferralId,
    isActive, kycId, tdhId, deletedAt, deletedBy, leadSourceId, formId,
    formVersion, archivedAt, archivedBy, lastModifiedAt, lastModifiedBy, is_migrated
)*/
/*
INSERT INTO `data-migration-staging`.migrate_kyc_requests_v4 (
    externalRefId, leadId, idNumber, serialNumber, dob, status, description, 
    companyRegionId, meta, createdBy, updatedBy, createdAt, updatedAt, 
    documentType_temp, documentType, callbackJsonBlob, smileJobId, resultCode, 
    resultText, actions, source, gender, description2, isActive, formId, 
    formVersion, deletedAt, deletedBy, archivedAt, archivedBy, lastModifiedAt, 
    lastModifiedBy, is_migrated
)*/
INSERT INTO `data-migration-staging`.migrate_next_of_kin_details_v4 (
  sourceSystemId, sourceSystem,leadId, firstName,lastName,phoneNumber,
  alternativePhoneNumber,gender,idNumber,relationship,type,
  isActive,createdAt,createdBy,updatedAt,updatedBy,is_migrated
)
with
sf_leads_cte as (
	SELECT _salesforce_id,
	createddate, 
	lastmodifieddate, 
	lead_date_created_c,
	converteddate, 
	createdbyid, 
	lastmodifiedbyid, 
	id, 
	lastname, 
	firstname, 
	name, 
	mobilephone, 
	mobilenumberwithcountrycode_c, 
	other_phone_c, 
	COALESCE(leadsource, 'Others') as leadsource,
	COALESCE(lead_channel_c, 'Others') as lead_channel_c,
	status, 
	isconverted, 
	convertedaccountid,  
	#acreage_c, 
	date_of_birth_c, 
	gender_c, 
	lead_amt_customer_id_c, 
	lead_category_c, 
	#location_c, 
	payment_method_c, 
	preferred_language_c, 
	purchase_date_c, 
	#water_source_distance_c, 
	#water_source_c, 
	customer_type_c, 
	id_number_c, 
	#kyc_status_c, 
	agent_phone_number_c, 
	agent_c, 
	referral_name_c, 
	referral_id_c, 
	#daily_water_usage_c, 
	#total_dynamic_head_c, 
	smileidentity_json_c, 
	referral_phone_number_c, 
	amt_customer_name_c, 
	agent_employee_number_c, 
	kra_pin_c, 
	customer_product_of_interest_c, 
	#referral_source_application_c, 
	through_partner_lead_c, 
	through_partner_customer_c, 
	referral_lead_id_c, 
	#cds1tracker_c, cds_status_c, survey_stat_c,sadm_account_c, 
	employee_id_c, 
	employee_phone_c, 
	is_lead_employed_c,
	#auto_assignment_date_c, 
	extagentreferral_code_c, 
	extagentprovider_region_c, 
	extagentprovider_name_c, 
	extagentphone_number_c, 
	extagentname_c, 
	extagentid_c,
	kyc_status_c,
	cds_status_c,
	sadm_cds_id_c, 
	sadm_cds1_date_c,
	sadm_cds2_date_c 
	FROM `data-migration-staging`.sf_lead_v12
	),
amt_refferals_cte as (
	SELECT Leadid, referralid
	FROM `data-migration-staging`.refferals
	),
stg_sf_agents_cte as (
	SELECT sf_agent_id, 
	amt_agent_employee_id
FROM `data-migration-staging`.stg_sf_agents_v2
),
stg_sf_users_cte as (
	SELECT sf_user_id,  
	amt_user_employee_id
	FROM `data-migration-staging`.stg_sf_users_v3
	),
sf_raw_cds_cte as (
	select distinct createddate,
	lastmodifieddate,
	createdbyid,
	lastmodifiedbyid,
	lead_record,
	next_of_kin_name,
	next_of_kin_surname,
	relation_with_next_of_kin,
	share_name_phone_of_next_of_kin,
	next_of_kin_phone_number,
	ROW_NUMBER() OVER (
                PARTITION BY lead_record ORDER BY lastmodifieddate DESC
            ) as rn
	FROM `data-migration-staging`.sf_cds_v2
),
sf_cds_cte as (
	select distinct createddate,
	lastmodifieddate,
	createdbyid,
	lastmodifiedbyid,
	lead_record,
	next_of_kin_name,
	next_of_kin_surname,
	relation_with_next_of_kin,
	share_name_phone_of_next_of_kin,
	next_of_kin_phone_number
	from sf_raw_cds_cte
	where rn = 1
),
amt_employees_cte as (
	SELECT id, 
	email, 
	trim(identificationNumber) as identificationNumber, 
	mobileMoneyPhoneNumber, 
	phoneNumber, 
	salesForceAgentId
FROM `data-migration-staging`.stg_amt_employees
),
amt_customers_cte as (
	SELECT id, identification_number, phone_number, sales_agents, alternative_phone_number, sales_force_id
	FROM `data-migration-staging`.stg_amtdb_customers
	),
amt_accounts_cte as (
	SELECT id, 
	customer_id, 
	account_ref, 
	status, 
	full_deposit_date, 
	sales_agents
	FROM `data-migration-staging`.stg_amtdb_accounts
	),
amt_customers_accounts_agg_cte as (
	select distinct customer_id,
	count(distinct id) as amt_accounts_count,
	count(distinct account_ref) as amt_account_ref_count
	from amt_accounts_cte
	group by 1
	),
amt_customer_referrals_cte as (
	SELECT referral_id, 
	#agent_id, 
	REPLACE(customer_phone_number, '+', '') as customer_phone_number,
	#customer_id_number, 
	agent_provider_id 
	FROM `data-migration-staging`.amt_customer_referrals
),
sales_service_leadsources_cte as (
	SELECT id, name 
	FROM `data-migration-staging`.ss_leadsources
	),
sales_service_lead_channels_cte as (
	SELECT id, name 
	FROM `data-migration-staging`.ss_lead_channels
	),
sf_leads_mashup_cte as (
	select distinct 
	sf_leads_cte.createddate as sf_created_date,
	sf_leads_cte.lastmodifieddate as sf_last_modified_date,
	sf_leads_cte.lead_date_created_c as sf_lead_date_created,
	sf_leads_cte.converteddate as sf_converted_date,
	sf_leads_cte.purchase_date_c as sf_purchase_date_c,
	sf_leads_cte.createdbyid as sf_created_by_id,
	coalesce(sf_leads_cte.employee_id_c, stg_sf_agents_cte.amt_agent_employee_id) as amt_lead_created_by_id,
	sf_leads_cte.lastmodifiedbyid as sf_last_modified_by_id,
	get_lead_last_updated_by.amt_user_employee_id as amt_last_modified_id,
	# agents
	sf_leads_cte.agent_c as sf_agent_c,
	coalesce(sf_leads_cte.employee_id_c, stg_sf_agents_cte.amt_agent_employee_id) as amt_agent_id,
	sf_leads_cte.employee_id_c as employee_id_c,
	#sf_leads_cte.agent_phone_number_c as sf_agent_phone_number, 
	#sf_leads_cte.agent_employee_number_c,
	#amt_employees_cte.identificationNumber as amt_employees_identification_number,
	#sf_agents_cte.mobilephone as sf_agents_mobilephone,
	# lead
	sf_leads_cte.id as sf_lead_id,
	amt_refferals_cte.referralid,
	sf_leads_cte.isconverted,
	lead_category_c,
	sf_leads_cte.status as sf_status,
	sf_leads_cte.firstname,
	sf_leads_cte.lastname,
	sf_leads_cte.name as name,
	sf_leads_cte.kra_pin_c,
	sf_leads_cte.leadsource as leadsource,
	sales_service_leadsources_cte.id as ss_leadsource_id,
	sf_leads_cte.lead_channel_c as sf_lead_channel, 
	case when sf_leads_cte.lead_channel_c = 'SALESAPP' then 8 else sales_service_lead_channels_cte.id end as ss_lead_channel_id,
	# customer
	sf_leads_cte.customer_type_c,
	sf_leads_cte.id_number_c,
	date_of_birth_c,
	sf_leads_cte.mobilenumberwithcountrycode_c as sf_mobile_number_with_country_code_c,  
	sf_leads_cte.other_phone_c as sf_other_phone_c, 
	amt_customers_cte.phone_number as amt_customer_phone_number,
	#lead_amt_customer_id_c,
	amt_customers_cte.id as amt_customer_id,
	gender_c as sf_gender,
	sf_leads_cte.payment_method_c,
	sf_leads_cte.customer_product_of_interest_c,
	sf_leads_cte.preferred_language_c,
	#extagentid_c,
	#extagentname_c,
	#extagentphone_number_c,
	#extagentprovider_name_c,
	#extagentprovider_region_c,
	#extagentreferral_code_c,
	##case when sf_leads_cte.leadsource = 'iPOs' then amt_customer_referrals_cte.agent_provider_id else null end as agent_provider_id,
	#amt_customer_referrals_cte.agent_provider_id ,
	#case when amt_customers_cte.id is not null then 'YES' else 'NO' end as customer_id_number_in_amt,
	#case when amt_accounts_count > 0 then 'YES' else 'NO' end as customer_has_amt_account,
	#amt_employees_cte.name as amt_employees_name,
	#amt_departments_cte.name as amt_employee_department,
	# smile identity
	smileidentity_json_c,
	smileidentity_json_c->>'$.SmileJobID' as smile_identity_smile_job_id,
	smileidentity_json_c->>'$.ResultCode' as smile_identity_result_code,
	smileidentity_json_c->>'$._status_' as smile_identity_status,
	smileidentity_json_c->>'$.IDType' as smile_identity_id_type,
	smileidentity_json_c->>'$.IDNumber' as smile_identity_id_number,
	CASE 
        WHEN smileidentity_json_c->>'$.DOB' = 'null' THEN NULL
        WHEN smileidentity_json_c->>'$.DOB' IS NULL THEN NULL
        WHEN smileidentity_json_c->>'$.DOB' = '' THEN NULL
        ELSE smileidentity_json_c->>'$.DOB'
    END as smile_identity_dob,
	smileidentity_json_c->>'$.ResultType' as smile_identity_result_type,
	# CDS
	sf_cds_cte.createddate as cds_created_date,
	sf_cds_cte.lastmodifieddate as cds_last_modified_date,
	sf_cds_cte.createdbyid as cds_created_by_id,
	get_cds_created_by_id.amt_user_employee_id as cds_amt_created_by_id,
	sf_cds_cte.lastmodifiedbyid as cds_last_modified_by_id,
	get_cds_updated_by_id.amt_user_employee_id as cds_last_modified_by_amt_id,
	# Next Of Kin
	sf_cds_cte.next_of_kin_name,
	sf_cds_cte.next_of_kin_surname,
	sf_cds_cte.relation_with_next_of_kin,
	sf_cds_cte.share_name_phone_of_next_of_kin,
	sf_cds_cte.next_of_kin_phone_number,
	# SADM
	kyc_status_c,
	cds_status_c,
	sadm_cds_id_c, 
	sadm_cds1_date_c,
	sadm_cds2_date_c 
	from sf_leads_cte
	left join amt_refferals_cte on amt_refferals_cte.LeadId = sf_leads_cte.id COLLATE utf8mb4_0900_ai_ci
	left join sf_cds_cte on sf_cds_cte.lead_record = sf_leads_cte.id
	left join stg_sf_agents_cte on stg_sf_agents_cte.sf_agent_id = sf_leads_cte.agent_c
	left join stg_sf_users_cte on stg_sf_users_cte.sf_user_id = sf_leads_cte.createdbyid 
	left join stg_sf_users_cte as get_lead_last_updated_by on get_lead_last_updated_by.sf_user_id = sf_leads_cte.lastmodifiedbyid
	left join stg_sf_users_cte as get_cds_created_by_id on get_cds_created_by_id.sf_user_id = sf_cds_cte.createdbyid 
	left join stg_sf_users_cte as get_cds_updated_by_id on get_cds_updated_by_id.sf_user_id = sf_cds_cte.createdbyid 
	#left join sf_agents_cte on sf_agents_cte.id = sf_leads_cte.agent_c
	#left join amt_employees_cte as get_amt_leads_created_by on get_amt_leads_created_by.identificationNumber = sf_agents_cte.employee_id_number COLLATE utf8mb4_0900_ai_ci
	#left join sf_user_cte as sf_last_modified_by on sf_last_modified_by.id = sf_leads_cte.lastmodifiedbyid
	#left join amt_employees_cte as amt_last_modified_by on amt_last_modified_by.email = sf_last_modified_by.email COLLATE utf8mb4_0900_ai_ci
	#left join sf_user_cte as cds_created_by on cds_created_by.id = sf_cds_cte.createdbyid COLLATE utf8mb4_0900_ai_ci
	#left join amt_employees_cte as amt_cds_created_by_id on amt_cds_created_by_id.email = cds_created_by.email COLLATE utf8mb4_0900_ai_ci
	#left join sf_user_cte as cds_modified_by on cds_modified_by.id =  sf_cds_cte.lastmodifiedbyid COLLATE utf8mb4_0900_ai_ci
	#left join amt_employees_cte as get_cds_modified_by on get_cds_modified_by.email = cds_modified_by.email COLLATE utf8mb4_0900_ai_ci
	#left join amt_employees_cte on amt_employees_cte.salesForceAgentId COLLATE utf8mb4_0900_ai_ci = sf_leads_cte.agent_c
	left join amt_customers_cte on amt_customers_cte.identification_number = sf_leads_cte.id_number_c
	#left join amt_customers_accounts_agg_cte on amt_customers_accounts_agg_cte.customer_id = amt_customers_cte.id
	#left join amt_customer_referrals_cte on amt_customer_referrals_cte.customer_phone_number = sf_leads_cte.mobilenumberwithcountrycode_c
	left join sales_service_leadsources_cte on sales_service_leadsources_cte.name = sf_leads_cte.leadsource COLLATE utf8mb4_0900_ai_ci
	left join sales_service_lead_channels_cte on sales_service_lead_channels_cte.name = sf_leads_cte.lead_channel_c COLLATE utf8mb4_0900_ai_ci
	#where sf_leads_cte.id = '00QPz00000L0SOPMA3'
	#where mobilenumberwithcountrycode_c = '254723146129'
	#where mobilenumberwithcountrycode_c = '254724094412' #Duplicated Lead
	#where sf_agent_c = 'a05Pz00000AfezpIAB' # agent with at least 1 converted
	#where smileidentity_json_c is not null
	#where leadsource is not null # check if there are sales with null leadsources
	order by sf_created_date desc
	),
-- leads to be migrated
migrate_leads_cte as (
	select 
	#id, # DB Generated
	sf_leads_mashup_cte.sf_lead_id as leadId,
	COALESCE(sf_leads_mashup_cte.firstname, sf_leads_mashup_cte.name) as firstName,
	NULL  as middleName,
	sf_leads_mashup_cte.lastname as lastName,
	sf_leads_mashup_cte.sf_mobile_number_with_country_code_c as mobilePhone,
	NULL as email,
	NULL as sundeskUserId,
	sf_leads_mashup_cte.id_number_c as idNumber,
	1 as companyRegionId,
	sf_leads_mashup_cte.sf_converted_date as leadConvertedDate,
	COALESCE(sf_leads_mashup_cte.payment_method_c, 'CASH') as paymentMethod, # should accept NULL values
	case
		when sf_leads_mashup_cte.sf_purchase_date_c = 'Now' then 'NOW'
		when sf_leads_mashup_cte.sf_purchase_date_c in ('Later','Two Weeks', 'Two Months', 'Default')  then 'LATER'
		when sf_leads_mashup_cte.sf_purchase_date_c is null then 'LATER'
	else 'LATER' end as purchaseDate,
	NULL as agent_provider_id,  # from Agent Provider Table (for iPos Leads)
	#sf_leads_mashup_cte.agent_provider_id as agentProviderId, # from Agent Provider Table (for iPos Leads)
	case
		when sf_status = 'New' then 'NEW'
		when sf_status = 'Converted' then 'CONVERTED'
		when sf_status = 'Qualified' then 'QUALIFIED'
	else 'NEW' end as  status, # Question: shoud Qualified status be mapped to NEW or null?
	case 
		when isconverted = 0 then 'LEAD_CREATION'
		when isconverted = 1 then 'CONVERTED'
	else null end as leadStatus, # Question:
	case 
		 WHEN sf_leads_mashup_cte.lead_category_c = 'Hot' then 'HOT'
		 WHEN sf_leads_mashup_cte.lead_category_c = 'Cold' then 'COLD'
		 WHEN sf_leads_mashup_cte.lead_category_c = 'Warm' then 'WARM'
	else 'WARM' end as leadCategory,
	referralid  as referralId,
	sf_leads_mashup_cte.customer_product_of_interest_c as productOfInterest,
	sf_leads_mashup_cte.preferred_language_c as preferredLanguage,
	sf_leads_mashup_cte.amt_agent_id as agentId,
	sf_leads_mashup_cte.amt_lead_created_by_id as createdById,
	sf_leads_mashup_cte.amt_last_modified_id as lastUpdatedById,
	sf_leads_mashup_cte.sf_lead_id as throughPartnerLeadId, # Lead Id
	case when leadsource = 'Reshuffled Leads' then 1 else 0 end as isReshuffleLead,
	sf_leads_mashup_cte.sf_created_date as createdAt,
	sf_leads_mashup_cte.sf_last_modified_date as updatedAt,
	sf_leads_mashup_cte.sf_mobile_number_with_country_code_c as phoneNumber, # To Be Ignored
	#case when sf_leads_mashup_cte.sf_other_phone_c = '0741992946/0791304665' then '0741992946' else sf_leads_mashup_cte.sf_other_phone_c end as alternatePhoneNumber,
	NULL as alternatePhoneNumber, # to be updated
	sf_leads_mashup_cte.name as name,
	sf_leads_mashup_cte.kra_pin_c as kraPinNumber,
	NULL  as companyName, # not being used
	sf_leads_mashup_cte.leadsource as source,
	NULL  as paymentTerms,
	sf_leads_mashup_cte.amt_customer_id as leadAmtCustomerId, # from AMT
	'INDIVIDUAL' as entityType,  # setting this as default
	sf_leads_mashup_cte.customer_type_c as customerTypeId,
	ss_lead_channel_id as leadChannelId,
	case 
		when leadsource = 'Engineer Referral' then 'EMPLOYEE'
		when leadsource = 'iPOs' then 'IPOS'
		when leadsource in ('Refer & Earn', 'Refer and Earn') then 'CUSTOMER'
	else null end as referralType,
	case 
		when leadsource = 'Engineer Referral' then employee_id_c 
	else null end as employeeReferralId, #
	1 as isActive,
	NULL as kycId,
	NULL  as tdhId,
	NULL  as deletedAt,
	NULL as deletedBy,
	sf_leads_mashup_cte.ss_leadsource_id as leadSourceId,
	NULL  as formId,
	NULL  as formVersion,
	NULL  as archivedAt,
	NULL  as archivedBy,
	sf_leads_mashup_cte.sf_last_modified_date as lastModifiedAt,
	sf_leads_mashup_cte.amt_last_modified_id as lastModifiedBy,
	1 as is_migrated
	from sf_leads_mashup_cte
	),
migrate_kyc_requests_cte as (
	select
	#id # DB Auto generated
	concat('sf-', smile_identity_smile_job_id) as externalRefId, # Random id e.g, (sf-)
	sf_leads_mashup_cte.sf_lead_id as leadId,
	COALESCE(sf_leads_mashup_cte.smile_identity_id_number, sf_leads_mashup_cte.id_number_c) as idNumber,
	NULL  as serialNumber, # What is the source?
	case
		when smile_identity_dob = 'Not Available' then sf_leads_mashup_cte.date_of_birth_c
	else COALESCE(smile_identity_dob, sf_leads_mashup_cte.date_of_birth_c) end as dob,
	case 
		when smile_identity_status = 'COMPLETED' then 'SUCCESS' 
		when smile_identity_status = 'FAILED' then 'FAILED'
	else 'PENDING' end as status, # Default Value: # clarification
	NULL  as description,
	1 as companyRegionId,
	smileidentity_json_c as meta,
	sf_leads_mashup_cte.amt_agent_id as createdBy,
	case 
		when sf_leads_mashup_cte.amt_last_modified_id = 'brian onyango' then sf_leads_mashup_cte.amt_lead_created_by_id 
	else sf_leads_mashup_cte.amt_last_modified_id end as updatedBy,
	sf_leads_mashup_cte.sf_created_date as createdAt,
	sf_leads_mashup_cte.sf_last_modified_date as updatedAt,
	NULL  as documentType_temp,
	smile_identity_id_type as documentType,
	NULL  as callbackJsonBlob,
	smile_identity_smile_job_id as smileJobId,
	smile_identity_result_code as resultCode,
	NULL  as resultText, # Clrify source
	NULL  as actions, # clrify source
	smile_identity_result_type as source,
	sf_gender as gender, # Question: Okay if we pick from root 
	NULL  as description2,
	1 as isActive,
	NULL  as formId,
	NULL  as formVersion,
	NULL  as deletedAt,
	NULL  as deletedBy,
	NULL  as archivedAt,
	NULL  as archivedBy,
	sf_leads_mashup_cte.sf_last_modified_date as lastModifiedAt, # This a duplicate - we have updatedAt
	sf_leads_mashup_cte.amt_last_modified_id as lastModifiedBy,
	1 as is_migrated
	from sf_leads_mashup_cte
	),
migrate_next_of_kin_details_cte as (
	select
	#id # DB auto geenrated
	'SALESFORCE'  as sourceSystemId,
	NULL  as sourceSystem,
	sf_leads_mashup_cte.sf_lead_id as leadId,
	coalesce(sf_leads_mashup_cte.next_of_kin_name,sf_leads_mashup_cte.next_of_kin_surname, 'Not Specified') as firstName,
	coalesce(sf_leads_mashup_cte.next_of_kin_surname, sf_leads_mashup_cte.next_of_kin_name, 'Not Specified')  as lastName,
	sf_leads_mashup_cte.next_of_kin_phone_number as phoneNumber,
	NULL  as alternativePhoneNumber, # No Aternative Phone Number For Next of Kin
	NULL  as gender, # All NULL
	NULL  as id_number_c, # Next of kin ID numbers not being captured
	case
		when (sf_leads_mashup_cte.relation_with_next_of_kin is null)  then 'Other'
		when sf_leads_mashup_cte.relation_with_next_of_kin in ('Child (son/daughter)', 'Parent', 'Grandparents') then 'Other'
		when sf_leads_mashup_cte.relation_with_next_of_kin in ('Wife', 'Husband') then 'Spouse'
	else sf_leads_mashup_cte.relation_with_next_of_kin end as relationship,
	'NEXT_OF_KIN' as type,
	1 as isActive,
	cds_created_date as createdAt,
	CASE
		when cds_amt_created_by_id = 'brian onyango' then 895
		else coalesce(cds_amt_created_by_id,cds_last_modified_by_amt_id,895)
	END as createdBy, # clean this later - null values for amt created by
	cds_last_modified_date as updatedAt,
	cds_last_modified_by_amt_id as updatedBy,
	1 as is_migrated
	from sf_leads_mashup_cte
	)
select *
#from sf_leads_mashup_cte 
#from migrate_leads_cte
#from migrate_kyc_requests_cte where (idNumber is not null) and (dob is not null) and (externalRefId is not null)
from migrate_next_of_kin_details_cte where (phoneNumber is not null)