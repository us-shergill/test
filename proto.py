

--2026SAR_Mailing_Fulfillment_Contract_PBP_Segment_CCYYMMDD.csv
--Extract SQL work in progress
SELECT
MBR.MEMCODNUM --N/A
,MBR.MemberID as "Member ID"
,MBR.CurrentEffDate as LatestEffectiveDate --N/A 
,MBR.TermDate as TermDate --N/A
,MBRS.Description as CurrentStatus --N/A 
,DMG.OECCounty --N/A
,DMG.SCC1 as DMGSCC1 --N/A
,DMG.SCC2 as DMGSCC2 --N/A
,DMG.FirstName as FirstName
,DMG.LastName as LastName
,PhyADDR.Address1 as PhyAddr1 --N/A
,PhyADDR.Address2 as PhyAddr2 --N/A
,PhyADDR.City as PhyCity --N/A
,PhyADDR.State as PhyState --N/A
,CASE 
  WHEN PhyADDR.ZipFour IS NULL THEN PhyADDR.Zip 
  ELSE PhyADDR.Zip||'-'||PhyADDR.ZipFour 
 END as PhyZip --N/A
,MailADDR.Address1 as MailAddr1
,MailADDR.Address2 as MailAddr2
,MailADDR.City as MailCity
,MailADDR.State as MailState
,CASE 
  WHEN MailADDR.ZipFour IS NULL THEN MailADDR.Zip 
  ELSE MailADDR.Zip||'-'||MailADDR.ZipFour  
 END as MailZip
,MBR.PlanID as CContract
,MBR.PBP as CPBP
,COALESCE(EAMSGMNT.SegmentID,MBR.SegmentID,'000') as CSegment
,EAMSGMNT.Span_EffDate
,EAMSGMNT.Span_TermDate
,PLN.ProductName as "Plan Name"
,COALESCE(SPAN.SPANSCC,DMG.SCC1) as SCCCode
,SPAN.SPANSCC --N/A 
,DMG.SCC1 --N/A 
,COALESCE(DMG."Language",'ENG') as LanguageText
,CASE 
	WHEN DMG.AccessibilityFormat = 1 THEN 'Braille'
	WHEN DMG.AccessibilityFormat = 2 THEN 'Large Print'
	WHEN DMG.AccessibilityFormat = 3 THEN 'Audio CD'
	WHEN DMG.AccessibilityFormat = 4 THEN 'Data CD'
	ELSE '' 
	END AS "Alternate Format"
,SC.CountyName as County
,PhyADDR.State as PhysicalState
,SC.State as CountyState --N/A
,MBOM.State as "Plan State"
,MemberID||'_SAR_'||(CURRENT_DATE (FORMAT 'YYYYMMDD')) as "Record ID"
,MBOM.RecordType as "Record Type"
,MBOM.LetterMaterialID as "Material ID"
,MBOM.PLANReplacementID as "Plan Replacement ID"

FROM (
	SELECT
	MemberID, MemCodNum, PlanID, PBP, SegmentID, SRC_DATA_KEY, CurrentEffDate, TermDate, MemberStatus
	FROM GBS_FACET_CORE_V.EAM_tbEENRLMembers
	WHERE SRC_DATA_KEY = '210'
	and cast(substr(TermDate,1,10) as date format 'YYYY-MM-DD') > current_date --To exclude termed members 
	QUALIFY ROW_NUMBER() OVER (PARTITION BY MemCodNum ORDER BY CurrentEffDate DESC) = 1) MBR

	JOIN GBS_FACET_CORE_V.EAM_tbMemberInfo DMG
	ON 	MBR.SRC_DATA_KEY = DMG.SRC_DATA_KEY 
	AND MBR.MemCodNum = DMG.MemCodNum
	--AND MBR.PBP NOT LIKE '8%' --Exclude EGWP

	JOIN GBS_FACET_CORE_V.EAM_tbMemberStatus MBRS
	ON MBR.MemberStatus = MBRS.Status
	--AND MBR.MemberStatus in ('1','2') --Awaiting business confirmation 
	
	LEFT JOIN 
	(
		Select 
				tbe.PlanID,
				tbe.MemCodNum,
				tbe.HIC AS SpanMBINumber,
				tbe.SPANTYPE AS SpanType,
				tbe."Value" AS SpanValue,
				CAST(tbe.STARTDATE AS DATE) AS Span_EffDate,
				CAST(tbe.ENDDATE AS DATE) AS Span_TermDate,
				CAST(tbe.LastModifiedDate AS DATE) AS LAST_MODIFIED,
				CAST(tbe.DateCreated AS DATE) AS CREATE_DATE,
				Tbt.DateCreated AS TR_CREATE_DATE,
				--SegmentId population preference: SEGC, SEGD, Transactions, Default000
				COALESCE(NULLIF(TRIM(tbe.SEGC_SegmentID), ''),NULLIF(TRIM(tbe.SEGD_SegmentID), ''),NULLIF(TRIM(tbt.SegmentID), ''), '000') AS SegmentID,
				SEGC_startDate,SEGC_EndDate,SEGD_startDate,SEGD_EndDate
                   FROM (
                   --Adding Value from SEGC,SEGD spans as SegmentID to PBP span
                   select  d."Value" as SEGD_SegmentID,d.StartDate as SEGD_startDate, d.EndDate as SEGD_EndDate, c.* from
                   (select  b."Value" as SEGC_SegmentID, b.StartDate as SEGC_startDate, b.EndDate as SEGC_EndDate,
                          a.* from GBS_FACET_CORE_V.EAM_tbENRLSpans a LEFT JOIN GBS_FACET_CORE_V.EAM_tbENRLSpans b
                           ON a.MemCodNum = b.MemCodNum and a.PlanID = b.PlanID and (b.StartDate between  a.StartDate and a.EndDate) and a.SPANTYPE = 'PBP' and b.SPANTYPE='SEGC' ) c 
               LEFT JOIN GBS_FACET_CORE_V.EAM_tbENRLSpans d
                                 ON c.MemCodNum = d.MemCodNum and c.PlanID = d.PlanID and ((SEGC_startDate is not null and c.SEGC_startDate = d.StartDate) or (SEGC_startDate is null and c.StartDate= d.StartDate)) and c.SPANTYPE = 'PBP' and d.SPANTYPE='SEGD'
                     ) tbe  LEFT JOIN GBS_FACET_CORE_V.EAM_tbTransactions tbt
                                        ON tbt.MemCodNum = tbe.MemCodNum
                                          AND tbt.PlanID = tbe.PlanID
                                          AND tbt.PBPID = tbe."Value"
                                          AND (tbe.StartDate <= tbt.EffectiveDate AND tbe.EndDate >= tbt.EffectiveDate)
                                          AND ((tbt.TransCode = '61') OR (tbt.TransCode IN ('80') AND tbt.ReplyCodes = '287'))
                                          AND tbt.TransStatus IN (5)
                                          WHERE tbe.SpanType = 'PBP'
                                          QUALIFY ROW_NUMBER() OVER (PARTITION BY tbe.MemCodNum, tbe.PlanID, tbe."Value" ORDER BY Span_EffDate DESC, Span_TermDate desc) = 1
                                          ) EAMSGMNT
										  
	ON MBR.MEMCODNUM = EAMSGMNT.MEMCODNUM
	AND MBR.PlanID = EAMSGMNT.PlanID
	AND MBR.PBP = EAMSGMNT.SpanValue

	JOIN GBS_FACET_CORE_V.EAM_tbPlan_PBP PLN
	ON MBR.PlanID = PLN.PlanID
	AND MBR.PBP = PLN.PBPID

	LEFT JOIN (
	SELECT 
	MemCodNum, Address1, Address2, City, State, Zip, ZipFour, SRC_DATA_KEY
	FROM GBS_FACET_CORE_V.EAM_MemberManagerAddress
	WHERE AddressUse = '1'
	QUALIFY ROW_NUMBER() OVER (PARTITION BY MemCodNum ORDER BY StartDate DESC) = 1) PhyADDR 
	ON MBR.SRC_DATA_KEY = PhyADDR.SRC_DATA_KEY 
	AND MBR.MemCodNum = PhyADDR.MemCodNum

	LEFT JOIN (
	SELECT 
	MemCodNum, Address1, Address2, City, State, Zip, ZipFour, SRC_DATA_KEY
	FROM GBS_FACET_CORE_V.EAM_MemberManagerAddress
	WHERE AddressUse = '2'
	QUALIFY ROW_NUMBER() OVER (PARTITION BY MemCodNum ORDER BY StartDate DESC) = 1) MailADDR 
	ON MBR.SRC_DATA_KEY = MailADDR.SRC_DATA_KEY 
	AND MBR.MemCodNum = MailADDR.MemCodNum
	
	LEFT JOIN (
	select 
	memcodnum, "value" as SPANSCC  
	FROM GBS_FACET_CORE_V.EAM_tbENRLSpans
	WHERE spantype = 'SCC'
	qualify row_number() over (partition by memcodnum order by startdate desc)=1) span
	ON dmg.memcodnum = span.memcodnum

	JOIN VT_SAR_PLAN SAR
	ON MBR.PlanID = SAR.Contract
	AND MBR.PBP = SAR.PBP
	AND CSegment = SAR.Segment
	AND SCCCode = SAR.ServiceAreaCountyCode

	LEFT JOIN GBS_FACET_CORE_V.EAM_TB_EAM_SCC_STATES SC
	ON SCCCode = SC.SCC_CODE
	
	LEFT JOIN 
	(
		SELECT distinct STATE_ABBREVIATED_NAME, STATE_NAME from REFDATA_CORE_V.STATE_COUNTY) STREF
	ON SC.State = STREF.STATE_ABBREVIATED_NAME 

	LEFT JOIN VT_SAR_NR_BOM_2026 MBOM
	ON MBR.PlanID = MBOM.Contract
	AND MBR.PBP = MBOM.PBP
	AND CSegment = MBOM.Segment
	AND STREF.STATE_NAME = MBOM.State;
	
	--SAR Fallout and Exclusion
select 
Case 
when physicalstate <> countystate Then 'Fallout, Physical Address State '||physicalstate||', SCC State '||CountyState
when MailAddr1 is NULL Then 'Fallout, no mailing address'
when "Member ID" is NULL Then 'Fallout, no member ID'
when FirstName is NULL Then 'Fallout, no first name'
when LastName is NULL Then 'Fallout, no last name'
when MailCity is NULL Then 'Fallout, no mail address city'
when MailState is NULL Then 'Fallout, no mail address state'
when MailZip is NULL Then 'Fallout, no mail address zip'
When "Plan Name" is NULL Then 'Fallout, no plan name'
When PhyState is NULL Then 'Fallout, no physical address state'
when "Material ID" is NULL Then 'BOM, Missing Data'
--when CurrentStatus = 'Not Enrolled' and RIGHT("Member ID",2)='XX' Then 'Do not report, not enrolled'
when CurrentStatus = 'Not Enrolled' Then 'Do not report, not enrolled'
when CurrentStatus = 'Pending' and cast(substr(LatestEffectiveDate,1,10) as date format 'YYYY-MM-DD') < current_date and Span_EffDate IS NULL 
	Then 'Do not report, member effective date is in the past, has no span and is considered canceled'
when CurrentStatus = 'Pending' and cast(substr(LatestEffectiveDate,1,10) as date format 'YYYY-MM-DD') >= current_date and Span_EffDate IS NULL 
	Then 'Fallout, member status pending with effective date in future with no span'
when CurrentStatus = 'Pending' Then 'Valid, status pending and effectivedate in future with span'
Else 'Valid' end as Comments,
SAR.*
from hslabgrowthrpt.TEST_SAR_AB_VT SAR;


--NR Extract SQL 
SELECT
MBR.MEMCODNUM --N/A
,MBR.MemberID as "Member ID"
,MBR.CurrentEffDate as LatestEffectiveDate --N/A 
,MBR.TermDate as TermDate --N/A
,MBRS.Description as CurrentStatus --N/A 
,DMG.OECCounty --N/A
,DMG.SCC1 as DMGSCC1 --N/A
,DMG.SCC2 as DMGSCC2 --N/A
,DMG.FirstName as FirstName
,DMG.LastName as LastName
,PhyADDR.Address1 as PhyAddr1 --N/A
,PhyADDR.Address2 as PhyAddr2 --N/A
,PhyADDR.City as PhyCity --N/A
,PhyADDR.State as PhyState --N/A
,CASE 
  WHEN PhyADDR.ZipFour IS NULL THEN PhyADDR.Zip 
  ELSE PhyADDR.Zip||'-'||PhyADDR.ZipFour 
 END as PhyZip --N/A
,MailADDR.Address1 as MailAddr1
,MailADDR.Address2 as MailAddr2
,MailADDR.City as MailCity
,MailADDR.State as MailState
,CASE 
  WHEN MailADDR.ZipFour IS NULL THEN MailADDR.Zip 
  ELSE MailADDR.Zip||'-'||MailADDR.ZipFour  
 END as MailZip
,MBR.PlanID as CContract
,MBR.PBP as CPBP
,COALESCE(EAMSGMNT.SegmentID,MBR.SegmentID,'000') as CSegment
,EAMSGMNT.Span_EffDate
,EAMSGMNT.Span_TermDate
,PLN.ProductName as "Plan Name"
,COALESCE(SPAN.SPANSCC,DMG.SCC1) as SCCCode
,SPAN.SPANSCC --N/A
,DMG.SCC1 --N/A
,COALESCE(DMG."Language",'ENG') as LanguageText
,CASE 
	WHEN DMG.AccessibilityFormat = 1 THEN 'Braille'
	WHEN DMG.AccessibilityFormat = 2 THEN 'Large Print'
	WHEN DMG.AccessibilityFormat = 3 THEN 'Audio CD'
	WHEN DMG.AccessibilityFormat = 4 THEN 'Data CD'
	ELSE '' 
	END AS "Alternate Format"
,SC.CountyName as County
,PhyADDR.State as PhysicalState
,SC.State as CountyState --N/A
--,NR.State as NRInclusionState
,COALESCE(BOMWITHST.State,BOMNOST.State) as "Plan State"
,MemberID||'_NR_'||(CURRENT_DATE (FORMAT 'YYYYMMDD')) as "Record ID"
,COALESCE(BOMWITHST.RecordType,BOMNOST.RecordType) as "Record Type"
,COALESCE(BOMWITHST.LetterMaterialID,BOMNOST.LetterMaterialID) as "Material ID"
,COALESCE(BOMWITHST.PLANReplacementID,BOMNOST.PLANReplacementID) as "Plan Replacement ID"

FROM (
	SELECT
	MemberID, MemCodNum, PlanID, PBP, SegmentID, SRC_DATA_KEY, CurrentEffDate, TermDate, MemberStatus
	FROM GBS_FACET_CORE_V.EAM_tbEENRLMembers
	WHERE SRC_DATA_KEY = '210'
	and cast(substr(TermDate,1,10) as date format 'YYYY-MM-DD') > current_date --To exclude termed members 
	QUALIFY ROW_NUMBER() OVER (PARTITION BY MemCodNum ORDER BY CurrentEffDate DESC) = 1) MBR

	JOIN GBS_FACET_CORE_V.EAM_tbMemberInfo DMG
	ON 	MBR.SRC_DATA_KEY = DMG.SRC_DATA_KEY 
	AND MBR.MemCodNum = DMG.MemCodNum
	--AND MBR.PBP NOT LIKE '8%' --Exclude EGWP

	JOIN GBS_FACET_CORE_V.EAM_tbMemberStatus MBRS
	ON MBR.MemberStatus = MBRS.Status
	--AND MBR.MemberStatus in ('1','2') --Awaiting business confirmation 
	
	LEFT JOIN 
	(
		Select 
				tbe.PlanID,
				tbe.MemCodNum,
				tbe.HIC AS SpanMBINumber,
				tbe.SPANTYPE AS SpanType,
				tbe."Value" AS SpanValue,
				CAST(tbe.STARTDATE AS DATE) AS Span_EffDate,
				CAST(tbe.ENDDATE AS DATE) AS Span_TermDate,
				CAST(tbe.LastModifiedDate AS DATE) AS LAST_MODIFIED,
				CAST(tbe.DateCreated AS DATE) AS CREATE_DATE,
				Tbt.DateCreated AS TR_CREATE_DATE,
				--SegmentId population preference: SEGC, SEGD, Transactions, Default000
				COALESCE(NULLIF(TRIM(tbe.SEGC_SegmentID), ''),NULLIF(TRIM(tbe.SEGD_SegmentID), ''),NULLIF(TRIM(tbt.SegmentID), ''), '000') AS SegmentID,
				SEGC_startDate,SEGC_EndDate,SEGD_startDate,SEGD_EndDate
                   FROM (
                   --Adding Value from SEGC,SEGD spans as SegmentID to PBP span
                   select  d."Value" as SEGD_SegmentID,d.StartDate as SEGD_startDate, d.EndDate as SEGD_EndDate, c.* from
                   (select  b."Value" as SEGC_SegmentID, b.StartDate as SEGC_startDate, b.EndDate as SEGC_EndDate,
                          a.* from GBS_FACET_CORE_V.EAM_tbENRLSpans a LEFT JOIN GBS_FACET_CORE_V.EAM_tbENRLSpans b
                           ON a.MemCodNum = b.MemCodNum and a.PlanID = b.PlanID and (b.StartDate between  a.StartDate and a.EndDate) and a.SPANTYPE = 'PBP' and b.SPANTYPE='SEGC' ) c 
               LEFT JOIN GBS_FACET_CORE_V.EAM_tbENRLSpans d
                                 ON c.MemCodNum = d.MemCodNum and c.PlanID = d.PlanID and ((SEGC_startDate is not null and c.SEGC_startDate = d.StartDate) or (SEGC_startDate is null and c.StartDate= d.StartDate)) and c.SPANTYPE = 'PBP' and d.SPANTYPE='SEGD'
                     ) tbe  LEFT JOIN GBS_FACET_CORE_V.EAM_tbTransactions tbt
                                        ON tbt.MemCodNum = tbe.MemCodNum
                                          AND tbt.PlanID = tbe.PlanID
                                          AND tbt.PBPID = tbe."Value"
                                          AND (tbe.StartDate <= tbt.EffectiveDate AND tbe.EndDate >= tbt.EffectiveDate)
                                          AND ((tbt.TransCode = '61') OR (tbt.TransCode IN ('80') AND tbt.ReplyCodes = '287'))
                                          AND tbt.TransStatus IN (5)
                                          WHERE tbe.SpanType = 'PBP'
                                          QUALIFY ROW_NUMBER() OVER (PARTITION BY tbe.MemCodNum, tbe.PlanID, tbe."Value" ORDER BY Span_EffDate DESC, Span_TermDate desc) = 1
                                          ) EAMSGMNT
										  
	ON MBR.MEMCODNUM = EAMSGMNT.MEMCODNUM
	AND MBR.PlanID = EAMSGMNT.PlanID
	AND MBR.PBP = EAMSGMNT.SpanValue

	JOIN GBS_FACET_CORE_V.EAM_tbPlan_PBP PLN
	ON MBR.PlanID = PLN.PlanID
	AND MBR.PBP = PLN.PBPID

	LEFT JOIN (
	SELECT 
	MemCodNum, Address1, Address2, City, State, Zip, ZipFour, SRC_DATA_KEY
	FROM GBS_FACET_CORE_V.EAM_MemberManagerAddress
	WHERE AddressUse = '1'
	QUALIFY ROW_NUMBER() OVER (PARTITION BY MemCodNum ORDER BY StartDate DESC) = 1) PhyADDR 
	ON MBR.SRC_DATA_KEY = PhyADDR.SRC_DATA_KEY 
	AND MBR.MemCodNum = PhyADDR.MemCodNum

	LEFT JOIN (
	SELECT 
	MemCodNum, Address1, Address2, City, State, Zip, ZipFour, SRC_DATA_KEY
	FROM GBS_FACET_CORE_V.EAM_MemberManagerAddress
	WHERE AddressUse = '2'
	QUALIFY ROW_NUMBER() OVER (PARTITION BY MemCodNum ORDER BY StartDate DESC) = 1) MailADDR 
	ON MBR.SRC_DATA_KEY = MailADDR.SRC_DATA_KEY 
	AND MBR.MemCodNum = MailADDR.MemCodNum
	
	LEFT JOIN (
	select 
	memcodnum, "value" as SPANSCC  
	FROM GBS_FACET_CORE_V.EAM_tbENRLSpans
	WHERE spantype = 'SCC'
	qualify row_number() over (partition by memcodnum order by startdate desc)=1) span
	ON dmg.memcodnum = span.memcodnum

	LEFT JOIN GBS_FACET_CORE_V.EAM_TB_EAM_SCC_STATES SC
	ON SCCCode = SC.SCC_CODE
	
	JOIN (SELECT Contract, PBP, Segment FROM VT_NR_PLAN group by 1,2,3) NR
	ON MBR.PlanID = NR.Contract
	AND MBR.PBP = NR.PBP
	--AND SC.State = NR.State
	AND CSegment = NR.Segment
	
	LEFT JOIN 
	(
		SELECT distinct STATE_ABBREVIATED_NAME, STATE_NAME from REFDATA_CORE_V.STATE_COUNTY) STREF
	ON PhyADDR.State = STREF.STATE_ABBREVIATED_NAME 

	LEFT JOIN VT_SAR_NR_BOM_2026 BOMWITHST
	ON MBR.PlanID = BOMWITHST.Contract
	AND MBR.PBP = BOMWITHST.PBP
	AND CSegment = BOMWITHST.Segment
	AND STREF.STATE_NAME = BOMWITHST.State
	
	LEFT JOIN (SELECT * 
	FROM VT_SAR_NR_BOM_2026 
	QUALIFY row_number() OVER (PARTITION BY Contract, PBP, Segment Order by State) = 1) BOMNOST
	ON MBR.PlanID = BOMNOST.Contract
	AND MBR.PBP = BOMNOST.PBP
	AND CSegment = BOMNOST.Segment;
	
--NR Validation with comments 
sel 
CASE 
when physicalstate <> countystate Then 'Fallout, Physical Address State '||physicalstate||', SCC State '||CountyState
when MailAddr1 is NULL Then 'Fallout, no mailing address'
when "Member ID" is NULL Then 'Fallout, no member ID'
when FirstName is NULL Then 'Fallout, no first name'
when LastName is NULL Then 'Fallout, no last name'
when MailCity is NULL Then 'Fallout, no mail address city'
when MailState is NULL Then 'Fallout, no mail address state'
when MailZip is NULL Then 'Fallout, no mail address zip'
When "Plan Name" is NULL Then 'Fallout, no plan name'
When PhyState is NULL Then 'Fallout, no physical address state'
when "Material ID" is NULL Then 'BOM, Missing Data'
--when CurrentStatus = 'Not Enrolled' and RIGHT("Member ID",2)='XX' Then 'Do not report, not enrolled'
when CurrentStatus = 'Not Enrolled' Then 'Do not report, not enrolled'
when CurrentStatus = 'Pending' and cast(substr(LatestEffectiveDate,1,10) as date format 'YYYY-MM-DD') < current_date and Span_EffDate IS NULL 
	Then 'Do not report, member effective date is in the past, has no span and is considered canceled'
when CurrentStatus = 'Pending' and cast(substr(LatestEffectiveDate,1,10) as date format 'YYYY-MM-DD') >= current_date and Span_EffDate IS NULL 
	Then 'Fallout, member status pending with effective date in future with no span'
when CurrentStatus = 'Pending' Then 'Valid, status pending and effectivedate in future with span'
when countystate is null then 'Fallout, no SCC'
else 'Valid'
end as comments, VT.*
from HSLABGROWTHRPT.TEST_NR_AB_VT VT;	



