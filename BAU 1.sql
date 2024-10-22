--- ACCOUNT denotes raw_veeva.account_sf1
with ACCOUNT as (
Select * from raw_veeva.account_sf1
       where recordtypeid in ('012460000015LxGAAU','01246000000u0DwAAI','01246000000u0E0AAI')
),CONTACT as (  
    --- CONTACT denotes raw_veeva.contact_info_sf1
    SELECT Row_number() OVER (partition BY account_mvn__c ORDER BY lastmodifieddate DESC) r,
               *
        FROM raw_veeva.contact_info_sf1
        WHERE primary_mvn__c = 1
              AND (
                      recordtypeid = '01246000000NGFNAA4'
                      AND zip_mvn__c IS NOT NULL
                  )
),BENEFIT_COVERAGE_1 as(  ---BENEFIT_COVERAGE_1 denotes raw_patient_services.benefits_coverage_sf1
SELECT hzn_pm_id__c FROM raw_patient_services.benefits_coverage_sf1 WHERE (
                                                    hzn_plan_coverage_type__c NOT LIKE '%Commercial%'
                                                    OR hzn_plan_coverage_type__c IS NULL)
                                                    AND status_mvn__c = 'Active'
),BENEFIT_COVERAGE_2 as(  ---BENEFIT_COVERAGE_2 denotes raw_patient_services.benefits_coverage_sf1
    SELECT hzn_pm_id__c FROM raw_patient_services.benefits_coverage_sf1 WHERE status_mvn__c = 'Active'
)
SELECT DISTINCT TOP 1024
    Concat(bc.NAME, ' ', pm.NAME) AS "Customer BV Task ID",
    'prod' AS "Program ID",
    CASE WHEN(
            pm.status_mvn__c = 'Pending'
            AND pm.status_reason_mvn__c = 'Benefits Investigation'
            AND pm.program_name_mvn__c = 'KRYSTEXXA'
        ) THEN
            'INF_FACILITY_TYPE_SPECIALIST_OFFICE'
        WHEN
        (
            pm.status_mvn__c = 'Pending'
            AND pm.status_reason_mvn__c = 'Benefits Investigation'
            AND pm.program_name_mvn__c = 'TEPRO'
        ) THEN
            'INF_FACILITY_TYPE_INFUSION_CENTER'
        WHEN
        (
            NOT (
                    pm.status_mvn__c = 'Pending'
                    AND pm.status_reason_mvn__c = 'Benefits Investigation'
                )
            AND (e.type LIKE '%Independent Site of Infusion%')
        ) THEN
            'INF_FACILITY_TYPE_INFUSION_CENTER'
        WHEN
        (
            NOT (
                    pm.status_mvn__c = 'Pending'
                    AND pm.status_reason_mvn__c = 'Benefits Investigation'
                )
            AND (e.type LIKE '%Specialty Pharmacy Infusion%')
        ) THEN
            'INF_FACILITY_TYPE_PHARMACY'
        WHEN
        (
            NOT (
                    pm.status_mvn__c = 'Pending'
                    AND pm.status_reason_mvn__c = 'Benefits Investigation'
                )
            AND (e.type LIKE '%Hospital (HOPD)%')
        ) THEN
            'INF_FACILITY_TYPE_HOSPITAL_OUTPATIENT'
        WHEN
        (
            NOT (
                    pm.status_mvn__c = 'Pending'
                    AND pm.status_reason_mvn__c = 'Benefits Investigation'
                )
            AND (e.type LIKE '%Non-Infusing Location%')
        ) THEN
            'INF_FACILITY_TYPE_INFUSION_CENTER'
        WHEN
        (
            NOT (
                    pm.status_mvn__c = 'Pending'
                    AND pm.status_reason_mvn__c = 'Benefits Investigation'
                )
            AND (e.type LIKE '%Physician Office (POI)%')
        ) THEN
            'INF_FACILITY_TYPE_SPECIALIST_OFFICE'
        ELSE
    (CASE
         WHEN pm.program_name_mvn__c = 'KRYSTEXXA' THEN
             'INF_FACILITY_TYPE_SPECIALIST_OFFICE'
         WHEN pm.program_name_mvn__c = 'TEPRO' THEN
             'INF_FACILITY_TYPE_INFUSION_CENTER'
     END
    )
    END AS "Facility Type",
    pm.id AS "PM Veeva ID",
    pm.status_mvn__c AS "PM Status",
    pm.status_reason_mvn__c AS "Status Reason",
    bc.id AS "BC Veeva ID",
    bc.status_mvn__c AS "BC Status",
    bc.hzn_plan_coverage_type__c AS "Insurance Segment",
    CASE
        WHEN (
                 Isnull(bc.hzn_plan_coverage_type__c, 'Commercial') LIKE '%Commercial%'
                 AND COALESCE(bc.hzn_insurance_type__c, bc.coverage_type_mvn__c) = 'Secondary'
                 AND bc.status_mvn__c = 'Active'
                 AND pm.NAME IN (  ---Used optimized CTE instead of whole query 
                                    Select hzn_pm_id__c from BENEFIT_COVERAGE_1 where COALESCE(hzn_insurance_type__c, coverage_type_mvn__c) = 'Primary'
                                )
             )
             OR (
                    Isnull(bc.hzn_plan_coverage_type__c, 'Commercial') LIKE '%Commercial%'
                    AND COALESCE(bc.hzn_insurance_type__c, bc.coverage_type_mvn__c) = 'Primary'
                    AND bc.status_mvn__c = 'Active'
                    AND pm.NAME IN ( ---Used optimized CTE instead of whole query 
                                     Select hzn_pm_id__c from BENEFIT_COVERAGE_1 where COALESCE(hzn_insurance_type__c, coverage_type_mvn__c) = 'Secondary'
                               )
                ) THEN
            1
        ELSE
            0
    END AS "Member has at least one Govt Ins",
    (pe.firstname) AS "Member First Name",
    (pe.lastname) AS "Member Last Name",
    (pm.patient_birthdate_mvn__c) AS "Member Date of Birth",
    (cipe.address_line_1_mvn__c) AS "Member Street Address",
    (cipe.address_line_2_mvn__c) AS "Member Street Address Line 2",
    (cipe.city_mvn__c) AS "Member City",
    Replace(cipe.state_mvn__c, 'US-', '') AS "Member State",
    Cast(CASE
             WHEN Len(LEFT(utility.Removespecialcharatersandletters(Cast(cipe.zip_mvn__c AS VARCHAR)), 5)) = 5 THEN --  regexp_replace(Cast(cipe.zip_mvn__c AS VARCHAR)), '[^0-9]', '')
                 LEFT(utility.Removespecialcharatersandletters(Cast(cipe.zip_mvn__c AS VARCHAR)), 5)
             ELSE
                 NULL
         END AS VARCHAR) AS "Member Zip",
    (COALESCE(e.NAME, e.formatted_name_vod__c)) AS "Practice Name",
    CASE
        WHEN Len(utility.Removespecialcharatersandletters(COALESCE(e.hzn_npi_facility__c, e.hzn_npi_primary_hcp__c, e.hzn_npi_secondary__c, e.hzn_npi_tertiary__c, e.npi_vod__c))) = 10 THEN
            utility.Removespecialcharatersandletters(COALESCE(e.hzn_npi_facility__c, e.hzn_npi_primary_hcp__c, e.hzn_npi_secondary__c, e.hzn_npi_tertiary__c, e.npi_vod__c))
        ELSE
            NULL
    END AS "Practice NPI",
    CASE
        WHEN Len(utility.Removespecialcharatersandletters(e.hzn_tax_id__c)) = 9 THEN
            utility.Removespecialcharatersandletters(e.hzn_tax_id__c)
        ELSE
            NULL
    END AS "Practice Tax ID",
    (cie.address_line_1_mvn__c) AS "Practice Street Address",
    (cie.address_line_2_mvn__c) AS "Practice Street Address Line 2",
    (cie.city_mvn__c) AS "Practice City",
    Replace(cie.state_mvn__c, 'US-', '') AS "Practice State",
    Cast(CASE
             WHEN Len(LEFT(utility.Removespecialcharatersandletters(Cast(cie.zip_mvn__c AS VARCHAR)), 5)) = 5 THEN
                 LEFT(utility.Removespecialcharatersandletters(Cast(cie.zip_mvn__c AS VARCHAR)), 5)
             ELSE
                 NULL
         END AS VARCHAR) AS "Practice Zip",
    CASE
        WHEN pm.status_mvn__c = 'Pending'
             AND pm.status_reason_mvn__c = 'Benefits Investigation' THEN
    (COALESCE(e1.NAME, e1.formatted_name_vod__c))
        ELSE
            NULL
    END AS "ASOC Option 1 Name",
    CASE
        WHEN
        ((
                pm.status_mvn__c = 'Pending'
                AND pm.status_reason_mvn__c = 'Benefits Investigation'
            )
            AND Len(utility.Removespecialcharatersandletters(COALESCE(e1.hzn_npi_facility__c, e1.hzn_npi_primary_hcp__c, e1.hzn_npi_secondary__c, e1.hzn_npi_tertiary__c, e1.npi_vod__c))) = 10
        ) THEN
            utility.Removespecialcharatersandletters(COALESCE(e1.hzn_npi_facility__c, e1.hzn_npi_primary_hcp__c, e1.hzn_npi_secondary__c, e1.hzn_npi_tertiary__c, e1.npi_vod__c))
        ELSE
            NULL
    END "ASOC Option 1 NPI",
    CASE
        WHEN
        ((
                pm.status_mvn__c = 'Pending'
                AND pm.status_reason_mvn__c = 'Benefits Investigation'
            )
            AND Len(utility.Removespecialcharatersandletters(e1.hzn_tax_id__c)) = 9
        ) THEN
            utility.Removespecialcharatersandletters(e1.hzn_tax_id__c)
        ELSE
            NULL
    END AS "ASOC Option 1 Tax ID",
    CASE
        WHEN pm.status_mvn__c = 'Pending'
             AND pm.status_reason_mvn__c = 'Benefits Investigation' THEN
    (cie1.address_line_1_mvn__c)
        ELSE
            NULL
    END AS "ASOC Option 1 Street Address",
    CASE
        WHEN pm.status_mvn__c = 'Pending'
             AND pm.status_reason_mvn__c = 'Benefits Investigation' THEN
    (cie1.address_line_2_mvn__c)
        ELSE
            NULL
    END AS "ASOC Option 1 Street Address Line 2",
    CASE
        WHEN pm.status_mvn__c = 'Pending'
             AND pm.status_reason_mvn__c = 'Benefits Investigation' THEN
    (cie1.city_mvn__c)
        ELSE
            NULL
    END AS "ASOC Option 1 City",
    CASE
        WHEN pm.status_mvn__c = 'Pending'
             AND pm.status_reason_mvn__c = 'Benefits Investigation' THEN
            Replace(cie1.state_mvn__c, 'US-', '')
        ELSE
            NULL
    END AS "ASOC Option 1 State",
    Cast(CASE
             WHEN
             ((
                     pm.status_mvn__c = 'Pending'
                     AND pm.status_reason_mvn__c = 'Benefits Investigation'
                 )
                 AND Len(LEFT(utility.Removespecialcharatersandletters(Cast(cie1.zip_mvn__c AS VARCHAR)), 5)) = 5
             ) THEN
                 LEFT(utility.Removespecialcharatersandletters(Cast(cie1.zip_mvn__c AS VARCHAR)), 5)
             ELSE
                 NULL
         END AS VARCHAR) AS "ASOC Option 1 Zip",
    CASE
        WHEN pm.status_mvn__c = 'Pending'
             AND pm.status_reason_mvn__c = 'Benefits Investigation' THEN
    (COALESCE(e2.NAME, e2.formatted_name_vod__c))
        ELSE
            NULL
    END AS "ASOC Option 2 Name",
    CASE
        WHEN
        ((
                pm.status_mvn__c = 'Pending'
                AND pm.status_reason_mvn__c = 'Benefits Investigation'
            )
            AND Len(utility.Removespecialcharatersandletters(COALESCE(e2.hzn_npi_facility__c, e2.hzn_npi_primary_hcp__c, e2.hzn_npi_secondary__c, e2.hzn_npi_tertiary__c, e2.npi_vod__c))) = 10
        ) THEN
            utility.Removespecialcharatersandletters(COALESCE(e2.hzn_npi_facility__c, e2.hzn_npi_primary_hcp__c, e2.hzn_npi_secondary__c, e2.hzn_npi_tertiary__c, e2.npi_vod__c))
        ELSE
            NULL
    END AS "ASOC Option 2 NPI",
    CASE
        WHEN
        ((
                pm.status_mvn__c = 'Pending'
                AND pm.status_reason_mvn__c = 'Benefits Investigation'
            )
            AND Len(utility.Removespecialcharatersandletters(e2.hzn_tax_id__c)) = 9
        ) THEN
            utility.Removespecialcharatersandletters(e2.hzn_tax_id__c)
        ELSE
            NULL
    END AS "ASOC Option 2 Tax ID",
    CASE
        WHEN pm.status_mvn__c = 'Pending'
             AND pm.status_reason_mvn__c = 'Benefits Investigation' THEN
    (cie2.address_line_1_mvn__c)
        ELSE
            NULL
    END AS "ASOC Option 2 Street Address",
    CASE
        WHEN pm.status_mvn__c = 'Pending'
             AND pm.status_reason_mvn__c = 'Benefits Investigation' THEN
    (cie2.address_line_2_mvn__c)
        ELSE
            NULL
    END AS "ASOC Option 2 Street Address Line 2",
    CASE
        WHEN pm.status_mvn__c = 'Pending'
             AND pm.status_reason_mvn__c = 'Benefits Investigation' THEN
    (cie2.city_mvn__c)
        ELSE
            NULL
    END AS "ASOC Option 2 City",
    CASE
        WHEN pm.status_mvn__c = 'Pending'
             AND pm.status_reason_mvn__c = 'Benefits Investigation' THEN
            Replace(cie2.state_mvn__c, 'US-', '')
        ELSE
            NULL
    END AS "ASOC Option 2 State",
    Cast(CASE
             WHEN
             ((
                     pm.status_mvn__c = 'Pending'
                     AND pm.status_reason_mvn__c = 'Benefits Investigation'
                 )
                 AND Len(LEFT(utility.Removespecialcharatersandletters(Cast(cie2.zip_mvn__c AS VARCHAR)), 5)) = 5
             ) THEN
                 LEFT(utility.Removespecialcharatersandletters(Cast(cie2.zip_mvn__c AS VARCHAR)), 5)
             ELSE
                 NULL
         END AS VARCHAR) AS "ASOC Option 2 Zip",
    CASE
        WHEN pm.status_mvn__c = 'Pending'
             AND pm.status_reason_mvn__c = 'Benefits Investigation' THEN
    (COALESCE(e3.NAME, e3.formatted_name_vod__c))
        ELSE
            NULL
    END AS "ASOC Option 3 Name",
    CASE
        WHEN
        ((
                pm.status_mvn__c = 'Pending'
                AND pm.status_reason_mvn__c = 'Benefits Investigation'
            )
            AND Len(utility.Removespecialcharatersandletters(COALESCE(e3.hzn_npi_facility__c, e3.hzn_npi_primary_hcp__c, e3.hzn_npi_secondary__c, e3.hzn_npi_tertiary__c, e3.npi_vod__c))) = 10
        ) THEN
            utility.Removespecialcharatersandletters(COALESCE(e3.hzn_npi_facility__c, e3.hzn_npi_primary_hcp__c, e3.hzn_npi_secondary__c, e3.hzn_npi_tertiary__c, e3.npi_vod__c))
        ELSE
            NULL
    END AS "ASOC Option 3 NPI",
    CASE
        WHEN
        ((
                pm.status_mvn__c = 'Pending'
                AND pm.status_reason_mvn__c = 'Benefits Investigation'
            )
            AND Len(utility.Removespecialcharatersandletters(e3.hzn_tax_id__c)) = 9
        ) THEN
            utility.Removespecialcharatersandletters(e3.hzn_tax_id__c)
        ELSE
            NULL
    END AS "ASOC Option 3 Tax ID",
    CASE
        WHEN pm.status_mvn__c = 'Pending'
             AND pm.status_reason_mvn__c = 'Benefits Investigation' THEN
    (cie3.address_line_1_mvn__c)
        ELSE
            NULL
    END AS "ASOC Option 3 Street Address",
    CASE
        WHEN pm.status_mvn__c = 'Pending'
             AND pm.status_reason_mvn__c = 'Benefits Investigation' THEN
    (cie3.address_line_2_mvn__c)
        ELSE
            NULL
    END AS "ASOC Option 3 Street Address Line 2",
    CASE
        WHEN pm.status_mvn__c = 'Pending'
             AND pm.status_reason_mvn__c = 'Benefits Investigation' THEN
    (cie3.city_mvn__c)
        ELSE
            NULL
    END AS "ASOC Option 3 City",
    CASE
        WHEN pm.status_mvn__c = 'Pending'
             AND pm.status_reason_mvn__c = 'Benefits Investigation' THEN
            Replace(cie3.state_mvn__c, 'US-', '')
        ELSE
            NULL
    END AS "ASOC Option 3 State",
    Cast(CASE
             WHEN
             ((
                     pm.status_mvn__c = 'Pending'
                     AND pm.status_reason_mvn__c = 'Benefits Investigation'
                 )
                 AND Len(LEFT(utility.Removespecialcharatersandletters(Cast(cie3.zip_mvn__c AS VARCHAR)), 5)) = 5
             ) THEN
                 LEFT(utility.Removespecialcharatersandletters(Cast(cie3.zip_mvn__c AS VARCHAR)), 5)
             ELSE
                 NULL
         END AS VARCHAR) AS "ASOC Option 3 Zip",
    h.firstname AS "Provider First Name",
    (h.lastname) AS "Provider Last Name",
    CASE
        WHEN Len(utility.Removespecialcharatersandletters(h.npi_vod__c)) = 10 THEN
            utility.Removespecialcharatersandletters(h.npi_vod__c)
        ELSE
            NULL
    END AS "Provider NPI",
    CASE
        WHEN Len(utility.Removespecialcharatersandletters(h.hzn_tax_id__c)) = 9 THEN --- regexp_replace(h.hzn_tax_id__c, '[^0-9]', '')
            utility.Removespecialcharatersandletters(h.hzn_tax_id__c)
        ELSE
            NULL
    END AS "Provider Tax ID",
    (cih.address_line_1_mvn__c) AS "Provider Street Address",
    (cih.address_line_2_mvn__c) AS "Provider Street Address Line 2",
    (cih.city_mvn__c) AS "Provider City",
    Replace(cih.state_mvn__c, 'US-', '') AS "Provider State",
    Cast(CASE
             WHEN Len(LEFT(utility.Removespecialcharatersandletters(Cast(cih.zip_mvn__c AS VARCHAR)), 5)) = 5 THEN
                 LEFT(utility.Removespecialcharatersandletters(Cast(cih.zip_mvn__c AS VARCHAR)), 5)
             ELSE
                 NULL
         END AS VARCHAR) AS "Provider Zip",
    (CASE
         WHEN pm.program_name_mvn__c = 'TEPRO' THEN
             '75987013015'
         WHEN pm.program_name_mvn__c = 'KRYSTEXXA' THEN
             '7598708010'
         ELSE
             '72677055101'
     END
    ) AS "Product Code #1",
    pm.program_name_mvn__c AS "Product Name #1",
    96365 AS "CPT Code #1",
    96366 AS "CPT Code #2",
    96413 AS "CPT Code #3",
    96415 AS "CPT Code #4",
    CASE
        WHEN pm.program_name_mvn__c IN ( 'KRYSTEXXA' )
             AND pm.hzn_kxx_primary_diagnosis__c = 'Other' THEN
            hzn_other_primary_diagnosis__c
        WHEN pm.program_name_mvn__c IN ( 'KRYSTEXXA' )
             AND pm.hzn_kxx_primary_diagnosis__c <> 'Other' THEN
            hzn_kxx_primary_diagnosis__c
        WHEN pm.program_name_mvn__c NOT IN ( 'KRYSTEXXA' )
             AND pm.hzn_primary_diagnosis__c = 'Other' THEN
            hzn_other_primary_diagnosis__c
        ELSE
            pm.hzn_primary_diagnosis__c
    END AS "Diagnosis Code #1",
    CASE
        WHEN pm.program_name_mvn__c IN ( 'KRYSTEXXA' )
             AND pm.hzn_kxx_primary_diagnosis__c = 'Other' THEN
            hzn_secondary_dx_code__c
        WHEN pm.program_name_mvn__c IN ( 'KRYSTEXXA' )
             AND pm.hzn_kxx_primary_diagnosis__c <> 'Other' THEN
            hzn_other_primary_diagnosis__c
        WHEN pm.program_name_mvn__c NOT IN ( 'KRYSTEXXA' )
             AND pm.hzn_primary_diagnosis__c = 'Other' THEN
            hzn_secondary_dx_code__c
        ELSE
            pm.hzn_other_primary_diagnosis__c
    END AS "Diagnosis Code #2",
    CASE
        WHEN pm.program_name_mvn__c IN ( 'KRYSTEXXA' )
             AND pm.hzn_kxx_primary_diagnosis__c = 'Other' THEN
            NULL
        WHEN pm.program_name_mvn__c IN ( 'KRYSTEXXA' )
             AND pm.hzn_kxx_primary_diagnosis__c <> 'Other' THEN
            hzn_secondary_dx_code__c
        WHEN pm.program_name_mvn__c NOT IN ( 'KRYSTEXXA' )
             AND pm.hzn_primary_diagnosis__c = 'Other' THEN
            NULL
        ELSE
            pm.hzn_secondary_dx_code__c
    END AS "Diagnosis Code #3",
    CASE
        WHEN (Isnull(hpm.infinitus_payer_id, 'Payer currently not supported') LIKE '%Payer currently not supported%')
             AND (
                     COALESCE(bc.hzn_eblu_payer_name__c, bc.hzn_kry_payer_name__c) LIKE '%Blue Cross Blue Shield%'
                     OR COALESCE(bc.hzn_eblu_payer_name__c, bc.hzn_kry_payer_name__c) LIKE '%BCBS%'
                     OR COALESCE(bc.hzn_eblu_payer_name__c, bc.hzn_kry_payer_name__c) LIKE '%BlueCross BlueShield%'
                 ) THEN
            '4ac3679b-7123-450e-ae60-3960770a7f51'
        ELSE
            Isnull(hpm.infinitus_payer_id, 'Payer currently not supported')
    END AS "Infinitus Payer ID",
    COALESCE(bc.hzn_eblu_payer_name__c, bc.hzn_kry_payer_name__c) AS "Customer Payer Name",
    payer_phone_mvn__c AS "Customer Payer Phone",
    hzn_plan_member_id__c AS "Subscriber ID",
    CASE
        WHEN COALESCE(bc.hzn_insurance_type__c, bc.coverage_type_mvn__c) LIKE '%Primary%' THEN
            'INF_OTHER_INSURANCE_STATUS_EXISTS_CURRENT_PLAN_PRIMARY'
        WHEN COALESCE(bc.hzn_insurance_type__c, bc.coverage_type_mvn__c) LIKE '%Secondary%' THEN
            'INF_OTHER_INSURANCE_STATUS_EXISTS_CURRENT_PLAN_SECONDARY'
        WHEN COALESCE(bc.hzn_insurance_type__c, bc.coverage_type_mvn__c) LIKE '%Tertiary%' THEN
            'INF_OTHER_INSURANCE_STATUS_EXISTS_CURRENT_PLAN_TERTIARY'
        ELSE
            'INF_OTHER_INSURANCE_STATUS_UNKNOWN'
    END AS "Other Insurance Status",
    plan_name_mvn__c AS "Plan Name",
    'INF_INPUT_NETWORK_STATUS_UNKNOWN' AS "Provider Network Status",
    'Yes' AS "Collect Pharmacy Benefits"
FROM raw_patient_services.program_member_sf1 pm
    INNER JOIN
    ---Optimized query ......converting PIVOT into CASES
    (
SELECT 
    program_member_mvn__c,
    CASE WHEN MAX(CASE WHEN type_mvn__c = 'HIPAA Authorization' THEN status_mvn__c END) = 'Active' THEN 'Active' 
        ELSE 'Inactive' END AS hipaa_authorization_status,
    CASE WHEN MAX(CASE WHEN type_mvn__c = 'CNE/PAM Consent' THEN status_mvn__c END) = 'Active' THEN 'Active' 
        ELSE 'Inactive' END AS cne_pam_consent_status
FROM 
    ( SELECT 
            program_member_mvn__c,
            status_mvn__c,
            type_mvn__c
        FROM 
            raw_patient_services.authorization_consent_sf1
        WHERE 
            status_mvn__c = 'Active'
    ) AS SourceTable
GROUP BY 
    program_member_mvn__c
    ) ac
        ON ac.program_member_mvn__c = pm.id
           AND ac.hipaa_authorization_status = 'Active'
           AND ac.cne_pam_consent_status = 'Active'
    INNER JOIN raw_veeva.account_sf1 pe
        ON pe.recordtypeid = '01246000000NGFYAA4'
           AND pe.id = pm.member_mvn__c
    LEFT JOIN raw_veeva.account_sf1 h
        ON h.recordtypeid = '01246000000u0E6AAI'
           AND h.id = COALESCE(pm.physician_mvn__c, pm.enrolling_physician_mvn__c)
        --  reusable CTE Account instead of whole query 
    LEFT JOIN ACCOUNT e on e.id = COALESCE(pm.hzn_updated_site_of_care__c, pm.site_of_care_mvn__c)
    LEFT JOIN ACCOUNT e1
        ON  e1.id = pm.hzn_asoc_opt_1__c
    LEFT JOIN ACCOUNT e2
        ON e2.id = pm.hzn_asoc_opt_2__c
    LEFT JOIN ACCOUNT e3
        ON e3.id = pm.hzn_asoc_opt_3__c
    INNER JOIN raw_patient_services.benefits_coverage_sf1 bc
        ON pm.id = bc.program_member_mvn__c
           AND bc.status_mvn__c = 'Active'
    LEFT JOIN benefit_verification.infinitus_horizon_payer_mapping hpm
        ON hpm.payer_name = COALESCE(bc.hzn_eblu_payer_name__c, bc.hzn_kry_payer_name__c)
--reusale CTE Contact instead of whole query
    LEFT JOIN CONTACT cipe ON (pe.id = cipe.account_mvn__c)
    LEFT JOIN CONTACT cie ON (e.id = cie.account_mvn__c)
    LEFT JOIN CONTACT cie1 ON (e1.id = cie1.account_mvn__c)
    LEFT JOIN CONTACT cie2 ON (e2.id = cie2.account_mvn__c)
    LEFT JOIN CONTACT cie3 ON (e3.id = cie3.account_mvn__c)
    LEFT JOIN CONTACT cih ON (h.id = cih.account_mvn__c)
    LEFT JOIN raw_veeva.record_type_sf1 rt ON rt.id = bc.recordtypeid
    LEFT JOIN(
        SELECT "customer bv task id",
               Max("extraction_date") AS max_extraction_date,
               Sum(Cast(is_accepted AS INT)) AS count_accepted,
               CASE
                   WHEN Max("extraction_date") < (CASE WHEN Datepart(dw, (Dateadd(hour, -100, Getdate()))) IN ( 1 ) THEN Dateadd(hour, -148, Getdate())
                                                      WHEN Datepart(dw, (Dateadd(hour, -100, Getdate()))) IN ( 7 ) THEN Dateadd(hour, -124, Getdate())
                                                      ELSE Dateadd(hour, -100, Getdate())
                                                  END) 
                        THEN 1
                   ELSE      0
               END AS is_eligible_for_repull
        FROM benefit_verification.log_outbound_infinitus
        GROUP BY "customer bv task id"
    ) lg
        ON lg."customer bv task id" = Concat(bc.NAME, ' ', pm.NAME)
    LEFT JOIN
    (SELECT "customer bv task id",
               Max("extraction date") AS max_extraction_date,
               CASE WHEN Max("extraction date") < (CASE WHEN Datepart(dw, (Dateadd(hour, -100, Getdate()))) IN ( 1 ) THEN Dateadd(hour, -148, Getdate())
                                                      WHEN Datepart(dw, (Dateadd(hour, -100, Getdate()))) IN ( 7 ) THEN Dateadd(hour, -124, Getdate())
                                                      ELSE Dateadd(hour, -100, Getdate())
                                                  END) THEN 1
                   ELSE 0 END AS is_eligible_for_repull
        FROM benefit_verification.allcare_extract
        GROUP BY "customer bv task id"
    ) lga
        ON lga."customer bv task id" = Concat(bc.NAME, ' ', pm.NAME)
WHERE 
pm.program_name_mvn__c IN ( 'TEPRO', 'KRYSTEXXA' ) 
      AND (
              (lg.[customer bv task id] IS NULL)
              OR (
                     lg.is_eligible_for_repull = 1
                     AND lg.count_accepted > 0
                 )
          )
      AND (
              (lga.[customer bv task id] IS NULL)
              OR (lga.is_eligible_for_repull = 1)
          )
      AND (
              pm.status_mvn__c = 'Pending'
              AND pm.status_reason_mvn__c = 'Benefits Investigation'
          )
      AND NOT (
                  COALESCE(cipe.r, 0) > 1
                  OR COALESCE(cie.r, 0) > 1
                  OR COALESCE(cie1.r, 0) > 1
                  OR COALESCE(cie2.r, 0) > 1
                  OR COALESCE(cie3.r, 0) > 1
                  OR COALESCE(cih.r, 0) > 1
              )
AND (( Isnull(bc.hzn_plan_coverage_type__c, 'Commercial') LIKE '%Commercial%'
                  AND (CASE WHEN (
                                    Isnull(bc.hzn_plan_coverage_type__c, 'Commercial') LIKE '%Commercial%'
                                    AND COALESCE(bc.hzn_insurance_type__c, bc.coverage_type_mvn__c) = 'Secondary'
                                    AND bc.status_mvn__c = 'Active'
                                    --Reusbale CTE BENEFIT_COVERAGE_1
                                    AND pm.NAME IN (Select hzn_pm_id__c from BENEFIT_COVERAGE_1 where COALESCE(hzn_insurance_type__c, coverage_type_mvn__c) = 'Primary')
                                )
                                OR (
                                       Isnull(bc.hzn_plan_coverage_type__c, 'Commercial') LIKE '%Commercial%'
                                       AND COALESCE(bc.hzn_insurance_type__c, bc.coverage_type_mvn__c) = 'Primary'
                                       AND bc.status_mvn__c = 'Active'
                                       --Reusbale CTE BENEFIT_COVERAGE_1
                                       AND pm.NAME IN (Select hzn_pm_id__c from BENEFIT_COVERAGE_1 where COALESCE(hzn_insurance_type__c, coverage_type_mvn__c) = 'Secondary')
                                   ) THEN 1
                           ELSE 0
                       END = 0
                      ))
              OR (
                     bc.hzn_plan_coverage_type__c IN ( 'Medicare Advantage', 'Managed Medicaid', 'Medicare FFS',  'Commercial', 'Federal Employee', 'Supplemental')
                     AND CASE
                             WHEN 
							 (
                                      bc.hzn_plan_coverage_type__c IN ( 'Medicare Advantage' )
                                      AND pm.NAME IN (  ----Reusbale CTE BENEFIT_COVERAGE_2
                                                         SELECT hzn_pm_id__c 
                                                         FROM BENEFIT_COVERAGE_2 where hzn_plan_coverage_type__c not in ('Medicare Advantage', 'Medicare FFS')
                                                     )
                                  )
                                  OR 
								  (
                                         bc.hzn_plan_coverage_type__c IN ( 'Managed Medicaid' )
                                         AND pm.NAME IN ( --Reusbale CTE BENEFIT_COVERAGE_2
                                                            SELECT hzn_pm_id__c 
                                                         FROM BENEFIT_COVERAGE_2 where hzn_plan_coverage_type__c not in ('Managed Medicaid', 'Medicare FFS')
                                                        )
                                     ) 
									   OR (
                                         bc.hzn_plan_coverage_type__c IN ( 'Medicare FFS' )
                                         AND pm.NAME IN ( --Reusbale CTE BENEFIT_COVERAGE_2
                                                            SELECT hzn_pm_id__c FROM BENEFIT_COVERAGE_2 bc
															LEFT JOIN benefit_verification.infinitus_horizon_payer_mapping hpm
															ON hpm.payer_name = COALESCE(bc.hzn_eblu_payer_name__c, bc.hzn_kry_payer_name__c)
                                                                  AND 
																  (
																	  hzn_plan_coverage_type__c not in ('Medicare FFS', 'Medicare Advantage',  'Commercial', 'Federal Employee', 'Managed Medicaid', 'Supplemental')
																	  OR 
																	  (
																	  hzn_plan_coverage_type__c in ('Medicare FFS', 'Medicare Advantage',  'Commercial', 'Federal Employee', 'Managed Medicaid', 'Supplemental')
																	  AND
																		(
																		Isnull(hpm.infinitus_payer_id, 'Payer currently not supported') = 'Payer currently not supported'
																		  AND COALESCE(bc.hzn_eblu_payer_name__c, bc.hzn_kry_payer_name__c) NOT LIKE '%Blue Cross Blue Shield%'
																		  AND COALESCE(bc.hzn_eblu_payer_name__c, bc.hzn_kry_payer_name__c) NOT LIKE '%BCBS%'
																		  AND COALESCE(bc.hzn_eblu_payer_name__c, bc.hzn_kry_payer_name__c) NOT LIKE '%BlueCross BlueShield%'
																		 ) ))))
									 OR
									 (
                        bc.hzn_plan_coverage_type__c IN ( 'Commercial', 'Federal Employee', 'Supplemental')
                        AND pm.NAME IN (
                            ---Reusbale CTE BENEFIT_COVERAGE_2
                                        SELECT hzn_pm_id__c FROM BENEFIT_COVERAGE_2 bc
										LEFT JOIN benefit_verification.infinitus_horizon_payer_mapping hpm
										ON hpm.payer_name = COALESCE(bc.hzn_eblu_payer_name__c, bc.hzn_kry_payer_name__c)
                                                AND 
												((
													Isnull(hpm.infinitus_payer_id, 'Payer currently not supported') = 'Payer currently not supported'
														AND COALESCE(bc.hzn_eblu_payer_name__c, bc.hzn_kry_payer_name__c) NOT LIKE '%Blue Cross Blue Shield%'
														AND COALESCE(bc.hzn_eblu_payer_name__c, bc.hzn_kry_payer_name__c) NOT LIKE '%BCBS%'
														AND COALESCE(bc.hzn_eblu_payer_name__c, bc.hzn_kry_payer_name__c) NOT LIKE '%BlueCross BlueShield%'
														)	)     ) )
                                            THEN 1
                             ELSE 0
                         END = 0
                 ) )
 AND (CASE
               WHEN (Isnull(hpm.infinitus_payer_id, 'Payer currently not supported') LIKE '%Payer currently not supported%')
                    AND (
                            COALESCE(bc.hzn_eblu_payer_name__c, bc.hzn_kry_payer_name__c) LIKE '%Blue Cross Blue Shield%'
                            OR COALESCE(bc.hzn_eblu_payer_name__c, bc.hzn_kry_payer_name__c) LIKE '%BCBS%'
                            OR COALESCE(bc.hzn_eblu_payer_name__c, bc.hzn_kry_payer_name__c) LIKE '%BlueCross BlueShield%'
                        ) THEN
                   '4ac3679b-7123-450e-ae60-3960770a7f51'
               ELSE
                   Isnull(hpm.infinitus_payer_id, 'Payer currently not supported')
           END
          ) NOT LIKE '%Payer currently not supported%'
      AND NOT (
                  COALESCE(bc.hzn_eblu_payer_name__c, bc.hzn_kry_payer_name__c) IS NULL
                  OR bc.hzn_plan_member_id__c IS NULL
              )
      AND NOT (
                  h.firstname = 'Raymond'
                  AND h.lastname = 'Douglas'
              )
      AND (cipe.state_mvn__c <> 'CA')    