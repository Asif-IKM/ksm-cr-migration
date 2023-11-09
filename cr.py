from datetime import datetime, date
import time

import psycopg2
import json
import os

current_datetime = datetime.now()


# Define a custom JSON encoder to handle datetime objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)


conn = psycopg2.connect(
    host="localhost",
    database="IKM-Dump",
    user="postgres",
    password="1234"
)

batch_size = 1000

cursor = conn.cursor()
query_dict = {
    "localbody_query": """
        SELECT mgrn_localbody_id, office_name_en 
        FROM master.m_localbody 
        WHERE fk_lbtype_id in (3, 4) and status = 1
    """
}
sql_query = query_dict["localbody_query"]

# Execute the SQL query
cursor.execute(sql_query)

# Fetch all rows from the result set
rows = cursor.fetchall()

# Create a dictionary from the query results
result_dict = {str(row[0]): row[1].strip() for row in rows}

# Print the resulting dictionary
print(result_dict)
tot_count = 0
for master_lbid in result_dict:
    folder_name = "C:/Users/mohammedasif/Downloads/ksm-cr-migration/DATA" + result_dict[master_lbid]
    # print("lbid=",master_lbid+"="+result_dict[master_lbid])

    # Check if the folder doesn't already exist
    if not os.path.exists(folder_name):
        # Create the folder
        os.mkdir(folder_name)
        # print(f"Folder '{folder_name}' created successfully.")
    # else:
    # print(f"Folder '{folder_name}' already exists.")

    query1 = f"""
    SELECT
        bitmultybirth AS multiple,
        inymultynochild AS childCount,
        gen_random_uuid() AS id,
        COALESCE(chvregnno) AS registrationNumber,
        brth.chvackno AS certificateNumber,
        -- added on 03102022
        NULL AS applicationNumber,
        dtmregndate::date AS registrationDate,
        CASE WHEN inycancelled = 1 THEN 'STATUS_CANCELLED' ELSE 'STATUS_ACTIVE' END AS registration_status,
        inw.inwdate::date AS dateOfReporting,
        -- added on 03102022
        'B' AS registrationType,
        -- Child details
        CASE WHEN bitdeserted = 0 THEN dtmbirthdate1::date ELSE dtmbirthdate2::date END AS dateofbirth,
        CASE WHEN bitdeserted = 0 THEN dtmbirthdate1::time ELSE dtmbirthdate2::time END AS timeofbirth,
        CASE WHEN bitdeserted = 0 THEN TO_CHAR(dtmbirthdate1, 'AM') ELSE TO_CHAR(dtmbirthdate2, 'AM') END AS am_pm,
        chvengchild AS firstName,
        chvmalchild AS firstNameInLcl,
        inygender AS genderId,
        mg.gender_en AS genderEng,
        mg.gender_ml AS genderMal,
        NULL AS lastName,
        NULL AS lastNameInLcl,
        NULL AS middleName,
        NULL AS middleNameInLcl,
        brth.intbirthplaceid AS placeOfBirthId,
        mp.mplace_name_en AS placeOfBirthNameEng,
        mp.mplace_name_ml AS placeOfBirthNameMal,
        NULL AS childNameNotProvided,
        NULL AS aadharNo,
--         lb.distid AS districtIdD,
        d.office_code as districtIdD,
        lb.engdistname AS distEngName,
        lb.distname AS distMalName,
--         lb.lb_id AS lbId,
        lb.office_code As lbId,
        lb.englbname AS lbName,
        lb.lbname AS lbNameInLc,
        lb.englbtype AS lbtypeEng,
        lb.lbtype AS lbTypeMal,
        -- BIRTH PLACE HOME completed 12102023
        CONCAT_WS(', ', brth.chvresassno, brth.chvbphouseno, brth.chvbpenghouse) AS birthPlaceHome_enghouse,
        CONCAT_WS(', ', brth.chvresassno, brth.chvbphouseno, brth.chvbpmalhouse) AS birthPlaceHome_malayalam_name,
        NULL AS localityName,
        NULL AS localityNameLcl,
        NULL AS pincode,
--         intbppoid_port AS postOfficeId,
        p."Office_Code" AS postOfficeId,
        p."Office_Code" AS postOfficeCode,
        P.office_name_en AS postOfficeName,
        P.office_name_ml AS postOfficeNameInLc,
        CONCAT_WS(', ', chvbpengstreet, chvbpengplace, chvbpengvia) AS streetname,
        CONCAT_WS(', ', chvbpmalstreet, chvbpmalplace, chvbpengvia) AS streetname_inlocal,
        v."Office_Code" AS village_id,
        v."Village Name Eng" AS villageName,
        v."Village Name Mal" AS villageNameInLcl,
        -- ward details
        NULL AS wardid,
        NULL AS wardName,
        NULL AS wardNameInLcl,
        -- BIRTH PLACE HOSPITAL completed 13102023
        mh.Office_Code AS hospitalid,
        mh.chvenghospital AS hospital_name,
        mh.chvmalhospital AS hospital_name_in_local,
        mh.hospital_type AS hospitalTypeId,
        mhm.hospmgmnt_name_en AS hospitaltypeName,
        mhm.hospmgmnt_name_ml AS hospitalTypeNameInLc,
        -- BIRTH PLACE INSTITUTION
--         brth.intinstid AS birthplace_institution_id,
        ik.office_code AS birthplace_institution_id,
        ik.office_name AS birthplaceInstitutionName,
        ik.office_name_in_local AS birthplaceInstitutionNameInLc,
        brth.intinsttypeid AS birthplace_institution_type_id,
        itk.institutiontype AS institutionTypeName,
        itk.institutiontypelocal AS institutiontypeNameInLc,

        -- BIRTH PLACE PUBLIC
        NULL AS localityName,
        NULL AS localityNameLCL,
        NULL AS pinCode,
        NULL AS postOfficeid,
        NULL AS name,
        NULL AS nameLclL,
        brth.intotherid AS publicPlaceid, --line number 72
        brth.chvotherdetails AS publicName,
        repadd.chvmalbpothers AS PublicNameLcl,
        NULL AS streetName,
        NULL AS streetNameNameLcl,
        NULL AS villageids,
        NULL AS villageName,
        NULL AS villageNameLcl,
        brth.chvresassno AS wardNoName,
        NULL AS wardName,
        NULL AS WardnameLcl,
        --BIRTH PLACE VEHICLE completed as on 14102023
        NULL as lBWardPlaceofHalt,
        NULL as placeOfFirstHaltLB,
        NULL as vehicleRegistrationNo,
        NULL as vehicleTravellingFrom,
        NULL as vehicleTravellingFromInLcl,
        NULL as vehicleTravellingTo,
        NULL as vehicleTravellingInLcl,
        NULL as vehicleTypeId,--    brth.intotherid AS vehicle_type,
        NULL as vechilename,
        NULL as vechilenameInLcl,
        NULL as vechilevillageid,
        NULL as vechileVillagename,
        NULL as vechileVillagenameInLcl,
        -- ADDITIONAL BIRTH DETAILS

        NULL AS causeOfFotelDeath, -- Still Birth type clarify
        brth.intdelid AS deliverymethod,
        -- case when Intdelid = 1 then 'DELIVERY_NATURAL' when Intdelid = 2 then 'DELIVERY_CAESAREAN' when Intdelid = 3 then 'DELIVERY_FORCEPS_VACCUM' when Intdelid = 4 then 'DELIVERY_FORCEPS_VACCUM' end delivery_method,
        brth.intpregduration AS pregnancyDuration,
        brth.Intattnpreg AS natureOfMedicalAttention, -- chvattenpregothers
        brth.fltbabyweight AS babyWeight,

        -- BORN OUTSIDE BIRTH DETAILS

        NULL AS countryOfBirthid,
        NULL AS CountryBirthName,
        NULL AS CountryBirthNameInLcl,
        NULL AS placeOfBirthInEn,
        NULL AS placeOfBirthInLcl,
        NULL AS stateOrProvinceOrRegionInEn,
        NULL AS stateOrProvinceOrRegionInLcl,
        NULL AS nationalityid,
        NULL AS nationalityName,
        NULL AS nationalityNameInLcl,
        NULL AS passportNo,
        NULL AS dateOfArrivalInIndia,

        -- FATHER DETAILS

        NULL AS fatherAadhaarNo,
        brth.intfeduid AS fatherId,
        -- Chvfatheredu Join Masters
        NULL AS fathername,
        NULL AS fathernameInLcl,
        NULL AS fatherInformationMissing,
        brth.chvengfather AS fatherName,
        brth.chvmalfather AS fatherNameInLocal,
        NULL AS fatherNationalityid,
        NULL AS fatherNationalName,
        NULL AS fatherNationalNameInLcl,
        brth.intfoccupid AS fatherOccupationId, -- Chvfatheroccup Join Masters
        NULL AS fatherOccupationame,
        NULL AS fatherOccupationnameInLcl,

        -- MOTHER DETAILS

        -- case when inymotherplace = 1 then 'INSIDE LB' when inymotherplace = 2 then 'INSIDE_KERALA' when inymotherplace = 3 then 'INSIDE_INDIA' when inymotherplace = 4 then 'OUTSIDE_INDIA' end mother_resdnce_addr_type

        brth.intmothagebirth AS ageAtTheTimeOfBirth,
        brth.intmothmarrage AS ageAtTheTimeOfMarriage,
        NULL AS motherAadhaarNo,
        brth.intmeduid AS motherEducationid, -- Chvmotheredu
        NULL AS eduname,
        NULL AS edunameInLcl,
        CASE
            WHEN bitmothermarried = 1 THEN 'married'
            WHEN bitmothermarried = 0 THEN 'not married'
            ELSE '0'
        END AS motherMaritalStatus,
        CASE
            WHEN bitmothermarried = 1 THEN 'വിവാഹിത'
            WHEN bitmothermarried = 0 THEN 'അവിവാഹിത'
            ELSE '0'
        END AS motherMaritalStatusLcl,
        brth.chvengmother AS motherName,
        brth.chvmalmother AS motherNameInLcl,
        NULL AS motherNationalityid,
        NULL AS motherNationalityname,
        NULL AS motherNationalitynameInLcl,
        brth.intmoccupid AS motherOccupationId, -- Chvmotheroccup
        NULL AS motherOccupationname,
        NULL AS motherOccupationnameInLcl,
        intlivechildren AS noOfChildrenBornAliveIncludingThisChild,
        NULL AS parentEmailId,
        NULL AS parentsContactNo,
        NULL AS religionid,
        NULL AS religionname,
        NULL AS religionnameInLcl,

      -- PERMANENT ADDRESS INSIDE KERALA

--         repadd.intperdistrict AS districtId,
        d.office_code AS districtId,
        d.district_name_en AS permanaentAddInklName,
        d.district_name_mal AS permanaentAddInklNameInLcl,
        repadd.chvperaddresseng1 AS perAddInKlhouseName,
        repadd.chvperaddressmal1 AS perAddInKlhouseNameInLcl,
        -- repadd.intperlbid AS lbid,
        NULL AS lbid,
        NULL AS lbidName,
        NULL AS lbidNameInLcl,
        NULL AS lbTypeid,
        NULL AS lbTypeIdName,
        NULL AS lbTypeIdNameInLcl,
        repadd.chvperaddresseng2 AS AddresslocalityName,
        repadd.chvperaddressmal2 AS AddresslocalityNameInlocal,
        repadd.chvperpin AS AddresslocalityPincode,
--         repadd.intperpoid_port AS PostofficeId,
        po."Office_Code" AS PostofficeId,
        po.office_name_en AS PostofficeIdName,
        po.office_name_ml AS PostofficeIdNameInLcl,
        NULL AS perAddInKlstreetName,
        NULL AS perAddInKlstreetNameInLcl,
        NULL AS perAddKltalukid,
        NULL AS perAddKltalukname,
        NULL AS perAddKltaluknameInLcl,
        -- brth.intvillageid AS villageId,
        NULL AS villageId,
        NULL AS villageAddInKlname,
        NULL AS villageAddInKlnameInLcl,
        NULL AS perAddKlwardid,
        NULL AS perAddKlWardname,
        NULL AS perAddKlWardnameInLcl,

        -- PERMANENT ADDRESS OUTSIDE INDIA
        NULL AS addressLine1,
        NULL AS addressLine1InLcl,
        NULL AS addressLine2,
        NULL AS addressLine2InLcl,
        NULL AS cityOrTownOrVillageName,
        NULL AS postalCode,
        NULL AS stateProvinceRegion,
        NULL AS townOrVillage,

        -- PRESENT ADDRESS INSIDE KERALA

--         repadd.intcurrdistrict AS presentAddressInsideKeralaDistrictid,
        cd.office_code AS presentAddressInsideKeralaDistrictid,
        cd.district_name_en AS preAddInKlname,
        cd.district_name_mal AS preAddInKlnameInLcl,
        repadd.chvcurraddresseng1 AS presentAddressInsideKeralahouseName,
        repadd.chvcurraddressmal1 AS presentAddressInsideKeralahouseNameInlocal,
--         repadd.intcurrlbid AS presentAddressInsideKeralalbid,
        lbnew.office_code AS presentAddressInsideKeralalbid,
        NULL AS presentAddressInsideKeralalbid,
        NULL AS pstAddInKlname,
        NULL AS pstAddInKlnameInLcl,
        NULL AS pstAddInKlnamelbTypeid,
        NULL AS pstAddInKlname,
        NULL AS pstAddInKlnamenameInLcl,

        repadd.chvcurraddresseng2 AS pstAddInKllocalityname,
        repadd.chvcurraddressmal2 AS pstAddInKllocalitynameInlocal,
        repadd.chvcurrpin AS presentAddressInsideKeralaPincode,
--         repadd.intcurrpoid_port AS presentAddressInsideKeralaPostofficeId,
        cpo."Office_Code" AS presentAddressInsideKeralaPostofficeId,
        cpo.office_name_en AS POName,
        cpo.office_name_ml AS PONameInLcl,
        NULL AS pstAddinKlstreetname,
        NULL AS pstAddinKlstreetnameInLcl,
        NULL AS pstAddinKltalukid,
        NULL AS pstAddinKlname,
        NULL AS pstAddinKlnameInLcl,
        NULL AS pstAddinKlvillageid,
        NULL AS pstAddinKlname,
        NULL AS pstAddinKlnameInLcl,
        NULL AS pstAddinKlwardid,
        NULL AS pstAddinKlname,
        NULL AS pstAddinKlnameInLcl,

        -- PRESENT ADDRESS OUTSIDE INDIA
        NULL AS pstAddOutIndiaaddressLine1,
        NULL AS pstAddOutIndiaaddressLine1InLcl,
        NULL AS pstAddOutIndiaaddressLine2,
        NULL AS pstAddOutIndiaaddressLine2InLcl,
        NULL AS pstAddOutIndiacityOrTownOrVillageName,
        NULL AS pstAddOutIndiapostalCode,
        NULL AS pstAddOutIndiastateProvinceRegion,
        NULL AS pstAddOutIndiatownOrVillage,

        -- PRESENT ADDRESS OUTSIDE KERALA
        NULL AS pstAddOutKldistrictid,
        NULL AS pstAddOutKlname,
        NULL AS pstAddOutKlnameInLcl,
        NULL AS pstAddOutKlhouseNoName,
        NULL AS pstAddOutKlhouseNoNameInLcl,
        NULL AS pstAddOutKllocalityName,
        NULL AS pstAddOutKllocalityNameInLcl,
        NULL AS pstAddOutKlpinCode,
        NULL AS pstAddOutKlpostOfficeName,
        NULL AS pstAddOutKlstreetName,
        NULL AS pstAddOutKlstreetNameInLcl,
        NULL AS pstAddOutKltehsilName,
        NULL AS pstAddOutKltownOrVillage,
        NULL AS pstAddOutKlvillageName,

--         repadd.intpercountry AS permanentCountryId,
        c.Office_code AS permanentCountryId,
        c.country_name_en AS presentCnname,
        c.country_name_ml AS presentCnnameInLcl,
        NULL AS permanentSameAsPresent,
--         repadd.intperstate AS permanentStateId,
        s.office_code AS permanentStateId,
        s.state_name_en AS presentStatename,
        s.state_name_ml AS presentStatenameInLcl,

        NULL AS initiatorDetails,
        NULL AS initiatoraadhaarNo,
        NULL AS initiatoraddress,
        NULL AS initiatorcontactNo,
        NULL AS initiatoremailId,

        NULL AS initiatorid,
        NULL AS initiatorname,
        NULL AS initiatornameInLcl,
        NULL AS initiatorInstitutiondesignationid,
        NULL AS initiatorInstitutionname,
        NULL AS initiatorInstitutionnameInLcl,
        NULL AS institutionid,
        NULL AS institutionname,
        NULL AS institutionnameInLcl,
        NULL AS name,
        NULL AS relationshipToTheChild
    FROM
        crtemp.tbirthrep_unicode brth
    LEFT JOIN
        crtemp.tinward inw ON brth.chvackno = inw.chvackno
    LEFT JOIN
        crtemp.tbirtrep_address_unicode repadd ON brth.chvackno = repadd.chvackno
    LEFT JOIN
        master.hospital_KSMART mh ON brth.lbid = mh.LBid AND mh.mgrn_hospital_id = brth.inthospitalid
    LEFT JOIN
        master.m_hospital_management mhm ON mh.hospital_type = mhm.pk_hospmgmnt_id
    LEFT JOIN
        master.localbody_ksmart lb ON brth.lbid = lb.lb_id AND lb.status = 1
    LEFT JOIN
        master.institutionTypes_ksmart itk ON brth.intinsttypeid = itk.intid
    LEFT JOIN
        master.institutions_ksmart ik ON brth.intinstid = ik.id AND brth.lbid = ik.lb_id
    LEFT JOIN
        master.m_minor_education emnr ON brth.intfeduid = emnr.migration_id::int AND emnr.status = '1'
    LEFT JOIN
        master.m_major_education emjr ON emnr.fk_mjr_education_id = emjr.pk_mjr_education_id AND emjr.status = '1'
    LEFT JOIN
        master.m_minor_education m_emnr ON brth.intMEduID = m_emnr.migration_id::int AND m_emnr.status = '1'
    LEFT JOIN
        master.m_major_education m_emjr ON m_emnr.fk_mjr_education_id = m_emjr.pk_mjr_education_id AND m_emjr.status = '1'
    LEFT JOIN
        master.m_sub_occupation sub ON brth.intfoccupid = sub.migration_id::int AND sub.status = '1'
    LEFT JOIN
        master.m_sub_occupation m_sub ON brth.intmoccupid = m_sub.migration_id::int AND m_sub.status = '1'
    LEFT JOIN
        master.m_district_ksmart d ON repadd.intperdistrict = d.mgrn_district_id AND repadd.intperstate = d.state_id AND d.status = '1'
    LEFT JOIN
        master.m_district_ksmart cd ON repadd.intcurrdistrict = cd.mgrn_district_id AND cd.status = '1' AND cd.mgrn_district_id > 0 and repadd.intcurrstate = cd.state_id
    LEFT JOIN
        master."postOffice_KSMART" po ON repadd.intperpoid_port = po.mgrn_cr_postoffice_id AND po.status = '1'
    LEFT JOIN
        master."postOffice_KSMART" cpo ON repadd.intcurrpoid_port = cpo.mgrn_cr_postoffice_id AND cpo.status = '1'
    LEFT JOIN
        master."postOffice_KSMART" p ON brth.intbppoid_port = p.mgrn_cr_postoffice_id AND p.status = '1'
    LEFT JOIN
        master.m_state_ksmart s ON repadd.intperstate = s.state_id::int AND s.status = '1'
    LEFT JOIN
        master.m_state_ksmart cs ON repadd.intcurrstate = cs.state_id::int AND cs.status = '1'
    LEFT JOIN
        master.m_country_ksmart c ON repadd.intpercountry = c.country_id::int AND c.status = '1'
    LEFT JOIN
        master.m_country_ksmart cc ON repadd.intcurrcountry = cc.country_id::int AND cc.status = '1'
    left JOIN
        master."village_KSMART" v ON brth.intBPVID = v.mgrn_village_id::int AND v.status = '1' AND v.mgrn_village_id::int <> 0
    LEFT JOIN
        master.m_religion r ON brth.intfamilyreligionid = r.migration_id::int AND r.status = '1'
    LEFT JOIN
        master.m_gender mg ON brth.inygender = mg.migration_id AND r.status = '1'
    LEFT JOIN
        master.mplace mp ON brth.intbirthplaceid = mp.pk_place_id
    LEFT JOIN
        master.localbody_ksmart lbnew ON repadd.intcurrlbid = lbnew.lb_id

    WHERE
        chvregnno IS NOT NULL
         AND mh.LBid = {master_lbid} LIMIT 10;

    """

    cursor.execute(query1)
    # AND mh.LBid = {master_lbid} limit 5;

    batch = []
    mm = master_lbid
    print("Migration starts...")


    def date_handler(obj):
        if isinstance(obj, date):
            return obj.strftime("%Y-%m-%d")
        raise TypeError("Object of type date is not JSON serializable")


    # with open(folder_name+"/" + result_dict[master_lbid] + ".json", "w", encoding="utf-8") as json_file:
    temp_count = 0
    for row in cursor:
        (multiple, childCount, id, registrationNumber, certificateNumber, applicationNumber, registrationDate,
         registration_status,
         dateOfReporting, registrationType, dateofbirth, timeofbirth, am_pm, firstName, firstNameInLcl, genderId,
         genderEng,
         genderMal, lastName, lastNameInLcl, middleName, middleNameInLcl, placeOfBirthId, placeOfBirthNameEng,
         placeOfBirthNameMal,
         childNameNotProvided, aadharNo, districtIdD, distEngName, distMalName, lbId, lbName, lbNameInLc,
         lbtypeEng, lbTypeMal, birthPlaceHome_enghouse, birthPlaceHome_malayalam_name, localityName,
         localityNameLcl,
         pincode,
         postOfficeId, postOfficeCode, postOfficeName, postOfficeNameInLc, streetname, streetname_inlocal,
         village_id,
         villageName, villageNameInLcl, wardid, wardName, wardNameInLcl, hospitalid, hospital_name,
         hospital_name_in_local,
         hospitalTypeId, hospitaltypeName, hospitalTypeNameInLc, birthplace_institution_id,
         birthplaceInstitutionName,
         birthplaceInstitutionNameInLc, birthplace_institution_type_id, institutionTypeName,
         institutiontypeNameInLc,
         localityName,
         localityNameLCL, pinCode, postOfficeid, name, nameLclL, publicPlaceid, publicName, PublicNameLcl,
         streetName,
         streetNameNameLcl, villageids, villageName,
         villageNameLcl, wardNoName, wardName, WardnameLcl, lBWardPlaceofHalt, placeOfFirstHaltLB,
         vehicleRegistrationNo,
         vehicleTravellingFrom,
         vehicleTravellingFromInLcl, vehicleTravellingTo, vehicleTravellingInLcl, vehicleTypeId,
         vechilename, vechilenameInLcl, vechilevillageid, vechileVillagename, vechileVillagenameInLcl,
         causeOfFotelDeath, deliverymethod, pregnancyDuration,
         natureOfMedicalAttention, babyWeight, countryOfBirthid,
         CountryBirthName, CountryBirthNameInLcl, placeOfBirthInEn, placeOfBirthInLcl, stateOrProvinceOrRegionInEn,
         stateOrProvinceOrRegionInLcl, nationalityid, nationalityName, nationalityNameInLcl, passportNo,
         dateOfArrivalInIndia,
         fatherAadhaarNo, fatherId, fathername, fathernameInLcl, fatherInformationMissing, fatherName,
         fatherNameInLocal,
         fatherNationalityid, fatherNationalName, fatherNationalNameInLcl, fatherOccupationId, fatherOccupationame,
         fatherOccupationnameInLcl,
         ageAtTheTimeOfBirth, ageAtTheTimeOfMarriage, motherAadhaarNo, motherEducationid, eduname, edunameInLcl,
         motherMaritalStatus, motherMaritalStatusLcl, motherName, motherNameInLcl, motherNationalityid,
         motherNationalityname, motherNationalitynameInLcl, motherOccupationId, motherOccupationname,
         motherOccupationnameInLcl, noOfChildrenBornAliveIncludingThisChild, parentEmailId, parentsContactNo,
         religionid,
         religionname, religionnameInLcl, districtId,
         permanaentAddInklName, permanaentAddInklNameInLcl, perAddInKlhouseName, perAddInKlhouseNameInLcl, lbid,
         lbidName,
         lbidNameInLcl, lbTypeid, lbTypeIdName, lbTypeIdNameInLcl, AddresslocalityName, AddresslocalityNameInlocal,
         AddresslocalityPincode, PostofficeId, PostofficeIdName,
         PostofficeIdNameInLcl, perAddInKlstreetName, perAddInKlstreetNameInLcl, perAddKltalukid, perAddKltalukname,
         perAddKltaluknameInLcl, villageId, villageAddInKlname, villageAddInKlnameInLcl, perAddKlwardid,
         perAddKlWardname,
         perAddKlWardnameInLcl, addressLine1, addressLine1InLcl,
         addressLine2, addressLine2InLcl, cityOrTownOrVillageName, postalCode, stateProvinceRegion, townOrVillage,
         presentAddressInsideKeralaDistrictid, preAddInKlname, preAddInKlnameInLcl,
         presentAddressInsideKeralahouseName,
         presentAddressInsideKeralahouseNameInlocal, presentAddressInsideKeralalbid, presentAddressInsideKeralalbid,
         pstAddInKlname, pstAddInKlnameInLcl, pstAddInKlnamelbTypeid, pstAddInKlname, pstAddInKlnamenameInLcl,
         pstAddInKllocalityname, pstAddInKllocalitynameInlocal, presentAddressInsideKeralaPincode,
         presentAddressInsideKeralaPostofficeId, POName, PONameInLcl, pstAddinKlstreetname,
         pstAddinKlstreetnameInLcl,
         pstAddinKltalukid, pstAddinKlname, pstAddinKlnameInLcl, pstAddinKlvillageid, pstAddinKlname,
         pstAddinKlnameInLcl,
         pstAddinKlwardid, pstAddinKlname, pstAddinKlnameInLcl, pstAddOutIndiaaddressLine1,
         pstAddOutIndiaaddressLine1InLcl, pstAddOutIndiaaddressLine2, pstAddOutIndiaaddressLine2InLcl,
         pstAddOutIndiacityOrTownOrVillageName, pstAddOutIndiapostalCode, pstAddOutIndiastateProvinceRegion,
         pstAddOutIndiatownOrVillage, pstAddOutKldistrictid, pstAddOutKlname, pstAddOutKlnameInLcl,
         pstAddOutKlhouseNoName, pstAddOutKlhouseNoNameInLcl, pstAddOutKllocalityName, pstAddOutKllocalityNameInLcl,
         pstAddOutKlpinCode, pstAddOutKlpostOfficeName, pstAddOutKlstreetName, pstAddOutKlstreetNameInLcl,
         pstAddOutKltehsilName,
         pstAddOutKltownOrVillage, pstAddOutKlvillageName, permanentCountryId, presentCnname, presentCnnameInLcl,
         permanentSameAsPresent, permanentStateId, presentStatename, presentStatenameInLcl, initiatorDetails,
         initiatoraadhaarNo, initiatoraddress, initiatorcontactNo, initiatoremailId, initiatorid,
         initiatorname, initiatornameInLcl, initiatorInstitutiondesignationid, initiatorInstitutionname,
         initiatorInstitutionnameInLcl, institutionid,
         institutionname, institutionnameInLcl, name, relationshipToTheChild) = row

        # Convert registrationDate and dateofbirth to strings
        registrationDate_str = registrationDate.isoformat() if registrationDate else None
        dateofbirth_str = dateofbirth.isoformat() if dateofbirth else None
        result = {
            "tenantId": "kl",
            "birthRegistry": []
        }
        birth_registry = {
            "source": "Sevana",
            "multiple": multiple,
            "childCount": childCount,
            "id": id,
            "registrationNumber": registrationNumber,
            "certificateNumber": certificateNumber,
            "applicationNumber": applicationNumber,
            "registrationDate": registrationDate,
            "active": True,  # Todo: Check the Added data.
            "cancelled": registration_status,
            "dateOfReporting": dateOfReporting,
            "registrationType": registrationType,  # Todo: Check the Added data.

            # CHILD DETAILS
            "childDetails": {
                "dateOfBirth": dateofbirth,  # Todo: Change the field name.
                "timeOfBirth": str(timeofbirth),
                "timeZone": am_pm,  # Todo: Attribute added by me.
                "firstName": firstName,
                "firstNameInLcl": firstNameInLcl,
                "gender": {
                    "id": genderId,
                    "name": genderEng,
                    "nameInLcl": genderMal
                },
                "lastName": lastName,
                "lastNameInLcl": lastNameInLcl,
                "middleName": middleName,
                "middleNameInLcl": middleNameInLcl,

                "placeOfBirthId": {
                    "id": str(placeOfBirthId),
                    "name": placeOfBirthNameEng,
                    "nameInLcl": placeOfBirthNameMal
                },
                "childNameNotProvided": childNameNotProvided,
                "aadhaarNo": aadharNo,
                "district": {
                    "id": districtIdD,
                    "name": distEngName,
                    "nameInLcl": distMalName
                },
                "lb": {
                    "id": lbId,
                    "name": lbName,
                    "nameInLcl": lbNameInLc
                },
                "lbType": {
                    "id": "",  # ******************* missing *******************
                    "name": lbtypeEng,
                    "nameInLcl": lbTypeMal
                },
                "birthPlaceHome": {
                    "houseName": birthPlaceHome_enghouse,
                    "houseNameInLcl": birthPlaceHome_malayalam_name,
                    "localityName": localityName,
                    "localityNameInLcl": localityNameLcl,
                    "pinCode": pincode,
                    "postOffice": {
                        "id": postOfficeId,
                        "postOfficeCode": postOfficeCode,
                        "name": postOfficeName,
                        "nameInLcl": postOfficeNameInLc
                    },
                    "streetName": streetname,
                    "streetNameInLcl": streetname_inlocal,
                    "village": {
                        "id": str(village_id),
                        "name": villageName,
                        "nameInLcl": villageNameInLcl
                    },
                    "ward": {
                        "id": str(wardid),
                        "name": wardName,
                        "nameInLcl": wardNameInLcl
                    }
                },
                "birthPlaceHospital": {
                    "hospital": {
                        "id": str(hospitalid),
                        "name": hospital_name,
                        "nameInLcl": hospital_name_in_local
                    },
                    "hospitalType": {  # Todo: to be added(no idea)
                        "id": str(hospitalTypeId),
                        "name": hospitaltypeName,
                        "nameInLcl": hospitalTypeNameInLc
                    }
                },
                "birthPlaceInstitution": {
                    "institution": {
                        "id": str(birthplace_institution_id),
                        "name": birthplaceInstitutionName,
                        "nameInLcl": birthplaceInstitutionNameInLc
                    },
                    "institutionType": {
                        "id": str(birthplace_institution_type_id),
                        "name": institutionTypeName,
                        "nameInLcl": institutiontypeNameInLc
                    }
                },
                "birthPlacePublic": {
                    "localityName": localityName,
                    "localityNameInLcl": localityNameLCL,
                    "pinCode": pinCode,
                    "postOffice": {
                        "id": str(postOfficeid),
                        "name": name,
                        "nameInLcl": nameLclL
                    },
                    "publicPlace": {
                        "id": publicPlaceid,
                        "name": publicName,
                        "nameInLcl": PublicNameLcl
                    },
                    "streetName": streetName,
                    "streetNameInLcl": streetNameNameLcl,
                    "village": {
                        "id": str(villageids),
                        "name": villageName,
                        "nameInLcl": villageNameLcl
                    },
                    "ward": {
                        "id": wardNoName,
                        "name": wardName,
                        "nameInLcl": WardnameLcl
                    }
                },
                # BIRTH PLACE VEHICLE

                "birthPlaceVehicle": {
                    "lbWardPlaceOfHalt": lBWardPlaceofHalt,
                    "placeOfFirstHaltLb": placeOfFirstHaltLB,
                    "vehicleRegistrationNo": vehicleRegistrationNo,
                    "vehicleTravellingFrom": vehicleTravellingFrom,
                    "vehicleTravellingFromInLcl": vehicleTravellingFromInLcl,
                    "vehicleTravellingTo": vehicleTravellingTo,
                    "vehicleTravellingToInLcl": vehicleTravellingInLcl,
                    "vehicleType": {
                        "id": vehicleTypeId,
                        "name": vechilename,
                        "nameInLcl": vechilenameInLcl
                    },
                    "village": {
                        "id": vechilevillageid,
                        "name": vechileVillagename,
                        "nameInLcl": vechileVillagenameInLcl
                    }
                },
                "additionalBirthDetails": {
                    "causeOfFoetalDeath": causeOfFotelDeath,
                    "deliveryMethod": deliverymethod,
                    "durationOfPregnancy": pregnancyDuration,
                    "natureOfMedicalAttention": natureOfMedicalAttention,
                    "weightAt": babyWeight
                },
                # BORN OUTSIDE
                "bornOutsidePlaceOfBirthDetails": {
                    "countryOfBirth": {
                        "id": countryOfBirthid,
                        "name": CountryBirthName,
                        "nameInLcl": CountryBirthNameInLcl
                    },
                    "placeOfBirthInEn": placeOfBirthInEn,
                    "placeOfBirthInLcl": placeOfBirthInLcl,
                    "stateOrProvinceOrRegionInEn": stateOrProvinceOrRegionInEn,
                    "stateOrProvinceOrRegionInLcl": stateOrProvinceOrRegionInLcl,
                    "nationality": {
                        "id": nationalityid,
                        "name": nationalityName,
                        "nameInLcl": nationalityNameInLcl
                    },
                    "passportNo": passportNo,
                    "dateOfArrivalInIndia": dateOfArrivalInIndia
                }
            },
            # FATHER DETAILS
            "parentDetails": {
                "fatherDetails": {
                    "fatherAadhaarNo": fatherAadhaarNo,
                    "fatherEducation": {
                        "id": fatherId,
                        "name": fathername,
                        "nameInLcl": fathernameInLcl
                    },
                    "fatherInformationMissing": fatherInformationMissing,
                    "fatherName": fatherName,
                    "fatherNameInLcl": fatherNameInLocal,
                    "fatherNationality": {
                        "id": fatherNationalityid,
                        "name": fatherNationalName,
                        "nameInLcl": fatherNationalNameInLcl
                    },
                    "fatherOccupation": {
                        "id": str(fatherOccupationId),
                        "name": fatherOccupationame,
                        "nameInLcl": fatherOccupationnameInLcl
                    }
                },
                # MOTHER DETAILS

                "motherDetails": {
                    "ageAtTheTimeOfBirth": ageAtTheTimeOfBirth,
                    "ageAtTheTimeOfMarriage": ageAtTheTimeOfMarriage,
                    "motherAadhaarNo": motherAadhaarNo,
                    "motherEducation": {
                        "id": str(motherEducationid),
                        "name": eduname,
                        "nameInLcl": edunameInLcl
                    },
                    "motherMaritalStatus": motherMaritalStatus,
                    "motherMaritalStatusInLcl": motherMaritalStatusLcl,  # Todo Added by me
                    "motherName": motherName,
                    "motherNameInLcl": motherNameInLcl,
                    "motherNationality": {
                        "id": str(motherNationalityid),
                        "name": motherNationalityname,
                        "nameInLcl": motherNationalitynameInLcl
                    },
                    "motherOccupation": {
                        "id": motherOccupationId,
                        "name": motherOccupationname,
                        "nameInLcl": motherOccupationnameInLcl
                    },
                    "noOfChildrenBornAliveIncludingThisChild": noOfChildrenBornAliveIncludingThisChild
                },
                "parentEmailId": parentEmailId,
                "parentsContactNo": parentsContactNo,
                "religion": {
                    "id": religionid,
                    "name": religionname,
                    "nameInLcl": religionnameInLcl
                }
            },
            # PERMANENT ADDRESS INSIDE KERALA
            "addressDetails": {
                "permanentAddressInsideKerala": {
                    "district": {
                        "id": str(districtId),
                        "name": permanaentAddInklName,
                        "nameInLcl": permanaentAddInklNameInLcl
                    },
                    "houseName": perAddInKlhouseName,
                    "houseNameInLcl": perAddInKlhouseNameInLcl,
                    "lb": {
                        "id": str(lbid),
                        "name": lbidName,
                        "nameInLcl": lbidNameInLcl
                    },
                    "lbType": {
                        "id": lbTypeid,
                        "name": lbTypeIdName,
                        "nameInLcl": lbTypeIdNameInLcl
                    },
                    "localityName": AddresslocalityName,
                    "localityNameInLcl": AddresslocalityNameInlocal,
                    "pinCode": AddresslocalityPincode,
                    "postOffice": {
                        "id": str(PostofficeId),
                        "name": PostofficeIdName,
                        "nameInLcl": PostofficeIdNameInLcl
                    },
                    "streetName": perAddInKlstreetName,
                    "streetNameInLcl": perAddInKlstreetNameInLcl,
                    "taluk": {
                        "id": perAddKltalukid,
                        "name": perAddKltalukname,
                        "nameInLcl": perAddKltaluknameInLcl
                    },
                    "village": {
                        "id": str(villageId),
                        "name": villageAddInKlname,
                        "nameInLcl": villageAddInKlnameInLcl
                    },
                    "ward": {
                        "id": perAddKlwardid,
                        "name": perAddKlWardname,
                        "nameInLcl": perAddKlWardnameInLcl
                    }
                },
                # PREMANANET ADDRESS OUTSIDE INDIA

                "permanentAddressOutsideIndia": {
                    "addressLine1": addressLine1,
                    "addressLine1InLcl": addressLine1InLcl,
                    "addressLine2": addressLine2,
                    "addressLine2InLcl": addressLine2InLcl,
                    "cityOrTownOrVillageName": cityOrTownOrVillageName,
                    "postalCode": postalCode,
                    "stateProvinceRegion": stateProvinceRegion,
                    "townOrVillage": townOrVillage
                },
                # PREMANANET ADDRESS OUTSIDE KERALA
                "permanentAddressOutsideKerala": {
                    "district": {
                        "id": "",
                        "name": "",
                        "nameInLcl": ""
                    },
                    "houseNoName": "",
                    "houseNoNameInLcl": "",
                    "localityName": "",
                    "localityNameInLcl": "",
                    "pinCode": "",
                    "postOfficeName": "",
                    "streetName": "",
                    "streetNameInLcl": "",
                    "tehsilName": "",
                    "townOrVillage": "",
                    "villageName": ""
                },
                "permanentCountry": {
                    "id": str(permanentCountryId),
                    "name": "",
                    "nameInLcl": ""
                },
                "permanentSameAsPresent": "",
                "permanentState": {
                    "id": str(permanentStateId),
                    "name": "",
                    "nameInLcl": ""
                },
                # PRESENT ADDERESS INSIDE KERALA
                "presentAddressInsideKerala": {
                    "district": {
                        "id": presentAddressInsideKeralaDistrictid,
                        "name": preAddInKlname,
                        "nameInLcl": preAddInKlnameInLcl
                    },
                    "houseName": presentAddressInsideKeralahouseName,
                    "houseNameInLcl": presentAddressInsideKeralahouseNameInlocal,
                    "lb": {
                        "id": str(presentAddressInsideKeralalbid),
                        "name": pstAddInKlname,
                        "nameInLcl": pstAddInKlnameInLcl
                    },
                    "lbType": {
                        "id": str(pstAddInKlnamelbTypeid),
                        "name": pstAddInKlname,
                        "nameInLcl": pstAddInKlnamenameInLcl
                    },
                    "localityName": pstAddInKllocalityname,
                    "localityNameInLcl": pstAddInKllocalitynameInlocal,
                    "pinCode": str(presentAddressInsideKeralaPincode),
                    "postOffice": {
                        "id": str(presentAddressInsideKeralaPostofficeId),
                        "name": POName,
                        "nameInLcl": PONameInLcl
                    },
                    "streetName": pstAddinKlstreetname,
                    "streetNameInLcl": pstAddinKlstreetnameInLcl,
                    "taluk": {
                        "id": pstAddinKltalukid,
                        "name": pstAddinKlname,
                        "nameInLcl": pstAddinKlnameInLcl
                    },
                    "village": {
                        "id": pstAddinKlvillageid,
                        "name": pstAddinKlname,
                        "nameInLcl": pstAddinKlnameInLcl
                    },
                    "ward": {
                        "id": pstAddinKlwardid,
                        "name": pstAddinKlname,
                        "nameInLcl": pstAddinKlnameInLcl
                    }
                },
                # PRESENT ADDRESS OUTSIDE INDIA
                "presentAddressOutsideIndia": {
                    "addressLine1": pstAddOutIndiaaddressLine1,
                    "addressLine1InLcl": pstAddOutIndiaaddressLine1InLcl,
                    "addressLine2": pstAddOutIndiaaddressLine2,
                    "addressLine2InLcl": pstAddOutIndiaaddressLine2InLcl,
                    "cityOrTownOrVillageName": pstAddOutIndiacityOrTownOrVillageName,
                    "postalCode": pstAddOutIndiapostalCode,
                    "stateProvinceRegion": pstAddOutIndiastateProvinceRegion,
                    "townOrVillage": pstAddOutIndiatownOrVillage
                },
                # PRESENT ADDRESS OUTSIDE KERALA
                "presentAddressOutsideKerala": {
                    "district": {
                        "id": pstAddOutKldistrictid,
                        "name": pstAddOutKlname,
                        "nameInLcl": pstAddOutKlnameInLcl
                    },
                    "houseNoName": pstAddOutKlhouseNoName,
                    "houseNoNameInLcl": pstAddOutKlhouseNoNameInLcl,
                    "localityName": pstAddOutKllocalityName,
                    "localityNameInLcl": pstAddOutKllocalityNameInLcl,
                    "pinCode": pstAddOutKlpinCode,
                    "postOfficeName": pstAddOutKlpostOfficeName,
                    "streetName": pstAddOutKlstreetName,
                    "streetNameInLcl": pstAddOutKlstreetNameInLcl,
                    "tehsilName": pstAddOutKltehsilName,
                    "townOrVillage": pstAddOutKltownOrVillage,
                    "villageName": pstAddOutKlvillageName
                },
                "presentCountryId": {
                    "id": str(permanentCountryId),
                    "name": presentCnname,
                    "nameInLcl": presentCnnameInLcl
                },
                "presentState": {
                    "id": str(permanentStateId),
                    "name": presentStatename,
                    "nameInLcl": presentStatenameInLcl
                }
            },
            "initiatorDetails": {
                "aadhaarNo": initiatoraadhaarNo,
                "address": initiatoraddress,
                "contactNo": initiatorcontactNo,
                "emailId": initiatoremailId,
                "initiator": {
                    "id": initiatorid,
                    "name": initiatorname,
                    "nameInLcl": initiatornameInLcl
                },
                "initiatorInstitutionDetails": {
                    "designation": {
                        "id": initiatorInstitutiondesignationid,
                        "name": initiatorInstitutionname,
                        "nameInLcl": initiatorInstitutionnameInLcl
                    },
                    "institution": {
                        "id": institutionid,
                        "name": institutionname,
                        "nameInLcl": institutionnameInLcl
                    }
                },
                "name": name,
                "relationshipToTheChild": relationshipToTheChild
            },
            "version": "1.0"
        }

        result["birthRegistry"].append(birth_registry)
        # print(result)
        output_file = folder_name + "/" + certificateNumber + ".json"
        # print("out=",output_file)
        #

        # Write the result dictionary to a JSON file, using the custom date_handler
        with open(output_file, "w", encoding="utf-8") as json_file:
            json.dump(result, json_file, default=date_handler, indent=4, ensure_ascii=False)

        temp_count += 1
    tot_count += temp_count
    # print("tot rc=",tot_count)

# Close the database connection
cursor.close()
conn.close()
another_datetime = datetime.now()

# Create a string representation of the datetimes
current_datetime_str = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
another_datetime_str = another_datetime.strftime("%Y-%m-%d %H:%M:%S")

# Calculate the time difference
time_difference = another_datetime - current_datetime

file_path = "Sevena-data-migration-log.txt"

# Open the file in write mode ('w')
with open(file_path, 'w') as file:
    # Write data to the file
    file.write("                SEVENA-DATA-LOG                  " + "\n")
    file.write("*************************************************" + "\n")

    file.write("Process start: " + current_datetime_str + "\n")
    file.write("Process end: " + another_datetime_str + "\n")
    file.write("Total time: " + str(time_difference) + "\n")
    file.write("Total Record: " + str(tot_count) + "\n")