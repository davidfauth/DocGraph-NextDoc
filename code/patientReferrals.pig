
healthcare_provider_taxonomy = LOAD 's3n://NUCC-Taxonomy/nucc_taxonomy_130.txt' USING PigStorage('\t') AS
(nuccCode:chararray,
nuccType:chararray,
nuccClassification:chararray,
nuccSpecialty:chararray);

medGraphDataReferrals = LOAD 's3n://medgraph/refer.2011.csv' USING PigStorage(',') AS
(primaryProvider:chararray,
referredDoctor: chararray,
qtyReferred:double);

-- Load NPI Data
npiDataSpecialties = LOAD 's3n://NPIData/npidata_20050523-20130512.csv' USING PigStorage(',') AS
(NPICode:chararray,
f2:chararray,
f3:chararray,
f4:chararray,
f5:chararray,
f6:chararray,
f7:chararray,
f8:chararray,
f9:chararray,
f10:chararray,
f11:chararray,
f12:chararray,
f13:chararray,
f14:chararray,
f15:chararray,
f16:chararray,
f17:chararray,
f18:chararray,
f19:chararray,
f20:chararray,
f21:chararray,
f22:chararray,
f23:chararray,
f24:chararray,
f25:chararray,
f26:chararray,
f27:chararray,
f28:chararray,
f29:chararray,
f30:chararray,
f31:chararray,
f32:chararray,
f33:chararray,
f34:chararray,
f35:chararray,
f36:chararray,
f37:chararray,
f38:chararray,
f39:chararray,
f40:chararray,
f41:chararray,
f42:chararray,
f43:chararray,
f44:chararray,
f45:chararray,
f46:chararray,
f47:chararray,
provider_taxonomy_code_1:chararray,
f49:chararray);

npi_data_no_quotes = FOREACH npiDataSpecialties GENERATE REPLACE(NPICode,'\\"','') as newNPICode, 
REPLACE(f5, '\\"','') as orgName,
REPLACE(f6, '\\"','') as orgLastName,
REPLACE(f7, '\\"', '') as firstName, 
REPLACE(f21, '\\"','') as docAddra1,
REPLACE(f22, '\\"','') as docAddra2,
REPLACE(f23, '\\"','') as docCity1,
REPLACE(f29, '\\"','') as docAddr1,
REPLACE(f30, '\\"','') as docAddr2,
REPLACE(f31, '\\"','') as docCity,
REPLACE(f32, '\\"','') as docState,
REPLACE(f33, '\\"','') as docPostalCode,
REPLACE(provider_taxonomy_code_1, '\\"','') as taxonomyCode;

primaryDocTax = JOIN medGraphDataReferrals BY (primaryProvider), npi_data_no_quotes BY newNPICode;
primaryDocSimple = FOREACH primaryDocTax GENERATE taxonomyCode as primaryDocTaxonomy, referredDoctor, qtyReferred;

referralDocTax = JOIN primaryDocSimple BY (referredDoctor), npi_data_no_quotes BY newNPICode;
referralDocSimple = FOREACH referralDocTax GENERATE primaryDocTaxonomy, taxonomyCode as referralDocTaxonomy, qtyReferred;

groupReferralDocs = GROUP referralDocSimple BY (primaryDocTaxonomy, referralDocTaxonomy);

byReferralCounts = FOREACH groupReferralDocs GENERATE
    Flatten(group) as (primaryDocTaxonomy, referralDocTaxonomy),
    SUM(referralDocSimple.qtyReferred) as sumQtyReferred;
    
referralCountOrdered = ORDER byReferralCounts BY sumQtyReferred DESC;
referralCountOut = LIMIT referralCountOrdered 50;

referralCountOrderedASC = ORDER byReferralCounts BY sumQtyReferred ASC;
referralCountOutASC = LIMIT referralCountOrderedASC 100;

referralCountPrimary = JOIN referralCountOut BY (primaryDocTaxonomy), healthcare_provider_taxonomy BY (nuccCode);
referralCountPrimaryDetails = FOREACH referralCountPrimary GENERATE primaryDocTaxonomy, nuccType as PrimaryNuccType, nuccClassification as PrimaryNuccClassification, nuccSpecialty as PrimaryNuccSpecialty, referralDocTaxonomy, sumQtyReferred;

referralCountReferral = JOIN referralCountPrimaryDetails BY (referralDocTaxonomy), healthcare_provider_taxonomy BY (nuccCode);

referralCountReferralDetails = FOREACH  referralCountReferral 
GENERATE primaryDocTaxonomy,PrimaryNuccType, PrimaryNuccClassification, PrimaryNuccSpecialty,
referralDocTaxonomy, nuccType as ReferralNuccType, nuccClassification as ReferralNuccClassification, 
nuccSpecialty as ReferralNuccSpecialty, sumQtyReferred;
  
referralDetailsOut = ORDER referralCountReferralDetails BY sumQtyReferred DESC;  

-- Bottom 50 Records
referralCountPrimaryASC = JOIN referralCountOutASC BY (primaryDocTaxonomy), healthcare_provider_taxonomy BY (nuccCode);
referralCountPrimaryDetailsASC = FOREACH referralCountPrimaryASC GENERATE primaryDocTaxonomy, nuccType as PrimaryNuccType, nuccClassification as PrimaryNuccClassification, nuccSpecialty as PrimaryNuccSpecialty, referralDocTaxonomy, sumQtyReferred;

referralCountReferralASC = JOIN referralCountPrimaryDetailsASC BY (referralDocTaxonomy), healthcare_provider_taxonomy BY (nuccCode);

referralCountReferralDetailsASC = FOREACH referralCountReferralASC
GENERATE primaryDocTaxonomy,PrimaryNuccType, PrimaryNuccClassification, PrimaryNuccSpecialty,
referralDocTaxonomy, nuccType as ReferralNuccType, nuccClassification as ReferralNuccClassification, 
nuccSpecialty as ReferralNuccSpecialty, sumQtyReferred;
  
referralDetailsOutASC = ORDER referralCountReferralDetailsASC BY sumQtyReferred ASC;  

rmf s3n://DataOut/DocGraph/TopReferrals;
rmf s3n://DataOut/DocGraph/BottomReferrals;

STORE referralDetailsOut INTO 's3n://DataOut/DocGraph/TopReferrals' USING PigStorage('|');
STORE referralDetailsOutASC INTO 's3n://DataOut/DocGraph/BottomReferrals' USING PigStorage('|');