import {S3Client} from '@aws-sdk/client-s3'
import {S3SyncClient, TransferMonitor} from 's3-sync-client'
import fs, { existsSync, mkdirSync } from 'fs';

const s3Client = new S3Client({region: 'us-east-1'});
const {sync} = new S3SyncClient({client: s3Client});

/**
 * @param {'unionware' | 'enterprise-like'} channel 
 * @returns {string}
 */

const uri = (channel) => `s3://event-distrib-prod-archive-6dbf635/${channel}/`

//Enterprise-like event list:
const ENTERPRISE_LIKE = ["enterprise_aff_member", "enterprise_aff_organization", "enterprise_office", "enterprise_person", "enterprise_person_address", "enterprise_person_email", "enterprise_person_phone", "enterprise_term_of_office",
"enterprise_person_nexus"]

const UNIONWARE = ["Client_EntityLinking", "Client_EntityLinkingUDFCombo1", "Contact",
"Contact_City", "Contact_ContactMapRegion", "Contact_ContactPoint", "Contact_Country",
"Contact_County", "Contact_Information", "Contact_MapRegion", "Contact_MapRegionCategory",
"Contact_MapRegionSubtype", "Contact_MapRegionType", "Contact_Point", 
"Update Contact_Point", "Contact_PointRanking", "Contact_PointStatus", "Contact_Ranking",
"Contact_Territory", "Contact_Type", "Group", "Group_Assignment",
"Group_AssignmentUDFCombo1", "Group_GlobalGroupEntity", "Group_Subtype", "Group_Type",
"Job", "Job_Chapter", "Job_ChapterCategory", "Job_ChapterStatus", "Job_ChapterUDFCombo1",
"Job_ChapterUDFCombo2", "Job_ChapterUDFCombo3", "Job_Department",
"Job_DepartmentCategory", "Job_DepartmentStatus", "Job_District",
"Job_DistrictCategory", "Job_DistrictStatus", "Job_DistrictUDFCombo1",
"Job_DistrictUDFCombo2", "Job_Employer", "Job_EmployerCategory",
"Job_EmployerStatus", "Job_EmployerSubType",
"Job_EmployerType", "Job_EntityLinking", "Job_FinanceLastPaid",
"Job_FinanceLastPaidWithPayType", "Job_PayRateType", "Job_Sector", "Job_Status",
"Job_Type", "Job_UDFCombo1", "Job_UDFCombo2", "Job_UDFCombo3", "Job_UDFCombo4",
"Job_UDFCombo5", "Job_Unit", "Job_UnitCategory", "Job_UnitStatus",
"Job_WorkLocation", "Job_WorkLocationCategory", "Job_WorkLocationStatus",
"Person", "Person_ContactPointPreferred", "Person_ContactPointPreferredByType",
"Person_Gender", "Person_Language", "Person_Member", "Person_MemberCategory",
"Person_MemberExtra", "Person_MemberExtraType", "Person_MemberExtraUDFCombo1",
"Person_MemberExtraUDFCombo2", "Person_MemberExtraUDFCombo3",
"Person_MemberExtraUDFCombo4", "Person_MemberExtraUDFCombo5",
"Person_MemberExtraUDFCombo6", "Person_MemberExtraUDFCombo7",
"Person_MemberExtraUDFCombo8", "Person_MemberStatus", 
"Person_MemberSubType","Person_MemberType", "Person_MemberUDFCombo1", "Person_NonMember",
"Person_NonMemberStatus", "Person_NonMemberSubType", "Person_NonMemberType",
"Person_NonMember_Relationship", "Person_NonMember_Relationship_Category",
"Person_NonMember_Relationship_Status", "Person_NonMember_Relationship_SubType",
"Person_NonMember_Relationship_Type", "Person_PoliticalContribution",
"Person_Primary", "Person_Skill", "Person_SkillPerson", "Person_SkillSubtype",
"Person_SkillType", "Person_Suffix", "Person_Title", "Position",
"Position_Committee", "Position_CommitteeCategory", "Position_CommitteeStatus",
"Position_CommitteeSubtype", "Position_CommitteeType", "Position_Group",
"Position_Name", "Position_Template", "Position_Type", "System_ContactPointType",
"System_Entity", "User", "_AFSCME_Job_RetireeChapter", "_AFSCME_Job_RetireeChapterStatus",
"_AFSCME_Job_RetireeSubChapter", "_AFSCME_Job_RetireeSubChapterStatus"]

const unionware = UNIONWARE.map(val => val.toLowerCase())


//create manage filtering for channel and event:
/**
 * @param {string[]} events - pass list of unionware or enterprise_like events
 */
const includedEventsByChannel = (events) => {
    let filtersByChannel = {}
    let uw_events = []
    let el_events = []

    events.map(ev => {
        if(unionware.includes(ev)){
            uw_events.push(ev)
        }

        if(ENTERPRISE_LIKE.includes(ev)){
            el_events.push(ev)
        }


        if(uw_events.length > 0){
            filtersByChannel['unionware'] = uw_events.map(ev => `event=${ev}/`)
            
        }
        
        if(el_events.length > 0){
            filtersByChannel['enterprise_like'] = el_events.map(ev => `event=${ev}/`)
        }
    });


    return filtersByChannel
}

/**
 * 
 * @param {string[]} events - enter one or more enterprise_like or unionware events as an array.
 */
export const syncS3Events = async(events) => {
    const {enterprise_like, unionware} = includedEventsByChannel(events)
    
    const retrieveWithFilters = async(channel, event) => {
        //const monitor = new TransferMonitor();
        //const timeout = setInterval(() => console.log(monitor.getStatus()), 2000);
        
        try{
            !existsSync(`${event}`) ? mkdirSync(`${event}`) : undefined
            await sync(uri(channel), `./${event}`, {filters: [
                {exclude: () => true},
                {include: (key) => key.includes(event)}
            ]});

        } catch(err){
            throw new Error(`Couldn't sync s3 bucket due to the following error: ${err}`)
        }/*finally {
            clearInterval(timeout)
        }*/
    }

    if(unionware){
        unionware.map(async(event) => {
            await retrieveWithFilters('unionware', event)
        })
    }

    if(enterprise_like){
        enterprise_like.map(async(event) => {
            await retrieveWithFilters('enterprise-like', event)
        }); 
    }
}

syncS3Events(['person_member'])

//   'person_member'