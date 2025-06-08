const hubspot = require('@hubspot/api-client');
const { queue, each } = require('async');
const _ = require('lodash');

const { filterNullValuesFromObject, goal } = require('./utils');
const Domain = require('./Domain');

const hubspotClient = new hubspot.Client({ accessToken: '', numberOfApiCallRetries: 5 });
let expirationDate;

const BATCH_SIZE = 100;

const generateLastModifiedDateFilter = (date, nowDate, propertyName = 'hs_lastmodifieddate') => {
  const lastModifiedDateFilter = date ?
    {
      filters: [
        { propertyName, operator: 'GTE', value: `${date.valueOf()}` },
        { propertyName, operator: 'LTE', value: `${nowDate.valueOf()}` }
      ]
    } :
    {};

  return lastModifiedDateFilter;
};

const saveDomain = async domain => {
  // disable this for testing purposes
  return;

  domain.markModified('integrations.hubspot.accounts');
  await domain.save();
};

/**
 * Get access token from HubSpot
 */
const refreshAccessToken = async (refreshToken) => {
  const { HUBSPOT_CID, HUBSPOT_CS } = process.env;

  return hubspotClient.oauth.tokensApi
    .createToken('refresh_token', undefined, undefined, HUBSPOT_CID, HUBSPOT_CS, refreshToken)
    .then(async result => {
      const body = result.body ? result.body : result;

      const newAccessToken = body.accessToken;
      expirationDate = new Date(body.expiresIn * 1000 + new Date().getTime());

      hubspotClient.setAccessToken(newAccessToken);
      return newAccessToken;
    });
};

const logStart = (operation) => {
  const start = new Date().getTime();
  console.log(`${start}: start processing ${operation}`);
  return start;
};

const logFinish = (operation, start) => {
  const finish = new Date().getTime();
  const totalTime = finish - start;
  const minutes = Math.floor(totalTime/1000/60);
  const seconds = ((totalTime % 60000)/1000).toFixed(0);
  console.log(`${finish}: finish processing ${operation} in ${minutes}:${seconds.toString().padStart(2, '0')} minutes`);
};
/**
 * Get recently modified companies as 100 companies per page
 */
const processCompanies = async (account) => {
  const lastPulledDate = new Date(account.lastPulledDates.companies);
  const now = new Date();

  let hasMore = true;
  const offsetObject = {};
  const limit = BATCH_SIZE;
  const actions = [];

  const start = logStart('companies');

  while (hasMore) {
    if (new Date() > expirationDate) account.refreshToken = await refreshAccessToken(account.refreshToken);
    const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
    const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now);
    const searchObject = {
      filterGroups: [lastModifiedDateFilter],
      sorts: [{ propertyName: 'hs_lastmodifieddate', direction: 'ASCENDING' }],
      properties: [
        'name',
        'domain',
        'country',
        'industry',
        'description',
        'annualrevenue',
        'numberofemployees',
        'hs_lead_status'
      ],
      limit,
      after: offsetObject.after
    };

    let searchResult = {};

    try {
      searchResult = await hubspotClient.crm.companies.searchApi.doSearch(searchObject);
    } catch (err) {
      console.log(err, { metadata: { operation: 'processCompanies' } });
      throw err;
    }

    if (!searchResult || Object.keys(searchResult).length === 0) throw new Error('Failed to fetch companies. Aborting.');

    const data = searchResult?.results || [];
    if (data.length === 0) {
      break;
    }

    offsetObject.after = parseInt(searchResult?.paging?.next?.after);

    each(data, async (company) => {
      if (!company.properties) return;

      const actionTemplate = {
        includeInAnalytics: 0,
        companyProperties: {
          company_id: company.id,
          company_domain: company.properties.domain,
          company_industry: company.properties.industry
        }
      };

      const isCreated = !lastPulledDate || (new Date(company.createdAt) > lastPulledDate);

      actions.push({
        actionName: isCreated ? 'Company Created' : 'Company Updated',
        actionDate: new Date(isCreated ? company.createdAt : company.updatedAt) - 2000,
        ...actionTemplate
      });
    }, (err) => {
      if (err) {
        console.log(err, { metadata: { operation: 'processCompanies' } });
      }
    });

    if (!offsetObject?.after) {
      hasMore = false;
      break;
    } else if (offsetObject?.after >= 9900) {
      offsetObject.after = 0;
      offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
    }
  }
  logFinish('companies', start);
  account.lastPulledDates.companies = now;
  return actions;
};

/**
 * Batch process contacts in chunks to minimize API calls
 */
const contactCache = new Map();
const getContacts = async (contactIds) => {
  const contacts = [];
  
  for(let contactId of contactIds) {
    if(contactCache.has(contactId)) { 
      contacts.push(contactCache.get(contactId));
      continue;
    }
    const contact = await hubspotClient.crm.contacts.basicApi.getById(contactId);
    contactCache.set(contactId, contact);
    contacts.push(contact);
  }
  return contacts;
};

/**
 * Get recently modified contacts as 100 contacts per page
 */
const processContacts = async (account) => {
  const lastPulledDate = new Date(account.lastPulledDates.contacts);
  const now = new Date();

  let hasMore = true;
  const offsetObject = {};
  const limit = BATCH_SIZE;
  const actions = [];

  const start = logStart('contacts');

  while (hasMore) {
    if (new Date() > expirationDate) account.refreshToken = await refreshAccessToken(account.refreshToken);
    const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
    const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now, 'lastmodifieddate');
    const searchObject = {
      filterGroups: [lastModifiedDateFilter],
      sorts: [{ propertyName: 'lastmodifieddate', direction: 'ASCENDING' }],
      properties: [
        'firstname',
        'lastname',
        'jobtitle',
        'email',
        'hubspotscore',
        'hs_lead_status',
        'hs_analytics_source',
        'hs_latest_source'
      ],
      limit,
      after: offsetObject.after
    };

    let searchResult = {};

    try {
      searchResult = await hubspotClient.crm.contacts.searchApi.doSearch(searchObject);
    } catch (err) {
      console.log(err, { metadata: { operation: 'processContacts' } });
      throw err;
    }
    
    if (!searchResult || Object.keys(searchResult).length === 0) throw new Error('Failed to fetch contacts. Aborting.');

    const data = searchResult.results || [];

    offsetObject.after = parseInt(searchResult.paging?.next?.after);
    const contactIds = data.map(contact => contact.id);

    // contact to company association
    const contactsToAssociate = contactIds;
    const companyAssociationsResults = (await (await hubspotClient.apiRequest({
      method: 'post',
      path: '/crm/v3/associations/CONTACTS/COMPANIES/batch/read',
      body: { inputs: contactsToAssociate.map(contactId => ({ id: contactId })) }
    })).json())?.results || [];

    const companyAssociations = Object.fromEntries(companyAssociationsResults.map(a => {
      if (a.from) {
        contactsToAssociate.splice(contactsToAssociate.indexOf(a.from.id), 1);
        return [a.from.id, a.to[0].id];
      } else return false;
    }).filter(x => x));

    each(data, async (contact) => {
      if (!contact.properties || !contact.properties.email) return;

      const companyId = companyAssociations[contact.id];

      const isCreated = new Date(contact.createdAt) > lastPulledDate;

      const userProperties = {
        company_id: companyId,
        contact_name: ((contact.properties.firstname || '') + ' ' + (contact.properties.lastname || '')).trim(),
        contact_title: contact.properties.jobtitle,
        contact_source: contact.properties.hs_analytics_source,
        contact_status: contact.properties.hs_lead_status,
        contact_score: parseInt(contact.properties.hubspotscore) || 0
      };

      const actionTemplate = {
        includeInAnalytics: 0,
        identity: contact.properties.email,
        userProperties: filterNullValuesFromObject(userProperties)
      };

      actions.push({
        actionName: isCreated ? 'Contact Created' : 'Contact Updated',
        actionDate: new Date(isCreated ? contact.createdAt : contact.updatedAt),
        ...actionTemplate
      });
    }, (err) => {
      if (err) {
        console.log(err, { metadata: { operation: 'processContacts' } });
      }
    });

    if (!offsetObject?.after) {
      hasMore = false;
      break;
    } else if (offsetObject?.after >= 9900) {
      offsetObject.after = 0;
      offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
    }
  }
  logFinish('contacts', start);
  account.lastPulledDates.contacts = now;
  return actions;
};

/**
 * Get recently modified meeting engagements
 */
const processMeetings = async (account) => {
  const lastPulledDate = new Date(account.lastPulledDates.meetings || 0);
  const now = new Date();
  
  let hasMore = true;
  const offsetObject = {};
  const limit = BATCH_SIZE;
  const actions = [];
  contactCache.clear();
  
  const start = logStart('meetings');

  while (hasMore) {
    if (new Date() > expirationDate) account.refreshToken = await refreshAccessToken(account.refreshToken);
    let searchResult = {};
    const searchObject = {
      limit,
      after: offsetObject.after || 0,
      properties: [
        "hs_timestamp",
        "hubspot_owner_id",
        "hs_meeting_title",
        "hs_meeting_body",
        "hs_internal_meeting_notes",
        "hs_meeting_external_url",
        "hs_meeting_location",
        "hs_meeting_start_time",
        "hs_meeting_end_time",
        "hs_meeting_outcome"
      ]
    };

    try {
      searchResult = await hubspotClient.crm.objects.meetings.basicApi.getPage(limit, offsetObject.after, searchObject.properties, null, ["0-1"]);
    } catch (err) {
      console.log(JSON.stringify(err, null, 2), { metadata: { operation: 'processMeetings'} });
      throw err;
    }

    if (searchResult == null || Object.keys(searchResult).length === 0) throw new Error('Failed to fetch meetings for the 4th time. Aborting.');

    if (searchResult.results.length === 0) {
      hasMore = false;
      break;
    }

    const data = searchResult.results || [];
    offsetObject.after = searchResult.offset;
    hasMore = searchResult.results.length === limit;

    // Process meetings with cached contact data
    each(data, async (meeting) => {
      if (new Date(meeting.updatedAt) <= lastPulledDate) return;

      let contactsData = [];
      let userData = [];
      const contacts = meeting.associations?.contacts
      if (contacts) {
        const ids = contacts.results.map(r => r.id);
        contactsData = await getContacts(ids);
        userData = contactsData.map(c => ({
          id: c.id,
          email: c.properties.email,
          name: ((c.properties.firstname || '') + ' ' + (c.properties.lastname || '')).trim()
        }));
      }
      
      const meetingProperties = {
        meeting_id: meeting.id,
        meeting_title: meeting.properties.hs_meeting_title,
        meeting_body: meeting.properties.hs_meeting_body,
        meeting_start_time: meeting.properties.hs_meeting_start_time,
        meeting_end_time: meeting.properties.hs_meeting_end_time,
        meeting_location: meeting.properties.hs_meeting_location,
        meeting_outcome: meeting.properties.hs_meeting_outcome,
        meeting_external_url: meeting.properties.hs_meeting_external_url,
        meeting_internal_notes: meeting.properties.hs_internal_meeting_notes,
        meeting_timestamp: meeting.properties.hs_timestamp,
        meeting_attendees: userData
      };

      const isCreated = new Date(meeting.createdAt) > lastPulledDate;

      actions.push({
        actionName: isCreated ? 'Meeting Created' : 'Meeting Updated',
        actionDate: new Date(isCreated ? meeting.createdAt : meeting.updatedAt),
        includeInAnalytics: 0,
        meetingProperties: filterNullValuesFromObject(meetingProperties)
      });
    }, (err) => {
      if (err) {
        console.log(err, { metadata: { operation: 'processMeetings' } });
      }
    });
    
  }
  logFinish('meetings', start);
  account.lastPulledDates.meetings = now;
  return actions;
};

const createQueue = (actions) => queue(async (action, callback) => {
  actions.push(action);

  if (actions.length > 2000) {
    console.log('inserting actions to database', { count: actions.length });

    const copyOfActions = _.cloneDeep(actions);
    actions.splice(0, actions.length);

    goal(copyOfActions);
  }

  callback();
}, 100000000);

const drainQueue = async (actions, q) => {
  if (q.length() > 0) await q.drain();

  if (actions.length > 0) {
    goal(actions)
  }

  return true;
};

const pullDataFromHubspot = async () => {
  
  const start = logStart('pulling data from HubSpot');
  const domain = await Domain.findOne({});
  const actions = [];
  const q = createQueue(domain, actions);

  for (const account of domain.integrations.hubspot.accounts) {
    console.log(`${new Date().getTime()}: start processing account`);
    let allActions = {};
    try {
      account.refreshToken = await refreshAccessToken(account.refreshToken);
    } catch (err) {
      console.log(err, { metadata: { operation: 'refreshAccessToken' } });
      continue; // Skip this account if we can't refresh the token
    }

    try {
      // Run all processing operations in parallel
      const [contactActions, companyActions, meetingActions] = await Promise.all([
        processContacts(account).catch(err => {
          console.log(err, { metadata: { operation: 'processContacts' } });
          return [];
        }),
        processCompanies(account).catch(err => {
          console.log(err, { metadata: { operation: 'processCompanies' } });
          return [];
        }),
        processMeetings(account).catch(err => {
          console.log(err, { metadata: { operation: 'processMeetings' } });
          return [];
        })
      ]);

      allActions = {
        contactActions,
        companyActions,
        meetingActions,
        all: [...contactActions, ...companyActions, ...meetingActions]
      }
      q.push(allActions.all);
      console.log(`${new Date().getTime()}: processed all data`);

      try {
        await drainQueue(actions, q);
        console.log(`${new Date().getTime()}: drain queue`);
      } catch (err) {
        console.log(err, { metadata: { operation: 'drainQueue' } });
      }
    } catch (err) {
      console.log(err, { metadata: { operation: 'parallelProcessing' } });
    }

    console.log(`${new Date().getTime()}: finish processing account. ${allActions.all.length} actions stored: ${allActions.contactActions.length} contacts, ${allActions.companyActions.length} companies, ${allActions.meetingActions.length} meetings.`);
  }
  await saveDomain(domain);
  logFinish('pulling data from HubSpot', start);
  process.exit();
};

module.exports = pullDataFromHubspot;
